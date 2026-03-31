"""
================================================================================
CustomOCR Pipeline API  -  v4.0.0
================================================================================
FIXES IN THIS VERSION vs previous v4.0:
  FIX 1: UnicodeEncodeError on Windows (cp1252 terminal)
          Removed ALL Unicode special chars from every log/print message.
          Unicode arrows (->) and checkmarks replaced with plain ASCII.
          StreamHandler now opens stdout with explicit UTF-8 encoding.

  FIX 2: No module named psycopg2
          _get_pg_engine() now catches ImportError separately and prints
          the exact pip install command needed.
          SQLAlchemy connection string auto-adds +psycopg2 driver if missing.

  FIX 3: ocr_jobs table does not exist
          PostgreSQL was never initialized because psycopg2 import failed
          before CREATE TABLE ran. After installing psycopg2-binary the
          table is created automatically on first FastAPI startup.
================================================================================
"""

import os
import sys
import re
import gc
import time
import shutil
import logging
import secrets
import uvicorn
import asyncio
import psutil
import traceback
import threading
from pathlib import Path
from typing import Optional, Dict, Any, Set, List
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from collections import defaultdict
import urllib.parse

from fastapi import (
    FastAPI, HTTPException, File, UploadFile,
    Request, Depends, Security
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import aiofiles
from celery.result import AsyncResult

# =============================================================================
# LOAD .env
# =============================================================================
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=True)
        print("Loaded .env from: " + str(_env_path))
    else:
        _env_cwd = Path.cwd() / ".env"
        if _env_cwd.exists():
            load_dotenv(_env_cwd, override=True)
            print("Loaded .env from: " + str(_env_cwd))
        else:
            print("WARNING: No .env file found")
except ImportError:
    print("WARNING: python-dotenv not installed.")


# =============================================================================
# CONFIGURATION
# =============================================================================
VALID_TOKENS: Set[str] = set()
for _i in range(1, 11):
    _tv = os.getenv(f"AUTH_TOKEN_{_i}", "").strip()
    if _tv:
        VALID_TOKENS.add(_tv)
_legacy = os.getenv("API_AUTH_TOKEN", "").strip()
if _legacy:
    VALID_TOKENS.add(_legacy)

BASE_OUTPUT_DIR       = Path(os.getenv("OCR_OUTPUT_DIR", "/code/output")).resolve()
MAX_UPLOAD_SIZE_MB    = int(os.getenv("MAX_UPLOAD_SIZE_MB",    "500"))
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024
UPLOAD_CHUNK_SIZE     = 1024 * 1024
JOB_RETENTION_HOURS   = int(os.getenv("JOB_RETENTION_HOURS",  "24"))
MAX_JOB_DURATION      = int(os.getenv("MAX_JOB_DURATION",     "3600"))
STALE_JOB_THRESHOLD   = int(os.getenv("STALE_JOB_THRESHOLD",  "1800"))
MAX_QUEUE_DEPTH       = int(os.getenv("MAX_QUEUE_DEPTH",       "50"))
MIN_DISK_FREE_GB      = float(os.getenv("MIN_DISK_FREE_GB",    "2.0"))
MIN_RAM_FREE_MB       = float(os.getenv("MIN_RAM_FREE_MB",     "512"))
RATE_LIMIT_RPM        = int(os.getenv("RATE_LIMIT_RPM",        "0"))
WORKER_RAM_GB         = float(os.getenv("WORKER_RAM_GB",       "1.5"))
SYSTEM_RESERVE_GB     = float(os.getenv("SYSTEM_RESERVE_GB",   "4.0"))
_mw = os.getenv("MAX_WORKERS", "0")
MAX_WORKERS           = int(_mw) if _mw.strip().lower() not in ("0", "auto") else 0
REDIS_URL             = os.getenv("REDIS_URL",        "redis://localhost:6379/0")
REDIS_RESULT_URL      = os.getenv("REDIS_RESULT_URL", "redis://localhost:6379/1")
POSTGRES_URL          = os.getenv("POSTGRES_URL",     "").strip()
FLOWER_PORT           = int(os.getenv("FLOWER_PORT",  "5555"))

ALLOWED_EXTENSIONS: Set[str] = {
    ".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".odp", ".odt",
    ".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp",
    ".json", ".xml", ".txt", ".csv", ".py", ".md", ".html", ".css", ".js"
}

# =============================================================================
# DIRECTORIES
# =============================================================================
BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
COMPLETED_DIR = BASE_OUTPUT_DIR / "completed"
COMPLETED_DIR.mkdir(exist_ok=True)


# =============================================================================
# LOGGING  -  FIX 1: Force UTF-8 on stream handler to avoid Windows cp1252 crash
# =============================================================================
def _build_stream_handler() -> logging.StreamHandler:
    """
    On Windows, sys.stdout uses the console code page (cp1252 by default).
    Unicode characters like arrows and checkmarks are not in cp1252 and raise
    UnicodeEncodeError. We reopen stdout with UTF-8 explicitly.
    Falls back to plain sys.stdout if reconfiguration fails.
    """
    try:
        import io
        utf8_stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
        )
        return logging.StreamHandler(utf8_stdout)
    except AttributeError:
        # sys.stdout has no .buffer (e.g. in some IDE consoles) - use as-is
        return logging.StreamHandler(sys.stdout)


_log_fmt = '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] %(message)s'
_file_handler   = logging.FileHandler(BASE_OUTPUT_DIR / "api.log", encoding="utf-8")
_stream_handler = _build_stream_handler()
_file_handler.setFormatter(logging.Formatter(_log_fmt))
_stream_handler.setFormatter(logging.Formatter(_log_fmt))

logging.root.setLevel(logging.INFO)
logging.root.handlers = [_file_handler, _stream_handler]
logger = logging.getLogger(__name__)


# =============================================================================
# CELERY APP IMPORT
# =============================================================================
from celery_app import celery_app as _celery_app


# =============================================================================
# POSTGRESQL  -  FIX 2: Use psycopg2-binary driver, catch ImportError clearly
# =============================================================================
_pg_engine = None


def _get_pg_engine():
    """
    Lazily initialise SQLAlchemy + psycopg2 engine.

    Requires:  pip install psycopg2-binary sqlalchemy
    If psycopg2-binary is not installed, logs a clear install command and
    returns None (PostgreSQL logging is skipped silently for the session).
    """
    global _pg_engine
    if _pg_engine is not None:
        return _pg_engine
    if not POSTGRES_URL:
        return None

    # --- FIX 2a: catch missing driver early with a clear message ---
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        logger.warning(
            "[PostgreSQL] psycopg2 driver not found. "
            "Run:  pip install psycopg2-binary  then restart the API."
        )
        return None

    try:
        from sqlalchemy import create_engine, text

        # --- FIX 2b: ensure +psycopg2 dialect is in the URL ---
        pg_url = POSTGRES_URL
        if pg_url.startswith("postgresql://") and "+psycopg2" not in pg_url:
            pg_url = pg_url.replace("postgresql://", "postgresql+psycopg2://", 1)

        _pg_engine = create_engine(
            pg_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            connect_args={"connect_timeout": 10},
        )

        # --- FIX 3: CREATE TABLE runs here on first startup ---
        with _pg_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ocr_jobs (
                    job_id            VARCHAR(16)   PRIMARY KEY,
                    filename          TEXT,
                    status            VARCHAR(20),
                    total_pages       INTEGER,
                    output_size_bytes BIGINT,
                    elapsed_sec       FLOAT,
                    error             TEXT,
                    client_ip         VARCHAR(64),
                    created_at        TIMESTAMP     DEFAULT NOW(),
                    completed_at      TIMESTAMP
                )
            """))
            conn.commit()

        logger.info("[PostgreSQL] Connected. Table ocr_jobs ready.")
        return _pg_engine

    except Exception as e:
        logger.warning("[PostgreSQL] Not available (non-fatal): %s", str(e))
        _pg_engine = None
        return None


def _pg_upsert_job(job_id: str, meta: dict, celery_result: dict):
    """Write or update a job record in PostgreSQL. Silently skipped if no DB."""
    engine = _get_pg_engine()
    if not engine:
        return
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ocr_jobs
                    (job_id, filename, status, total_pages, output_size_bytes,
                     elapsed_sec, error, client_ip, created_at, completed_at)
                VALUES
                    (:job_id, :filename, :status, :total_pages, :output_size_bytes,
                     :elapsed_sec, :error, :client_ip, :created_at, :completed_at)
                ON CONFLICT (job_id) DO UPDATE SET
                    status            = EXCLUDED.status,
                    total_pages       = EXCLUDED.total_pages,
                    output_size_bytes = EXCLUDED.output_size_bytes,
                    elapsed_sec       = EXCLUDED.elapsed_sec,
                    error             = EXCLUDED.error,
                    completed_at      = EXCLUDED.completed_at
            """), {
                "job_id":            job_id,
                "filename":          meta.get("filename", ""),
                "status":            celery_result.get("status", "unknown"),
                "total_pages":       celery_result.get("total_pages"),
                "output_size_bytes": celery_result.get("output_size_bytes"),
                "elapsed_sec":       celery_result.get("elapsed_sec"),
                "error":             celery_result.get("error"),
                "client_ip":         meta.get("client_ip", ""),
                "created_at":        meta.get("created_at", datetime.utcnow()),
                "completed_at": (
                    datetime.utcnow()
                    if celery_result.get("status") in ("completed", "failed")
                    else None
                ),
            })
            conn.commit()
    except Exception as e:
        logger.warning("[PostgreSQL] Write failed for %s: %s", job_id, str(e))


# =============================================================================
# THREAD-SAFE JOB METADATA STORE
# =============================================================================

class ThreadSafeJobStore:
    def __init__(self):
        self._lock  = threading.RLock()
        self._store: Dict[str, Dict[str, Any]] = {}

    def create(self, job_id: str, data: Dict[str, Any]) -> None:
        with self._lock:
            self._store[job_id] = data

    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            info = self._store.get(job_id)
            return dict(info) if info else None

    def exists(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._store

    def update(self, job_id: str, **fields) -> bool:
        with self._lock:
            if job_id not in self._store:
                return False
            self._store[job_id].update(fields)
            self._store[job_id]["updated_at"] = datetime.now()
            return True

    def delete(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._store.pop(job_id, None)

    def get_expired(self, cutoff: datetime) -> List[str]:
        with self._lock:
            return [
                jid for jid, info in self._store.items()
                if info.get("created_at", datetime.now()) < cutoff
            ]

    def get_stale(self, threshold_seconds: int) -> List[str]:
        cutoff = datetime.now() - timedelta(seconds=threshold_seconds)
        with self._lock:
            return [
                jid for jid, info in self._store.items()
                if info.get("last_celery_state") in ("STARTED", "PROGRESS")
                and info.get("updated_at", datetime.now()) < cutoff
            ]

    def list_all(self, status_filter: Optional[str] = None,
                 limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            items = sorted(
                self._store.items(),
                key=lambda x: x[1].get("created_at", datetime.min),
                reverse=True
            )
            result = []
            for jid, info in items:
                snap = dict(info)
                snap["job_id"] = jid
                result.append(snap)
                if len(result) >= limit:
                    break
            return result

    def count_active(self) -> int:
        with self._lock:
            job_ids = list(self._store.keys())
        active = 0
        for jid in job_ids:
            try:
                r = AsyncResult(jid, app=_celery_app)
                if r.state in ("PENDING", "STARTED", "PROGRESS", "RETRY"):
                    active += 1
            except Exception:
                pass
        return active

    def stats(self) -> Dict[str, int]:
        with self._lock:
            return {"total_tracked": len(self._store)}

    def __len__(self):
        with self._lock:
            return len(self._store)


job_store = ThreadSafeJobStore()


# =============================================================================
# RATE LIMITER
# =============================================================================

class SlidingWindowRateLimiter:
    def __init__(self, max_requests: int, window_seconds: int = 60):
        self._lock        = threading.Lock()
        self._requests: Dict[str, List[float]] = defaultdict(list)
        self.max_requests = max_requests
        self.window       = window_seconds
        self.enabled      = max_requests > 0

    def is_allowed(self, key: str) -> bool:
        if not self.enabled:
            return True
        now    = time.time()
        cutoff = now - self.window
        with self._lock:
            self._requests[key] = [t for t in self._requests[key] if t > cutoff]
            if len(self._requests[key]) >= self.max_requests:
                return False
            self._requests[key].append(now)
            return True

    def cleanup(self):
        now    = time.time()
        cutoff = now - self.window * 2
        with self._lock:
            stale = [k for k, ts in self._requests.items()
                     if not ts or ts[-1] < cutoff]
            for k in stale:
                del self._requests[k]


rate_limiter = SlidingWindowRateLimiter(max_requests=RATE_LIMIT_RPM)


# =============================================================================
# RESOURCE MONITOR
# =============================================================================

class ResourceMonitor:
    @staticmethod
    def total_ram_gb() -> float:
        return psutil.virtual_memory().total / (1024 ** 3)

    @staticmethod
    def available_ram_mb() -> float:
        return psutil.virtual_memory().available / (1024 ** 2)

    @staticmethod
    def available_ram_gb() -> float:
        return psutil.virtual_memory().available / (1024 ** 3)

    @staticmethod
    def cpu_count() -> int:
        return os.cpu_count() or 2

    @staticmethod
    def load_avg() -> float:
        try:
            return os.getloadavg()[0]
        except (OSError, AttributeError):
            return 0.0

    @staticmethod
    def disk_free_gb(path: Path) -> float:
        try:
            return shutil.disk_usage(path).free / (1024 ** 3)
        except OSError:
            return 999.0

    @staticmethod
    def compute_optimal_workers(
        ram_per_worker_gb: float = 1.5,
        system_reserve_gb: float = 4.0,
    ) -> int:
        total_ram = psutil.virtual_memory().total / (1024 ** 3)
        avail     = max(0, total_ram - system_reserve_gb)
        ram_limit = int(avail / ram_per_worker_gb)
        cpu_limit = os.cpu_count() or 2
        optimal   = max(2, min(ram_limit, cpu_limit))
        # FIX 1: plain ASCII log message - no Unicode arrows
        logger.info(
            "[ResourceMonitor] Workers: RAM=%.1fGB ram_limit=%d cpu_limit=%d optimal=%d",
            total_ram, ram_limit, cpu_limit, optimal
        )
        return optimal

    @classmethod
    def can_accept_job(cls, output_dir: Path) -> tuple:
        free_gb = cls.disk_free_gb(output_dir)
        if free_gb < MIN_DISK_FREE_GB:
            return False, f"Low disk: {free_gb:.1f}GB free (min {MIN_DISK_FREE_GB}GB)"
        free_ram = cls.available_ram_mb()
        if free_ram < MIN_RAM_FREE_MB:
            return False, f"Low RAM: {free_ram:.0f}MB free (min {MIN_RAM_FREE_MB}MB)"
        return True, "ok"


monitor = ResourceMonitor()

_cleanup_task:       Optional[asyncio.Task] = None
_stale_reaper_task:  Optional[asyncio.Task] = None
_actual_max_workers: int = 2


# =============================================================================
# AUTHENTICATION
# =============================================================================
security_scheme = HTTPBearer(
    scheme_name="Bearer Token",
    description="Enter the API token from Katonic Platform -> AI Studio -> API Management.",
    auto_error=True
)


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(security_scheme)
) -> str:
    if not VALID_TOKENS:
        raise HTTPException(status_code=503, detail="Authentication not configured.")
    incoming = credentials.credentials
    for valid in VALID_TOKENS:
        if secrets.compare_digest(incoming, valid):
            return incoming
    raise HTTPException(
        status_code=401,
        detail="Invalid or expired token.",
        headers={"WWW-Authenticate": "Bearer"},
    )


# =============================================================================
# VALIDATION HELPERS
# =============================================================================

def sanitize_filename(filename: str) -> str:
    filename = os.path.basename(filename).replace("\x00", "")
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename).strip(". ")
    name, ext = os.path.splitext(filename)
    if len(name) > 200:
        name = name[:200]
    if not name:
        name = f"upload_{secrets.token_hex(4)}"
    return f"{name}{ext}"


def validate_file_metadata(filename: str) -> None:
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported type: '{ext}'. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
        )


def validate_job_id(job_id: str) -> None:
    if not job_id or not re.fullmatch(r'[a-f0-9]{16}', job_id):
        raise HTTPException(status_code=400, detail="Invalid job ID format")


def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# =============================================================================
# CELERY STATE HELPERS
# =============================================================================

def _celery_to_api_status(celery_state: str) -> str:
    return {
        "PENDING":  "queued",
        "STARTED":  "processing",
        "PROGRESS": "processing",
        "SUCCESS":  "completed",
        "FAILURE":  "failed",
        "RETRY":    "processing",
        "REVOKED":  "cancelled",
    }.get(celery_state, "unknown")


def _get_celery_result(job_id: str) -> dict:
    try:
        result     = AsyncResult(job_id, app=_celery_app)
        state      = result.state
        info       = result.info or {}
        api_status = _celery_to_api_status(state)
        response   = {"celery_state": state, "status": api_status}

        if state == "SUCCESS" and isinstance(info, dict):
            response.update({
                "download_url":      info.get("download_url", f"/download/{job_id}"),
                "total_pages":       info.get("total_pages"),
                "output_size_bytes": info.get("output_size_bytes"),
                "elapsed_sec":       info.get("elapsed_sec"),
                "completed_at":      info.get("completed_at"),
            })
        elif state in ("PROGRESS", "STARTED") and isinstance(info, dict):
            response["progress"] = {
                "step":        info.get("step"),
                "message":     info.get("message"),
                "pages_done":  info.get("pages_done", 0),
                "total_pages": info.get("total_pages", 0),
                "percent":     info.get("percent", 0),
                "elapsed_sec": info.get("elapsed_sec"),
            }
        elif state == "FAILURE":
            response["error"] = str(info) if info else "Unknown error"

        return response
    except Exception as e:
        logger.warning("[AsyncResult] Could not read state for %s: %s", job_id, str(e))
        return {"celery_state": "UNKNOWN", "status": "unknown"}


# =============================================================================
# CLEANUP
# =============================================================================

def cleanup_old_jobs() -> int:
    cutoff  = datetime.now() - timedelta(hours=JOB_RETENTION_HOURS)
    expired = job_store.get_expired(cutoff)
    for job_id in expired:
        info = job_store.delete(job_id)
        if info:
            rp = info.get("result_path")
            if rp and os.path.exists(rp):
                try:
                    os.remove(rp)
                except OSError as e:
                    logger.warning("Failed to remove %s: %s", rp, str(e))
    if expired:
        logger.info("Cleaned up %d expired jobs", len(expired))
    return len(expired)


def reap_stale_jobs() -> int:
    stale = job_store.get_stale(STALE_JOB_THRESHOLD)
    for job_id in stale:
        logger.warning("[Job %s] Stale - marking in metadata store", job_id)
        job_store.update(job_id, last_celery_state="STALE")
    if stale:
        logger.info("Reaped %d stale jobs", len(stale))
    return len(stale)


def cleanup_job_directory(directory: Path):
    try:
        if directory and directory.exists():
            shutil.rmtree(directory)
    except Exception as e:
        logger.warning("Cleanup failed for %s: %s", str(directory), str(e))


# =============================================================================
# FASTAPI APP
# =============================================================================

def detect_root_path() -> str:
    route = os.getenv("ROUTE", "")
    if route:
        if not route.startswith("/"):  route = "/" + route
        if not route.endswith("/"):    route = route + "/"
    return route


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cleanup_task, _stale_reaper_task, _actual_max_workers

    # FIX 1: All log messages are pure ASCII
    logger.info("=" * 60)
    logger.info("CustomOCR Pipeline API v4.0.0 (Celery + Redis) starting...")
    logger.info("=" * 60)

    _actual_max_workers = (
        MAX_WORKERS if MAX_WORKERS > 0
        else monitor.compute_optimal_workers(WORKER_RAM_GB, SYSTEM_RESERVE_GB)
    )
    logger.info("Reference workers  : %d", _actual_max_workers)
    logger.info("Output Directory   : %s", str(BASE_OUTPUT_DIR))
    logger.info("Max Upload Size    : %d MB", MAX_UPLOAD_SIZE_MB)
    logger.info("Job Retention      : %d hours", JOB_RETENTION_HOURS)
    logger.info("Rate Limit         : %d req/min/IP", RATE_LIMIT_RPM)
    logger.info("Max Queue Depth    : %d", MAX_QUEUE_DEPTH)
    logger.info("Redis Broker       : %s", REDIS_URL)
    logger.info("PostgreSQL         : %s",
                "configured" if POSTGRES_URL else "not set (optional)")

    if VALID_TOKENS:
        logger.info("Auth Tokens        : %d configured", len(VALID_TOKENS))
    else:
        logger.warning("NO AUTH TOKENS CONFIGURED!")

    logger.info("RAM                : %.1f GB", monitor.total_ram_gb())
    logger.info("CPU cores          : %d", monitor.cpu_count())
    logger.info("Disk free          : %.1f GB", monitor.disk_free_gb(BASE_OUTPUT_DIR))

    try:
        test_file = BASE_OUTPUT_DIR / ".write_test"
        test_file.touch()
        test_file.unlink()
        logger.info("Output directory   : Writable OK")
    except Exception as e:
        raise RuntimeError(f"Cannot write to output directory: {BASE_OUTPUT_DIR}") from e

    # Initialize PostgreSQL (creates table if psycopg2-binary is installed)
    _get_pg_engine()

    cleanup_old_jobs()
    _cleanup_task      = asyncio.create_task(periodic_job_cleanup())
    _stale_reaper_task = asyncio.create_task(periodic_stale_reaper())

    logger.info("=" * 60)
    logger.info("API Ready. Accepting requests.")
    logger.info("=" * 60)

    yield

    logger.info("Shutting down...")
    for task in [_cleanup_task, _stale_reaper_task]:
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    logger.info("Shutdown complete.")


app = FastAPI(
    title="CustomOCR Pipeline API",
    description=(
        "Production-grade OCR pipeline. Upload documents and receive Markdown output.\n\n"
        "All processing is asynchronous.\n\n"
        "---\n\n"
        "### Authentication\n"
        "All endpoints except /health require a Bearer token.\n"
    ),
    version="4.0.0",
    root_path=detect_root_path(),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# MIDDLEWARE
# =============================================================================

@app.middleware("http")
async def request_middleware(request: Request, call_next):
    request_id = secrets.token_hex(8)
    request.state.request_id = request_id

    if request.url.path.rstrip("/").endswith("/process") and request.method == "POST":
        client_ip = get_client_ip(request)
        if not rate_limiter.is_allowed(client_ip):
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded ({RATE_LIMIT_RPM} req/min). Please wait.",
                    "retry_after_seconds": 60,
                },
                headers={"Retry-After": "60", "X-Request-ID": request_id},
            )

    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "unknown")
    logger.error(
        "[%s] Unhandled: %s %s %s: %s",
        request_id, request.method, str(request.url.path),
        type(exc).__name__, str(exc)
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error.", "error_type": type(exc).__name__,
                 "request_id": request_id},
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_id = getattr(request.state, "request_id", "unknown")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "request_id": request_id},
        headers=getattr(exc, "headers", None),
    )


class JobResponse(BaseModel):
    job_id:       str
    status:       str
    filename:     str
    message:      str
    download_url: Optional[str] = None
    created_at:   datetime
    request_id:   Optional[str] = None


# =============================================================================
# ENDPOINTS - PUBLIC
# =============================================================================

@app.get("/health")
def health_check():
    disk_free = monitor.disk_free_gb(BASE_OUTPUT_DIR)
    ram_free  = monitor.available_ram_mb()
    load      = monitor.load_avg()
    warnings  = []
    if disk_free < MIN_DISK_FREE_GB:
        warnings.append(f"Low disk: {disk_free:.1f}GB free")
    if ram_free < MIN_RAM_FREE_MB:
        warnings.append(f"Low RAM: {ram_free:.0f}MB free")
    if not REDIS_URL:
        warnings.append("REDIS_URL not configured")

    return {
        "status":         "healthy" if not warnings else "degraded",
        "service":        "CustomOCR Pipeline API",
        "version":        "4.0.0",
        "architecture":   "Celery + Redis",
        "docs_url":       f"{detect_root_path()}docs",
        "authentication": {"tokens_configured": len(VALID_TOKENS)},
        "config": {
            "max_upload_size_mb":  MAX_UPLOAD_SIZE_MB,
            "job_retention_hours": JOB_RETENTION_HOURS,
            "job_timeout_sec":     MAX_JOB_DURATION,
            "rate_limit_rpm":      RATE_LIMIT_RPM,
            "max_queue_depth":     MAX_QUEUE_DEPTH,
            "redis_url":           REDIS_URL,
            "postgres":            "configured" if POSTGRES_URL else "not set",
            "flower_port":         FLOWER_PORT,
        },
        "resources": {
            "cpu_cores":        monitor.cpu_count(),
            "load_avg":         round(load, 2),
            "ram_available_mb": round(ram_free, 0),
            "disk_free_gb":     round(disk_free, 1),
        },
        "warnings": warnings,
    }


@app.get("/", include_in_schema=False)
def root():
    return health_check()


# =============================================================================
# ENDPOINTS - PROTECTED
# =============================================================================

@app.post("/process", response_model=JobResponse)
async def process_document(
    request: Request,
    file: UploadFile = File(...),
    token: str = Depends(verify_token),
):
    """Upload a document for OCR. Returns job_id immediately."""
    from tasks import process_document_task

    request_id = getattr(request.state, "request_id", "unknown")
    job_id     = None
    job_dir    = None
    input_path = None

    try:
        validate_file_metadata(file.filename)
        safe_filename = sanitize_filename(file.filename)

        can_accept, reason = monitor.can_accept_job(BASE_OUTPUT_DIR)
        if not can_accept:
            raise HTTPException(status_code=503,
                                detail=f"Server cannot accept jobs: {reason}. Retry later.")

        if MAX_QUEUE_DEPTH > 0:
            active = job_store.count_active()
            if active >= MAX_QUEUE_DEPTH:
                raise HTTPException(
                    status_code=429,
                    detail=f"Server busy: {active}/{MAX_QUEUE_DEPTH} jobs queued. Retry later.",
                )

        job_id     = secrets.token_hex(8)
        job_dir    = BASE_OUTPUT_DIR / job_id
        job_dir.mkdir(exist_ok=True)
        input_path = job_dir / safe_filename

        total_size = 0
        try:
            async with aiofiles.open(input_path, "wb") as out:
                while True:
                    chunk = await file.read(UPLOAD_CHUNK_SIZE)
                    if not chunk:
                        break
                    total_size += len(chunk)
                    if total_size > MAX_UPLOAD_SIZE_BYTES:
                        await out.close()
                        input_path.unlink(missing_ok=True)
                        raise HTTPException(
                            status_code=413,
                            detail=(f"File exceeds {MAX_UPLOAD_SIZE_MB}MB limit "
                                    f"(received {total_size / 1024 / 1024:.1f}MB)"),
                        )
                    await out.write(chunk)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to read file: {e}")
        finally:
            await file.close()

        if total_size == 0:
            input_path.unlink(missing_ok=True)
            raise HTTPException(status_code=400, detail="Empty file uploaded.")

        logger.info("[%s] [Job %s] Received: %s (%.2f MB)",
                    request_id, job_id, safe_filename, total_size / 1024 / 1024)

        now = datetime.now()
        job_store.create(job_id, {
            "last_celery_state": "PENDING",
            "filename":          safe_filename,
            "original_filename": file.filename,
            "file_size_bytes":   total_size,
            "result_path":       None,
            "request_id":        request_id,
            "client_ip":         get_client_ip(request),
            "created_at":        now,
            "updated_at":        now,
        })

        # Pass real absolute paths directly — works for both local Windows dev
        # and Katonic deployment (shared filesystem, same absolute path in all processes).
        process_document_task.apply_async(
            args=[job_id, str(input_path), str(job_dir), file.filename],
            task_id=job_id,
        )
        logger.info("[Job %s] Dispatched to Celery ocr_queue", job_id)
     
        return JobResponse(
            job_id=job_id, status="queued", filename=safe_filename,
            message="Document accepted. Processing in background.",
            download_url=f"/job/{job_id}", created_at=now, request_id=request_id,
        )

    except HTTPException:
        raise
    except Exception as e:
        if job_dir and job_dir.exists():
            cleanup_job_directory(job_dir)
        if job_id:
            job_store.delete(job_id)
        logger.error("[%s] Upload failed: %s", request_id, str(e))
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/job/{job_id}")
async def get_job_status(job_id: str, token: str = Depends(verify_token)):
    """Get current status of a processing job."""
    validate_job_id(job_id)
    meta = job_store.get(job_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Job not found")

    celery_data = _get_celery_result(job_id)
    job_store.update(job_id, last_celery_state=celery_data["celery_state"])

    if celery_data["status"] in ("completed", "failed"):
        asyncio.get_event_loop().run_in_executor(
            None, _pg_upsert_job, job_id, meta, celery_data
        )

    response = {
        "job_id":       job_id,
        "status":       celery_data["status"],
        "celery_state": celery_data["celery_state"],
        "filename":     meta["filename"],
        "file_size_mb": round(meta.get("file_size_bytes", 0) / 1024 / 1024, 2),
        "created_at":   meta["created_at"],
        "updated_at":   meta.get("updated_at", meta["created_at"]),
    }
    for key in ("progress", "download_url", "total_pages",
                "output_size_bytes", "elapsed_sec", "completed_at", "error"):
        if key in celery_data:
            response[key] = celery_data[key]
    if celery_data["status"] == "completed":
        response["download_url"] = f"/download/{job_id}"
    return response


@app.get("/tasks/{job_id}/status")
async def get_celery_task_status(job_id: str, token: str = Depends(verify_token)):
    """Raw Celery AsyncResult from Redis."""
    validate_job_id(job_id)
    return _get_celery_result(job_id)


@app.get("/queue/stats")
async def get_queue_stats(token: str = Depends(verify_token)):
    """Live Celery worker and queue statistics."""
    try:
        inspect        = _celery_app.control.inspect(timeout=2.0)
        active         = inspect.active()   or {}
        reserved       = inspect.reserved() or {}
        stats          = inspect.stats()    or {}
        active_tasks   = sum(len(v) for v in active.values())
        reserved_tasks = sum(len(v) for v in reserved.values())
        return {
            "workers_online":  len(stats),
            "active_tasks":    active_tasks,
            "queued_tasks":    reserved_tasks,
            "max_queue_depth": MAX_QUEUE_DEPTH,
            "worker_details":  {k: {"active": len(active.get(k, []))} for k in stats},
        }
    except Exception as e:
        return {
            "error": f"Could not reach Celery workers: {e}",
            "tip":   "Ensure the Celery worker is running and Redis is reachable.",
        }


@app.get("/download/{job_id}")
async def download_markdown(job_id: str, token: str = Depends(verify_token)):
    """Download the markdown result for a completed job."""
    validate_job_id(job_id)
    celery_data = _get_celery_result(job_id)

    if celery_data["status"] == "processing":
        raise HTTPException(status_code=202, detail="Job still processing.")
    if celery_data["status"] == "queued":
        raise HTTPException(status_code=202, detail="Job queued - not started yet.")
    if celery_data["status"] == "failed":
        raise HTTPException(
            status_code=400,
            detail=f"Job failed: {celery_data.get('error', 'Unknown error')}"
        )

    meta        = job_store.get(job_id)
    result_path = None
    if meta and meta.get("result_path") and os.path.exists(meta["result_path"]):
        result_path = meta["result_path"]
    else:
        matching = list(COMPLETED_DIR.glob(f"{job_id}_*.md"))
        if matching:
            result_path = str(matching[0])

    if not result_path or not os.path.exists(result_path):
        raise HTTPException(status_code=404, detail="Result file not found.")

    filename     = meta["filename"] if meta else job_id
    ascii_name   = f"{job_id}.md"
    full_name    = f"{job_id}_{filename}.md"
    encoded_name = urllib.parse.quote(full_name, safe="")

    return FileResponse(
        path=result_path,
        media_type="text/markdown",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{ascii_name}"; '
                f"filename*=UTF-8''{encoded_name}"
            )
        },
    )


@app.get("/jobs")
async def list_jobs(
    status: Optional[str] = None,
    limit: int = 50,
    token: str = Depends(verify_token),
):
    """List all tracked jobs."""
    limit     = max(1, min(limit, 500))
    jobs      = job_store.list_all(limit=limit)
    jobs_list = []
    for info in jobs:
        jid = info.get("job_id")
        if not jid:
            continue
        cel_data   = _get_celery_result(jid)
        api_status = cel_data.get("status", "unknown")
        if status and api_status != status:
            continue
        entry = {
            "job_id":     jid,
            "status":     api_status,
            "filename":   info.get("filename", ""),
            "created_at": info.get("created_at"),
            "updated_at": info.get("updated_at"),
        }
        if api_status == "completed":
            entry["download_url"] = f"/download/{jid}"
        if "progress" in cel_data:
            entry["progress"] = cel_data["progress"]
        jobs_list.append(entry)
    return {"total_tracked": len(job_store), "returned": len(jobs_list), "jobs": jobs_list}


@app.delete("/job/{job_id}")
async def delete_job(job_id: str, token: str = Depends(verify_token)):
    """Delete a job record and its result file."""
    validate_job_id(job_id)
    meta = job_store.get(job_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Job not found")
    cel_data = _get_celery_result(job_id)
    if cel_data["status"] in ("queued", "processing"):
        raise HTTPException(status_code=409, detail="Cannot delete active job.")
    if meta.get("result_path") and os.path.exists(meta["result_path"]):
        try:
            os.remove(meta["result_path"])
        except OSError as e:
            logger.warning("[Job %s] Delete result file failed: %s", job_id, str(e))
    job_store.delete(job_id)
    return {"message": f"Job {job_id} deleted successfully"}


# =============================================================================
# BACKGROUND TASKS
# =============================================================================

async def periodic_job_cleanup():
    while True:
        try:
            await asyncio.sleep(3600)
            cleanup_old_jobs()
            rate_limiter.cleanup()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Periodic cleanup error: %s", str(e))
            await asyncio.sleep(60)


async def periodic_stale_reaper():
    while True:
        try:
            await asyncio.sleep(300)
            reap_stale_jobs()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Stale reaper error: %s", str(e))
            await asyncio.sleep(60)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8050"))
    print("\n" + "=" * 60)
    print("  CustomOCR Pipeline API v4.0.0 (Celery + Redis)")
    print("=" * 60)
    if VALID_TOKENS:
        print(f"  Auth Tokens   : {len(VALID_TOKENS)} configured")
    else:
        print("  WARNING: NO TOKENS CONFIGURED!")
    print(f"  Output Dir    : {BASE_OUTPUT_DIR}")
    print(f"  Redis URL     : {REDIS_URL}")
    print(f"  PostgreSQL    : {'configured' if POSTGRES_URL else 'not set (optional)'}")
    print(f"  Max Queue     : {MAX_QUEUE_DEPTH}")
    print(f"  Port          : {port}")
    print("=" * 60 + "\n")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        timeout_keep_alive=120,
        log_level="info",
    )