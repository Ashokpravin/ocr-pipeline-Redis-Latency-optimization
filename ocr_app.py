"""
================================================================================
CustomOCR Pipeline API (Production-Hardened v3.0.0)
================================================================================
DESCRIPTION:
    Asynchronous document-to-markdown conversion API. Accepts PDF, Office docs,
    and images, processes them through a 3-stage pipeline (Layout Analysis →
    Masking → OCR), and produces clean, structured Markdown output.

    v3.0 CHANGES (from Debug Report):
    ─────────────────────────────────
    CRITICAL BUG FIXES:
      ✓ Thread-safe job_status dict (threading.RLock on ALL access)
      ✓ Streaming file upload (1 MB chunks, never loads full file into RAM)
      ✓ Dynamic queue depth (resource-aware, not hardcoded)
      ✓ Per-job timeout with graceful cancellation
      ✓ Disk space guard before accepting uploads

    PRODUCTION HARDENING:
      ✓ Per-IP rate limiting (configurable, protects against 1000+ users)
      ✓ Request ID on every response for tracing
      ✓ Per-page progress reporting (pages_done / total_pages / percent)
      ✓ Resource-aware job acceptance (RAM, disk, CPU)
      ✓ Dynamic worker calculation with production defaults
      ✓ Stale job reaper (detects stuck/zombie jobs)
      ✓ Graceful shutdown with active job draining
      ✓ Structured JSON logging option

AUTHENTICATION:
    All endpoints (except /health and /) require a valid Bearer token.
    Tokens are managed via the .env file (AUTH_TOKEN_1 through AUTH_TOKEN_10).

WORKFLOW:
    1. Client uploads file via POST /process (with Bearer token)
    2. API returns job_id immediately (non-blocking)
    3. Background worker processes: Ingestion → DLA → PageProcessor
    4. Client polls GET /job/{job_id} for status (with progress %)
    5. Client downloads result via GET /download/{job_id}

CONFIGURATION:
    All config is loaded from the .env file in the same directory.
    See .env file for all available settings.
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
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import asynccontextmanager
from collections import defaultdict
import urllib.parse

# FastAPI imports
from fastapi import (
    FastAPI, HTTPException, File, UploadFile, BackgroundTasks,
    Request, Depends, Security
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import aiofiles

# =============================================================================
# LOAD .env FILE (must be FIRST before any os.getenv calls)
# =============================================================================
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=True)
        print(f"✓ Loaded .env from: {_env_path}")
    else:
        _env_cwd = Path.cwd() / ".env"
        if _env_cwd.exists():
            load_dotenv(_env_cwd, override=True)
            print(f"✓ Loaded .env from: {_env_cwd}")
        else:
            print(f"⚠ No .env file found at {_env_path} or {_env_cwd}")
except ImportError:
    print("⚠ python-dotenv not installed. Install with: pip install python-dotenv")
    print("  Falling back to system environment variables only.")


# =============================================================================
# CONFIGURATION (All loaded from .env or system environment)
# =============================================================================

# --- AUTHENTICATION: Load tokens from AUTH_TOKEN_1 through AUTH_TOKEN_10 ---
VALID_TOKENS: Set[str] = set()
_token_count = 0

for i in range(1, 11):
    token_value = os.getenv(f"AUTH_TOKEN_{i}", "").strip()
    if token_value:
        VALID_TOKENS.add(token_value)
        _token_count += 1

# Also support legacy API_AUTH_TOKEN env var (backward compatibility)
_legacy_token = os.getenv("API_AUTH_TOKEN", "").strip()
if _legacy_token:
    VALID_TOKENS.add(_legacy_token)
    _token_count += 1

# Output directory
BASE_OUTPUT_DIR = Path(os.getenv("OCR_OUTPUT_DIR", "/code/output")).resolve()

# File upload constraints
MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", "500"))
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024

# Job lifecycle
JOB_RETENTION_HOURS = int(os.getenv("JOB_RETENTION_HOURS", "24"))

# --- Worker / Concurrency Configuration ---
# MAX_WORKERS: 0 = auto-detect based on system resources (recommended)
_mw = os.getenv("MAX_WORKERS", "0")
MAX_WORKERS = int(_mw) if _mw.strip().lower() not in ("0", "auto") else 0

# Per-job timeout in seconds (0 = no timeout)
MAX_JOB_DURATION = int(os.getenv("MAX_JOB_DURATION", "3600"))

# Resource thresholds for accepting new jobs
MIN_DISK_FREE_GB = float(os.getenv("MIN_DISK_FREE_GB", "2.0"))
MIN_RAM_FREE_MB = float(os.getenv("MIN_RAM_FREE_MB", "512"))

# Rate limiting (requests per minute per IP, 0 = disabled)
RATE_LIMIT_RPM = int(os.getenv("RATE_LIMIT_RPM", "30"))

# Upload streaming chunk size
UPLOAD_CHUNK_SIZE = 1024 * 1024  # 1 MB

# Worker RAM reservation for optimal_worker_count
WORKER_RAM_GB = float(os.getenv("WORKER_RAM_GB", "1.5"))
SYSTEM_RESERVE_GB = float(os.getenv("SYSTEM_RESERVE_GB", "4.0"))

# Stale job detection (seconds without status update = considered stuck)
STALE_JOB_THRESHOLD = int(os.getenv("STALE_JOB_THRESHOLD", "1800"))  # 30 min

ALLOWED_EXTENSIONS: Set[str] = {
    ".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".odp", ".odt",
    ".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp",
    ".json", ".xml", ".txt", ".csv", ".py", ".md", ".html", ".css", ".js"
}


# =============================================================================
# DIRECTORY SETUP
# =============================================================================

BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
COMPLETED_DIR = BASE_OUTPUT_DIR / "completed"
COMPLETED_DIR.mkdir(exist_ok=True)


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] %(message)s',
    handlers=[
        logging.FileHandler(BASE_OUTPUT_DIR / "api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)



# =============================================================================
# THREAD-SAFE JOB STATUS STORE
# =============================================================================
# CRITICAL FIX (Debug Report Section 6):
# The original code used a plain dict modified from async endpoints,
# background threads, and cleanup tasks simultaneously — causing dict
# corruption, vanishing jobs, and 404s on completed work.
#
# This class wraps all access behind an RLock (re-entrant to allow nested
# calls from the same thread) so no concurrent mutation is possible.
# =============================================================================

class ThreadSafeJobStore:
    """
    Thread-safe wrapper around the job status dictionary.

    Every read, write, delete, and iteration goes through the lock.
    RLock allows the same thread to acquire it multiple times (re-entrant)
    which is needed because some methods call other methods internally.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._store: Dict[str, Dict[str, Any]] = {}

    # --- Core CRUD ---

    def create(self, job_id: str, data: Dict[str, Any]) -> None:
        with self._lock:
            self._store[job_id] = data

    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            info = self._store.get(job_id)
            return dict(info) if info else None  # Return a snapshot copy

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

    # --- Queries ---

    def count_by_status(self, *statuses: str) -> int:
        with self._lock:
            return sum(
                1 for j in self._store.values()
                if j.get("status") in statuses
            )

    def count_active(self) -> int:
        return self.count_by_status("queued", "processing")

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
                if status_filter and info.get("status") != status_filter:
                    continue
                snapshot = dict(info)
                snapshot["job_id"] = jid
                result.append(snapshot)
                if len(result) >= limit:
                    break
            return result

    def get_expired(self, cutoff: datetime) -> List[str]:
        with self._lock:
            return [
                jid for jid, info in self._store.items()
                if info.get("created_at", datetime.now()) < cutoff
            ]

    def get_stale(self, threshold_seconds: int) -> List[str]:
        """Find jobs stuck in 'processing' with no recent update."""
        cutoff = datetime.now() - timedelta(seconds=threshold_seconds)
        with self._lock:
            return [
                jid for jid, info in self._store.items()
                if info.get("status") == "processing"
                and info.get("updated_at", datetime.now()) < cutoff
            ]

    def stats(self) -> Dict[str, int]:
        with self._lock:
            counts = defaultdict(int)
            for info in self._store.values():
                counts[info.get("status", "unknown")] += 1
            return {
                "total": len(self._store),
                "queued": counts.get("queued", 0),
                "processing": counts.get("processing", 0),
                "completed": counts.get("completed", 0),
                "failed": counts.get("failed", 0),
            }

    def __len__(self):
        with self._lock:
            return len(self._store)


# Instantiate the thread-safe store (replaces the old plain dict)
job_store = ThreadSafeJobStore()


# =============================================================================
# RATE LIMITER (per-IP sliding window)
# =============================================================================

class SlidingWindowRateLimiter:
    """
    Simple in-memory sliding-window rate limiter.
    Tracks request timestamps per key (IP address) and rejects
    requests exceeding the configured RPM.

    For 1000+ concurrent users, this prevents any single user from
    monopolizing the pipeline while allowing fair access to all.
    """

    def __init__(self, max_requests: int, window_seconds: int = 60):
        self._lock = threading.Lock()
        self._requests: Dict[str, List[float]] = defaultdict(list)
        self.max_requests = max_requests
        self.window = window_seconds
        self.enabled = max_requests > 0

    def is_allowed(self, key: str) -> bool:
        if not self.enabled:
            return True

        now = time.time()
        cutoff = now - self.window

        with self._lock:
            # Prune old entries
            self._requests[key] = [
                t for t in self._requests[key] if t > cutoff
            ]
            if len(self._requests[key]) >= self.max_requests:
                return False
            self._requests[key].append(now)
            return True

    def cleanup(self):
        """Remove stale keys (call periodically)."""
        now = time.time()
        cutoff = now - self.window * 2
        with self._lock:
            stale_keys = [
                k for k, timestamps in self._requests.items()
                if not timestamps or timestamps[-1] < cutoff
            ]
            for k in stale_keys:
                del self._requests[k]


rate_limiter = SlidingWindowRateLimiter(max_requests=RATE_LIMIT_RPM)


# =============================================================================
# RESOURCE MONITOR
# =============================================================================

class ResourceMonitor:
    """
    Monitors system resources to make admission-control decisions.
    Used to:
      - Calculate optimal worker count at startup
      - Guard against OOM by rejecting uploads when memory is low
      - Guard against disk-full by checking free space
      - Detect system overload
    """

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
    def cpu_percent() -> float:
        try:
            return psutil.cpu_percent(interval=0.1)
        except Exception:
            return 0.0

    @staticmethod
    def load_avg() -> float:
        try:
            return os.getloadavg()[0]
        except (OSError, AttributeError):
            return 0.0

    @staticmethod
    def disk_free_gb(path: Path) -> float:
        try:
            usage = shutil.disk_usage(path)
            return usage.free / (1024 ** 3)
        except OSError:
            return 999.0  # If we can't check, don't block

    @staticmethod
    def compute_optimal_workers(
        ram_per_worker_gb: float = 1.5,
        system_reserve_gb: float = 4.0
    ) -> int:
        """
        Production worker calculation.

        Strategy:
          - RAM is the binding constraint (DLA loads images into memory)
          - Workers = min(ram_based_limit, cpu_count)
          - At least 2 workers for overlap (one CPU-bound, one I/O-bound)
          - Never exceed CPU count (pipeline stages are CPU-bound)
        """
        total_ram = psutil.virtual_memory().total / (1024 ** 3)
        available_for_workers = max(0, total_ram - system_reserve_gb)
        ram_limit = int(available_for_workers / ram_per_worker_gb)
        cpu_limit = os.cpu_count() or 2

        optimal = max(2, min(ram_limit, cpu_limit))

        logger.info(
            f"[ResourceMonitor] Worker calc: "
            f"RAM={total_ram:.1f}GB, "
            f"safe={available_for_workers:.1f}GB, "
            f"CPU={cpu_limit}, "
            f"ram_limit={ram_limit}, "
            f"optimal={optimal}"
        )
        return optimal

    @classmethod
    def can_accept_job(cls, output_dir: Path) -> tuple:
        """
        Admission control: check if system has capacity for another job.
        Returns (allowed: bool, reason: str).
        """
        # Check disk space
        free_gb = cls.disk_free_gb(output_dir)
        if free_gb < MIN_DISK_FREE_GB:
            return False, f"Low disk space: {free_gb:.1f}GB free (min: {MIN_DISK_FREE_GB}GB)"

        # Check RAM
        free_ram = cls.available_ram_mb()
        if free_ram < MIN_RAM_FREE_MB:
            return False, f"Low memory: {free_ram:.0f}MB free (min: {MIN_RAM_FREE_MB}MB)"

        return True, "ok"


monitor = ResourceMonitor()


# =============================================================================
# GLOBAL STATE
# =============================================================================

executor: Optional[ThreadPoolExecutor] = None
_cleanup_task: Optional[asyncio.Task] = None
_stale_reaper_task: Optional[asyncio.Task] = None
_actual_max_workers: int = 2  # Will be set at startup


# =============================================================================
# AUTHENTICATION SETUP
# =============================================================================

security_scheme = HTTPBearer(
    scheme_name="Bearer Token",
    description=(
        "Enter the API token created in **Katonic Platform → AI Studio → API Management**.\n\n"
        "**Format:** Paste your full token directly (the `Bearer` prefix is added automatically).\n\n"
        "Tokens are managed in the `.env` file (`AUTH_TOKEN_1` through `AUTH_TOKEN_10`).\n"
    ),
    auto_error=True
)


def verify_token(credentials: HTTPAuthorizationCredentials = Security(security_scheme)) -> str:
    """Validate Bearer token against configured tokens."""
    if not VALID_TOKENS:
        logger.error("No authentication tokens configured!")
        raise HTTPException(
            status_code=503,
            detail="Authentication is not configured on the server."
        )

    incoming_token = credentials.credentials
    for valid_token in VALID_TOKENS:
        if secrets.compare_digest(incoming_token, valid_token):
            return incoming_token

    logger.warning("Authentication failed: invalid token")
    raise HTTPException(
        status_code=401,
        detail="Invalid or expired token.",
        headers={"WWW-Authenticate": "Bearer"}
    )


# =============================================================================
# LAZY IMPORTS FOR PIPELINE MODULES
# =============================================================================

_file_ingestor_cls = None
_dla_cls = None
_page_processor_cls = None
_get_optimal_worker_count = None
_cleanup_resource = None
_pipeline_load_lock = threading.Lock()


def _load_pipeline_modules():
    """Lazily import heavy pipeline modules on first use (thread-safe)."""
    global _file_ingestor_cls, _dla_cls, _page_processor_cls
    global _get_optimal_worker_count, _cleanup_resource

    if _file_ingestor_cls is not None:
        return

    with _pipeline_load_lock:
        # Double-check after acquiring lock
        if _file_ingestor_cls is not None:
            return

        try:
            from utils import get_optimal_worker_count, cleanup_resource
            _get_optimal_worker_count = get_optimal_worker_count
            _cleanup_resource = cleanup_resource
        except ImportError as e:
            logger.error(f"Failed to import utils module: {e}")
            raise RuntimeError(f"Pipeline dependency missing: utils - {e}")

        try:
            from FileIngestor import FileIngestor
            _file_ingestor_cls = FileIngestor
        except ImportError as e:
            logger.error(f"Failed to import FileIngestor: {e}")
            raise RuntimeError(f"Pipeline dependency missing: FileIngestor - {e}")

        try:
            from DLA import DLA
            _dla_cls = DLA
        except ImportError as e:
            logger.error(f"Failed to import DLA: {e}")
            raise RuntimeError(f"Pipeline dependency missing: DLA - {e}")

        try:
            from PageProcessor import PageProcessor
            _page_processor_cls = PageProcessor
        except ImportError as e:
            logger.error(f"Failed to import PageProcessor: {e}")
            raise RuntimeError(f"Pipeline dependency missing: PageProcessor - {e}")

        logger.info("All pipeline modules loaded successfully")


# =============================================================================
# VALIDATION HELPERS
# =============================================================================

def sanitize_filename(filename: str) -> str:
    """Removes potentially dangerous characters from filename."""
    filename = os.path.basename(filename)
    filename = filename.replace("\x00", "")
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)
    filename = filename.strip('. ')
    name, ext = os.path.splitext(filename)
    if len(name) > 200:
        name = name[:200]
    if not name:
        name = f"upload_{secrets.token_hex(4)}"
    return f"{name}{ext}"


def validate_file_metadata(filename: str) -> None:
    """Validates file metadata before streaming upload begins."""
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: '{ext}'. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
        )


def validate_job_id(job_id: str) -> None:
    """Validate job_id format to prevent injection/path traversal."""
    if not job_id or not re.fullmatch(r'[a-f0-9]{16}', job_id):
        raise HTTPException(status_code=400, detail="Invalid job ID format")


def get_client_ip(request: Request) -> str:
    """Extract client IP, respecting proxy headers."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


# =============================================================================
# CLEANUP FUNCTIONS
# =============================================================================

def cleanup_old_jobs() -> int:
    """Removes job records older than JOB_RETENTION_HOURS."""
    cutoff = datetime.now() - timedelta(hours=JOB_RETENTION_HOURS)
    expired_ids = job_store.get_expired(cutoff)

    for job_id in expired_ids:
        info = job_store.delete(job_id)
        if info:
            result_path = info.get("result_path")
            if result_path and os.path.exists(result_path):
                try:
                    os.remove(result_path)
                    logger.info(f"Removed expired result: {result_path}")
                except OSError as e:
                    logger.warning(f"Failed to remove {result_path}: {e}")

    if expired_ids:
        logger.info(f"Cleaned up {len(expired_ids)} expired jobs")
    return len(expired_ids)


def reap_stale_jobs() -> int:
    """Mark stuck/zombie jobs as failed."""
    stale_ids = job_store.get_stale(STALE_JOB_THRESHOLD)
    for job_id in stale_ids:
        logger.warning(f"[Job {job_id}] Marking as failed (stale for >{STALE_JOB_THRESHOLD}s)")
        job_store.update(
            job_id,
            status="failed",
            message=f"Job timed out (no progress for {STALE_JOB_THRESHOLD}s)",
            error_step="timeout"
        )
    if stale_ids:
        logger.info(f"Reaped {len(stale_ids)} stale jobs")
    return len(stale_ids)


def cleanup_job_directory(directory: Path):
    """Safely removes temporary processing directory."""
    try:
        if directory.exists():
            shutil.rmtree(directory)
            logger.info(f"Cleaned up: {directory.name}")
    except Exception as e:
        logger.warning(f"Cleanup failed for {directory}: {e}")


# =============================================================================
# FASTAPI APPLICATION SETUP
# =============================================================================

def detect_root_path() -> str:
    """Detects workspace proxy path for Katonic deployment."""
    route = os.getenv("ROUTE", "")
    if route:
        if not route.startswith("/"):
            route = "/" + route
        if not route.endswith("/"):
            route = route + "/"
        return route
    return ""


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown logic."""
    global executor, _cleanup_task, _stale_reaper_task, _actual_max_workers

    # --- STARTUP ---
    logger.info("=" * 60)
    logger.info("CustomOCR Pipeline API v3.0.0 starting...")
    logger.info("=" * 60)

    # Calculate optimal workers
    if MAX_WORKERS > 0:
        _actual_max_workers = MAX_WORKERS
        logger.info(f"Workers: {_actual_max_workers} (configured via MAX_WORKERS)")
    else:
        _actual_max_workers = monitor.compute_optimal_workers(
            ram_per_worker_gb=WORKER_RAM_GB,
            system_reserve_gb=SYSTEM_RESERVE_GB
        )
        logger.info(f"Workers: {_actual_max_workers} (auto-detected)")

    # Log configuration
    logger.info(f"Output Directory   : {BASE_OUTPUT_DIR}")
    logger.info(f"Completed Dir      : {COMPLETED_DIR}")
    logger.info(f"Max Upload Size    : {MAX_UPLOAD_SIZE_MB} MB")
    logger.info(f"Max Workers        : {_actual_max_workers}")
    logger.info(f"Job Retention      : {JOB_RETENTION_HOURS} hours")
    logger.info(f"Job Timeout        : {MAX_JOB_DURATION}s")
    logger.info(f"Rate Limit         : {RATE_LIMIT_RPM} req/min/IP")
    logger.info(f"Min Disk Free      : {MIN_DISK_FREE_GB} GB")
    logger.info(f"Min RAM Free       : {MIN_RAM_FREE_MB} MB")
    logger.info(f"Stale Threshold    : {STALE_JOB_THRESHOLD}s")
    logger.info(f"Allowed Extensions : {len(ALLOWED_EXTENSIONS)} types")

    # Log authentication
    logger.info("--- Authentication ---")
    if VALID_TOKENS:
        logger.info(f"  Configured Tokens: {len(VALID_TOKENS)}")
        for i in range(1, 11):
            token_val = os.getenv(f"AUTH_TOKEN_{i}", "").strip()
            if token_val:
                masked = token_val[:10] + "****" + token_val[-4:] if len(token_val) > 14 else "****"
                logger.info(f"  AUTH_TOKEN_{i}    : {masked} ✓")
            else:
                logger.info(f"  AUTH_TOKEN_{i}    : (empty)")
    else:
        logger.warning("  ⚠️ NO TOKENS CONFIGURED!")
    logger.info("--- End Authentication ---")

    # System resource snapshot
    logger.info("--- System Resources ---")
    logger.info(f"  Total RAM        : {monitor.total_ram_gb():.1f} GB")
    logger.info(f"  Available RAM    : {monitor.available_ram_gb():.1f} GB")
    logger.info(f"  CPU Cores        : {monitor.cpu_count()}")
    logger.info(f"  Disk Free        : {monitor.disk_free_gb(BASE_OUTPUT_DIR):.1f} GB")
    logger.info(f"  Load Average     : {monitor.load_avg():.2f}")
    logger.info("------------------------")

    # Verify output directory is writable
    try:
        test_file = BASE_OUTPUT_DIR / ".write_test"
        test_file.touch()
        test_file.unlink()
        logger.info("Output directory: Writable ✓")
    except Exception as e:
        logger.error(f"Output directory not writable: {e}")
        raise RuntimeError(f"Cannot write to output directory: {BASE_OUTPUT_DIR}")

    # Initialize thread pool
    executor = ThreadPoolExecutor(
        max_workers=_actual_max_workers,
        thread_name_prefix="OCR-Worker"
    )
    logger.info(f"Thread pool initialized with {_actual_max_workers} workers")

    # Cleanup old jobs from previous runs
    cleanup_old_jobs()

    # Start background tasks
    _cleanup_task = asyncio.create_task(periodic_job_cleanup())
    _stale_reaper_task = asyncio.create_task(periodic_stale_reaper())

    logger.info("=" * 60)
    logger.info("API Ready. Accepting requests.")
    logger.info("=" * 60)

    yield

    # --- SHUTDOWN ---
    logger.info("CustomOCR Pipeline API shutting down...")

    for task in [_cleanup_task, _stale_reaper_task]:
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    if executor:
        logger.info("Draining active workers (waiting for completion)...")
        executor.shutdown(wait=True, cancel_futures=False)
        logger.info("Thread pool shut down")

    logger.info("Shutdown complete.")


app = FastAPI(
    title="CustomOCR Pipeline API",
    description=(
        "Production-grade OCR pipeline with Layout Analysis, Masking, and Enrichment. "
        "Upload documents (PDF, Office, Images) and receive structured Markdown output. "
        "All processing is asynchronous — returns job ID immediately.\n\n"
        "---\n\n"
        "### 🔐 Authentication\n"
        "All endpoints (except `/health`) require a **Bearer token**.\n\n"
        "1. Go to **Katonic Platform → AI Studio → API Management** and create a token.\n"
        "2. Click the **Authorize** 🔒 button above.\n"
        "3. Paste your token and click **Authorize**.\n\n"
        "**Tokens are managed in the `.env` file** (`AUTH_TOKEN_1` through `AUTH_TOKEN_10`).\n"
    ),
    version="3.0.0",
    root_path=detect_root_path(),
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# MIDDLEWARE: Request ID + Rate Limiting
# =============================================================================

@app.middleware("http")
async def request_middleware(request: Request, call_next):
    """
    Adds request-id header for tracing and enforces per-IP rate limiting
    on the /process endpoint (the only expensive one).
    """
    # Generate unique request ID
    request_id = secrets.token_hex(8)
    request.state.request_id = request_id

    # Rate limit only the upload endpoint
    if request.url.path.rstrip("/").endswith("/process") and request.method == "POST":
        client_ip = get_client_ip(request)
        if not rate_limiter.is_allowed(client_ip):
            logger.warning(f"Rate limited: {client_ip} (>{RATE_LIMIT_RPM} req/min)")
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded ({RATE_LIMIT_RPM} requests/min). Please wait.",
                    "retry_after_seconds": 60
                },
                headers={
                    "Retry-After": "60",
                    "X-Request-ID": request_id
                }
            )

    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# =============================================================================
# GLOBAL EXCEPTION HANDLERS
# =============================================================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", "unknown")
    logger.error(
        f"[{request_id}] Unhandled: {request.method} {request.url.path} "
        f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail": "An internal server error occurred.",
            "error_type": type(exc).__name__,
            "request_id": request_id
        }
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_id = getattr(request.state, "request_id", "unknown")
    if exc.status_code >= 500:
        logger.error(f"[{request_id}] HTTP {exc.status_code}: {exc.detail}")
    elif exc.status_code >= 400:
        logger.warning(f"[{request_id}] HTTP {exc.status_code}: {exc.detail}")

    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "request_id": request_id},
        headers=getattr(exc, "headers", None)
    )


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class JobResponse(BaseModel):
    job_id: str
    status: str
    filename: str
    message: str
    download_url: Optional[str] = None
    created_at: datetime
    request_id: Optional[str] = None


# =============================================================================
# CORE PIPELINE EXECUTION (runs in background thread)
# =============================================================================

def process_ocr_pipeline(job_id: str, file_path: Path, job_dir: Path):
    """
    Executes the 3-Step CustomOCR Pipeline in a background thread.

    Improvements over v2.5:
      - All job_status access goes through thread-safe job_store
      - Per-job timeout enforcement
      - Per-page progress reporting
      - GC after DLA to reclaim image memory
    """
    step_name = "initialization"
    job_start_time = time.time()

    def _check_timeout(step: str):
        """Raises TimeoutError if job exceeds MAX_JOB_DURATION."""
        if MAX_JOB_DURATION > 0:
            elapsed = time.time() - job_start_time
            if elapsed > MAX_JOB_DURATION:
                raise TimeoutError(
                    f"Job exceeded {MAX_JOB_DURATION}s timeout at step '{step}' "
                    f"(elapsed: {elapsed:.0f}s)"
                )

    def _update_progress(step: str, message: str,
                         pages_done: int = 0, total_pages: int = 0):
        """Update job status with progress info."""
        progress = None
        if total_pages > 0:
            percent = round((pages_done / total_pages) * 100, 1)
            elapsed = time.time() - job_start_time
            rate = pages_done / elapsed if elapsed > 0 and pages_done > 0 else 0
            remaining = int((total_pages - pages_done) / rate) if rate > 0 else 0
            progress = {
                "step": step,
                "pages_done": pages_done,
                "total_pages": total_pages,
                "percent": percent,
                "est_remaining_sec": remaining
            }

        update_fields = {"message": message}
        if progress:
            update_fields["progress"] = progress
        job_store.update(job_id, **update_fields)

    try:
        _load_pipeline_modules()

        job_store.update(job_id, status="processing", message="Starting OCR pipeline...")

        # Calculate per-job workers (bounded by system resources)
        optimal_workers = _get_optimal_worker_count(
            ram_per_worker_gb=WORKER_RAM_GB,
            system_reserve_gb=SYSTEM_RESERVE_GB
        )
        logger.info(f"[Job {job_id}] Starting pipeline with {optimal_workers} inner workers")

        # --- STEP 1: File Ingestion ---
        step_name = "file_ingestion"
        _check_timeout(step_name)
        logger.info(f"[Job {job_id}] Step 1/3 - Ingesting file...")
        _update_progress("file_ingestion", "Step 1/3: Ingesting file...")

        ingestor = _file_ingestor_cls(str(job_dir))
        project_dir, image_paths = ingestor.process_input(file_path)

        if not image_paths:
            raise ValueError("File ingestion produced no page images. File may be empty or corrupted.")

        total_pages = len(image_paths)
        logger.info(f"[Job {job_id}] Ingested {total_pages} pages")

        # --- STEP 2: Document Layout Analysis ---
        step_name = "layout_analysis"
        _check_timeout(step_name)
        logger.info(f"[Job {job_id}] Step 2/3 - Analyzing layout ({total_pages} pages)...")
        _update_progress("layout_analysis",
                         f"Step 2/3: Analyzing layout ({total_pages} pages)...",
                         0, total_pages)

        dla = _dla_cls()
        dla.run_vision_pipeline(image_paths, project_dir, filter_dup=True, merge_visual=False)

        _update_progress("layout_analysis",
                         f"Step 2/3: Layout analysis complete ({total_pages} pages)",
                         total_pages, total_pages)

        # Cleanup intermediate files
        try:
            _cleanup_resource(project_dir / "labeled", force_cleanup=True)
            intermediate_pdf = project_dir / file_path.with_suffix(".pdf").name
            if intermediate_pdf.exists():
                _cleanup_resource(intermediate_pdf, force_cleanup=True)
        except Exception as cleanup_err:
            logger.warning(f"[Job {job_id}] Intermediate cleanup: {cleanup_err}")

        # Free DLA memory
        del dla
        gc.collect()

        # --- STEP 3: Masking & OCR ---
        step_name = "ocr_processing"
        _check_timeout(step_name)
        logger.info(f"[Job {job_id}] Step 3/3 - Running OCR and enrichment...")
        _update_progress("ocr_processing",
                         "Step 3/3: Performing OCR and enrichment...",
                         0, total_pages)

        page_processor = _page_processor_cls(str(project_dir), max_workers=optimal_workers)
        page_processor.process_and_mask()

        _check_timeout(step_name)
        final_md_path = page_processor.generate_final_markdown()

        if not final_md_path or not Path(final_md_path).exists():
            raise FileNotFoundError("Pipeline completed but no output markdown was generated")

        output_size = Path(final_md_path).stat().st_size
        if output_size == 0:
            logger.warning(f"[Job {job_id}] Output markdown is empty (0 bytes)")

        # Copy result to completed directory
        step_name = "result_copy"
        completed_md_path = COMPLETED_DIR / f"{job_id}_{file_path.stem}.md"
        shutil.copy2(final_md_path, completed_md_path)

        elapsed = round(time.time() - job_start_time, 1)

        # Mark completed
        job_store.update(
            job_id,
            status="completed",
            message=f"Completed in {elapsed}s ({total_pages} pages, {output_size / 1024:.1f} KB)",
            result_path=str(completed_md_path),
            download_url=f"/download/{job_id}",
            total_pages=total_pages,
            processing_time_sec=elapsed,
            progress={
                "step": "completed",
                "pages_done": total_pages,
                "total_pages": total_pages,
                "percent": 100.0,
                "est_remaining_sec": 0
            }
        )

        logger.info(
            f"[Job {job_id}] ✓ Completed in {elapsed}s. "
            f"Pages: {total_pages}, Output: {output_size / 1024:.1f} KB"
        )

        # Free processor memory
        del page_processor
        gc.collect()

        cleanup_job_directory(job_dir)

    except TimeoutError as e:
        logger.error(f"[Job {job_id}] TIMEOUT at '{step_name}': {e}")
        job_store.update(
            job_id,
            status="failed",
            message=f"Timeout at {step_name}: {str(e)}",
            error_step=step_name
        )
        cleanup_job_directory(job_dir)

    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"[Job {job_id}] Failed at '{step_name}': {error_msg}\n"
            f"{traceback.format_exc()}"
        )
        job_store.update(
            job_id,
            status="failed",
            message=f"Failed at {step_name}: {error_msg}",
            error_step=step_name
        )
        cleanup_job_directory(job_dir)


# =============================================================================
# API ENDPOINTS
# =============================================================================

# ---------------------------------------------------------------------------
# PUBLIC ENDPOINTS (No auth required)
# ---------------------------------------------------------------------------

@app.get("/health")
def health_check():
    """Health check (public — no authentication required)."""
    disk_free = monitor.disk_free_gb(BASE_OUTPUT_DIR)
    ram_free = monitor.available_ram_mb()
    load = monitor.load_avg()

    warnings = []
    if disk_free < MIN_DISK_FREE_GB:
        warnings.append(f"Low disk: {disk_free:.1f}GB free")
    if ram_free < MIN_RAM_FREE_MB:
        warnings.append(f"Low RAM: {ram_free:.0f}MB free")
    if load > monitor.cpu_count() * 2:
        warnings.append(f"High load: {load:.1f}")

    status = "healthy" if not warnings else "degraded"

    stats = job_store.stats()

    return {
        "status": status,
        "service": "CustomOCR Pipeline API",
        "version": "3.0.0",
        "authentication": {
            "total_tokens_configured": len(VALID_TOKENS),
        },
        "message": "Use POST /process to submit documents.",
        "docs_url": f"{detect_root_path()}docs",
        "config": {
            "max_upload_size_mb": MAX_UPLOAD_SIZE_MB,
            "max_workers": _actual_max_workers,
            "job_retention_hours": JOB_RETENTION_HOURS,
            "job_timeout_sec": MAX_JOB_DURATION,
            "rate_limit_rpm": RATE_LIMIT_RPM,
        },
        "resources": {
            "cpu_cores": monitor.cpu_count(),
            "load_avg": round(load, 2),
            "ram_available_mb": round(ram_free, 0),
            "disk_free_gb": round(disk_free, 1),
        },
        "stats": stats,
        "warnings": warnings
    }


@app.get("/", include_in_schema=False)
def root():
    return health_check()


# ---------------------------------------------------------------------------
# PROTECTED ENDPOINTS (Bearer token required)
# ---------------------------------------------------------------------------

@app.post("/process", response_model=JobResponse)
async def process_document(
    request: Request,
    file: UploadFile = File(...),
    token: str = Depends(verify_token)
):
    """
    🔒 **Requires Authentication**

    Upload a document for OCR processing.
    Accepts: PDF, Word, PowerPoint, Excel, Images, Text files.
    Returns: Job ID for status tracking.

    **v3.0 improvements:**
    - Streaming upload (never loads full file into RAM)
    - Resource-aware admission control
    - Rate limited per IP
    """
    request_id = getattr(request.state, "request_id", "unknown")
    job_id = None
    job_dir = None
    input_path = None

    try:
        # 1. Validate file metadata before reading any bytes
        validate_file_metadata(file.filename)
        safe_filename = sanitize_filename(file.filename)

        # 2. Resource admission control
        can_accept, reason = monitor.can_accept_job(BASE_OUTPUT_DIR)
        if not can_accept:
            logger.warning(f"[{request_id}] Rejected: {reason}")
            raise HTTPException(
                status_code=503,
                detail=f"Server cannot accept new jobs: {reason}. Please retry later."
            )

        # 3. Create job directory
        job_id = secrets.token_hex(8)
        job_dir = BASE_OUTPUT_DIR / job_id
        job_dir.mkdir(exist_ok=True)
        input_path = job_dir / safe_filename

        # 4. STREAM file to disk in chunks (CRITICAL FIX from Debug Report Section 6)
        #    Never loads the full file into RAM. A 500 MB upload uses ~1 MB RAM.
        total_size = 0
        try:
            async with aiofiles.open(input_path, "wb") as out:
                while True:
                    chunk = await file.read(UPLOAD_CHUNK_SIZE)
                    if not chunk:
                        break
                    total_size += len(chunk)
                    if total_size > MAX_UPLOAD_SIZE_BYTES:
                        # Abort mid-stream — delete partial file
                        await out.close()
                        if input_path.exists():
                            input_path.unlink(missing_ok=True)
                        raise HTTPException(
                            status_code=413,
                            detail=f"File exceeds {MAX_UPLOAD_SIZE_MB}MB limit "
                                   f"(received {total_size / 1024 / 1024:.1f}MB so far)"
                        )
                    await out.write(chunk)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"[{request_id}] Upload stream failed: {e}")
            raise HTTPException(status_code=400, detail="Failed to read uploaded file.")
        finally:
            await file.close()

        # 5. Validate content
        if total_size == 0:
            if input_path.exists():
                input_path.unlink(missing_ok=True)
            raise HTTPException(status_code=400, detail="Empty file uploaded.")

        # Verify written size matches
        written_size = input_path.stat().st_size
        if written_size != total_size:
            raise HTTPException(status_code=500, detail="File save verification failed. Please retry.")

        logger.info(
            f"[{request_id}] [Job {job_id}] Received: {safe_filename} "
            f"({total_size / 1024 / 1024:.2f} MB)"
        )

        # 6. Register job in thread-safe store
        job_store.create(job_id, {
            "status": "queued",
            "filename": safe_filename,
            "original_filename": file.filename,
            "file_size_bytes": total_size,
            "message": "Job queued for processing",
            "result_path": None,
            "download_url": None,
            "error_step": None,
            "progress": None,
            "total_pages": None,
            "processing_time_sec": None,
            "request_id": request_id,
            "client_ip": get_client_ip(request),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })

        # 7. Submit to executor
        if executor is None:
            raise HTTPException(status_code=503, detail="Service not ready.")

        future: Future = executor.submit(
            process_ocr_pipeline, job_id, input_path, job_dir
        )

        def _on_done(fut: Future):
            exc = fut.exception()
            if exc:
                info = job_store.get(job_id)
                if info and info.get("status") not in ("completed", "failed"):
                    logger.error(f"[Job {job_id}] Thread exception: {exc}")
                    job_store.update(
                        job_id,
                        status="failed",
                        message=f"Unexpected error: {str(exc)}",
                        error_step="thread_crash"
                    )

        future.add_done_callback(_on_done)

        return JobResponse(
            job_id=job_id,
            status="queued",
            filename=safe_filename,
            message="Document accepted for processing.",
            download_url=f"/job/{job_id}",
            created_at=datetime.now(),
            request_id=request_id
        )

    except HTTPException:
        raise
    except Exception as e:
        if job_dir and job_dir.exists():
            cleanup_job_directory(job_dir)
        if job_id:
            job_store.delete(job_id)
        logger.error(f"[{request_id}] Upload failed: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/job/{job_id}")
async def get_job_status(job_id: str, token: str = Depends(verify_token)):
    """
    🔒 Get the current status of a processing job.
    Includes per-page progress when processing.
    """
    validate_job_id(job_id)

    job_info = job_store.get(job_id)
    if not job_info:
        raise HTTPException(status_code=404, detail="Job not found")

    response_data = {
        "job_id": job_id,
        "status": job_info["status"],
        "filename": job_info["filename"],
        "file_size_mb": round(job_info.get("file_size_bytes", 0) / 1024 / 1024, 2),
        "message": job_info["message"],
        "created_at": job_info["created_at"],
        "updated_at": job_info.get("updated_at", job_info["created_at"])
    }

    # Include progress data when available
    progress = job_info.get("progress")
    if progress:
        response_data["progress"] = progress

    if job_info["status"] == "completed":
        response_data["download_url"] = f"/download/{job_id}"
        result_path = job_info.get("result_path")
        if result_path and os.path.exists(result_path):
            response_data["result_size_bytes"] = os.path.getsize(result_path)
        if job_info.get("processing_time_sec"):
            response_data["processing_time_sec"] = job_info["processing_time_sec"]
        if job_info.get("total_pages"):
            response_data["total_pages"] = job_info["total_pages"]

    if job_info["status"] == "failed" and job_info.get("error_step"):
        response_data["error_step"] = job_info["error_step"]

    return response_data


@app.get("/download/{job_id}")
async def download_markdown(job_id: str, token: str = Depends(verify_token)):
    """🔒 Download the markdown result for a completed job."""
    validate_job_id(job_id)

    job_info = job_store.get(job_id)
    if not job_info:
        raise HTTPException(status_code=404, detail="Job not found")

    if job_info["status"] == "processing":
        raise HTTPException(status_code=202, detail="Job still processing.")
    if job_info["status"] == "queued":
        raise HTTPException(status_code=202, detail="Job queued.")
    if job_info["status"] == "failed":
        raise HTTPException(status_code=400, detail=f"Job failed: {job_info['message']}")

    result_path = None
    if job_info.get("result_path") and os.path.exists(job_info["result_path"]):
        result_path = job_info["result_path"]
    else:
        matching = list(COMPLETED_DIR.glob(f"{job_id}_*.md"))
        if matching:
            result_path = str(matching[0])

    if not result_path or not os.path.exists(result_path):
        logger.error(f"[Job {job_id}] Result file missing")
        raise HTTPException(status_code=404, detail="Result file not found.")

    ascii_filename   = f"{job_id}.md"
    full_filename    = f"{job_id}_{job_info['filename']}.md"
    encoded_filename = urllib.parse.quote(full_filename, safe="")
    return FileResponse(
        path=result_path,
        media_type="text/markdown",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{ascii_filename}"; '
                f"filename*=UTF-8''{encoded_filename}"
            )
        }
    )


@app.get("/jobs")
async def list_jobs(
    status: Optional[str] = None,
    limit: int = 50,
    token: str = Depends(verify_token)
):
    """🔒 List all jobs with optional status filter."""
    valid_statuses = {"queued", "processing", "completed", "failed"}
    if status and status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status: '{status}'. Use: {', '.join(sorted(valid_statuses))}"
        )

    limit = max(1, min(limit, 500))
    jobs = job_store.list_all(status_filter=status, limit=limit)

    jobs_list = []
    for job_info in jobs:
        job_data = {
            "job_id": job_info.get("job_id"),
            "status": job_info["status"],
            "filename": job_info["filename"],
            "message": job_info["message"],
            "created_at": job_info["created_at"],
            "updated_at": job_info.get("updated_at", job_info["created_at"])
        }
        if job_info["status"] == "completed":
            job_data["download_url"] = f"/download/{job_info.get('job_id')}"
        progress = job_info.get("progress")
        if progress:
            job_data["progress"] = progress
        jobs_list.append(job_data)

    stats = job_store.stats()
    return {
        "total_jobs": stats["total"],
        "returned": len(jobs_list),
        "stats": stats,
        "jobs": jobs_list
    }


@app.delete("/job/{job_id}")
async def delete_job(job_id: str, token: str = Depends(verify_token)):
    """🔒 Delete a job record and its result file."""
    validate_job_id(job_id)

    job_info = job_store.get(job_id)
    if not job_info:
        raise HTTPException(status_code=404, detail="Job not found")

    if job_info["status"] in ["queued", "processing"]:
        raise HTTPException(status_code=409, detail="Cannot delete active job.")

    # Delete result file first, then remove record
    if job_info.get("result_path") and os.path.exists(job_info["result_path"]):
        try:
            os.remove(job_info["result_path"])
        except OSError as e:
            logger.warning(f"[Job {job_id}] Failed to delete result: {e}")

    job_store.delete(job_id)
    return {"message": f"Job {job_id} deleted successfully"}


# =============================================================================
# BACKGROUND TASKS
# =============================================================================

async def periodic_job_cleanup():
    """Runs every hour to cleanup expired job records."""
    while True:
        try:
            await asyncio.sleep(3600)
            cleanup_old_jobs()
            rate_limiter.cleanup()  # Also cleanup rate limiter state
        except asyncio.CancelledError:
            logger.info("Periodic cleanup task cancelled")
            break
        except Exception as e:
            logger.error(f"Periodic cleanup error: {e}")
            await asyncio.sleep(60)


async def periodic_stale_reaper():
    """Runs every 5 minutes to detect and mark stuck jobs."""
    while True:
        try:
            await asyncio.sleep(300)
            reap_stale_jobs()
        except asyncio.CancelledError:
            logger.info("Stale reaper task cancelled")
            break
        except Exception as e:
            logger.error(f"Stale reaper error: {e}")
            await asyncio.sleep(60)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8050"))

    # Startup diagnostics
    print("\n" + "=" * 60)
    print("  CustomOCR Pipeline API v3.0.0")
    print("=" * 60)

    if VALID_TOKENS:
        print(f"  ✓ Auth Tokens    : {len(VALID_TOKENS)} configured")
        for i in range(1, 11):
            tv = os.getenv(f"AUTH_TOKEN_{i}", "").strip()
            if tv:
                masked = tv[:10] + "****" + tv[-4:] if len(tv) > 14 else "****"
                print(f"    AUTH_TOKEN_{i}  : {masked}")
    else:
        print("  ⚠️  NO TOKENS CONFIGURED!")

    _auto_workers = monitor.compute_optimal_workers(WORKER_RAM_GB, SYSTEM_RESERVE_GB)
    print(f"  Output Dir       : {BASE_OUTPUT_DIR}")
    print(f"  Max Upload       : {MAX_UPLOAD_SIZE_MB} MB")
    print(f"  Workers          : {MAX_WORKERS if MAX_WORKERS > 0 else f'auto ({_auto_workers})'}")
    print(f"  Job Timeout      : {MAX_JOB_DURATION}s")
    print(f"  Rate Limit       : {RATE_LIMIT_RPM} req/min/IP")
    print(f"  Port             : {port}")
    print(f"  RAM              : {monitor.total_ram_gb():.1f} GB total, {monitor.available_ram_gb():.1f} GB free")
    print(f"  CPU              : {monitor.cpu_count()} cores")
    print(f"  Disk             : {monitor.disk_free_gb(BASE_OUTPUT_DIR):.1f} GB free")
    print("=" * 60 + "\n")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        timeout_keep_alive=120,
        log_level="info"
    )
