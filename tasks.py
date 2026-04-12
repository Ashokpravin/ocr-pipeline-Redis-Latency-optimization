"""
================================================================================
tasks.py
CustomOCR Pipeline v4.0 | Celery Task Definition
================================================================================
CHANGES IN THIS VERSION:

FIX 1 - load_dotenv() loads from Path(__file__).parent/.env (not cwd)
FIX 2 - sys.path insert ensures all pipeline modules are importable
FIX 3 - DLA crash-safe wrapper (_run_dla_safe)

NEW — Direct PostgreSQL logging from the worker process
  Previously: Postgres was only written when the client polled GET /job/{id}
              in ocr_app.py. If nobody polled, the row was never inserted.
  Now:        tasks.py writes to Postgres directly on job completion or failure.
              This guarantees every job appears in ocr_jobs regardless of
              whether the client ever polls the status endpoint.
================================================================================
"""

import os
import gc
import sys
import time
import shutil
import logging
import traceback
import threading
import requests as http_requests
from pathlib import Path
from datetime import datetime
from typing import Optional

from celery import Task
from celery.exceptions import SoftTimeLimitExceeded

# ---------------------------------------------------------------------------
# FIX 1: Load .env from the directory where THIS FILE lives.
# ---------------------------------------------------------------------------
from dotenv import load_dotenv
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=True)
else:
    load_dotenv(override=True)

from celery_app import celery_app

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_OUTPUT_DIR   = Path(os.getenv("OCR_OUTPUT_DIR", "./output")).resolve()
COMPLETED_DIR     = BASE_OUTPUT_DIR / "completed"
COMPLETED_DIR.mkdir(parents=True, exist_ok=True)

WORKER_RAM_GB     = float(os.getenv("WORKER_RAM_GB",     "1.5"))
SYSTEM_RESERVE_GB = float(os.getenv("SYSTEM_RESERVE_GB", "4.0"))
MAX_JOB_DURATION  = int(os.getenv("MAX_JOB_DURATION",   "3600"))
WEBHOOK_URL       = os.getenv("WEBHOOK_CALLBACK_URL", "").strip()
POSTGRES_URL      = os.getenv("POSTGRES_URL", "").strip()

# ---------------------------------------------------------------------------
# PostgreSQL — direct write from worker
# ---------------------------------------------------------------------------
_pg_engine = None


def _get_pg_engine():
    """
    Returns a SQLAlchemy engine connected to PostgreSQL.
    Returns None silently if POSTGRES_URL is not set or psycopg2 is missing.
    Engine is cached after first successful connection.
    """
    global _pg_engine
    if _pg_engine is not None:
        return _pg_engine
    if not POSTGRES_URL:
        return None
    try:
        import psycopg2  # noqa: F401 — verify driver is installed
    except ImportError:
        logger.warning("[PostgreSQL] psycopg2 not installed. "
                       "Run: pip install psycopg2-binary")
        return None
    try:
        from sqlalchemy import create_engine, text

        pg_url = POSTGRES_URL
        if pg_url.startswith("postgresql://") and "+psycopg2" not in pg_url:
            pg_url = pg_url.replace("postgresql://", "postgresql+psycopg2://", 1)

        _pg_engine = create_engine(
            pg_url,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=3,
            connect_args={"connect_timeout": 10},
        )
        # Ensure table exists (idempotent)
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
        logger.info("[PostgreSQL] Worker connected. Table ocr_jobs ready.")
        return _pg_engine
    except Exception as e:
        logger.warning("[PostgreSQL] Not available (non-fatal): %s", str(e))
        _pg_engine = None
        return None


def _pg_write_job(job_id: str, filename: str, status: str,
                  total_pages: Optional[int], output_size_bytes: Optional[int],
                  elapsed_sec: Optional[float], error: Optional[str],
                  client_ip: str = ""):
    """
    Writes or updates a job record in PostgreSQL.
    Called directly from the Celery worker on completion or failure.
    Silently skipped if POSTGRES_URL is not set.
    """
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
                     :elapsed_sec, :error, :client_ip, NOW(), :completed_at)
                ON CONFLICT (job_id) DO UPDATE SET
                    status            = EXCLUDED.status,
                    total_pages       = EXCLUDED.total_pages,
                    output_size_bytes = EXCLUDED.output_size_bytes,
                    elapsed_sec       = EXCLUDED.elapsed_sec,
                    error             = EXCLUDED.error,
                    completed_at      = EXCLUDED.completed_at
            """), {
                "job_id":            job_id,
                "filename":          filename,
                "status":            status,
                "total_pages":       total_pages,
                "output_size_bytes": output_size_bytes,
                "elapsed_sec":       elapsed_sec,
                "error":             error,
                "client_ip":         client_ip,
                "completed_at":      datetime.utcnow() if status in ("completed", "failed") else None,
            })
            conn.commit()
        logger.info("[PostgreSQL] Job %s written (status=%s)", job_id, status)
    except Exception as e:
        logger.warning("[PostgreSQL] Write failed for %s (non-fatal): %s", job_id, str(e))


# ---------------------------------------------------------------------------
# Lazy pipeline module loader
# ---------------------------------------------------------------------------
_modules_loaded      = False
_file_ingestor_cls   = None
_dla_cls             = None
_page_processor_cls  = None
_get_optimal_workers = None
_cleanup_resource_fn = None


def _load_pipeline_modules():
    global _modules_loaded, _file_ingestor_cls, _dla_cls
    global _page_processor_cls, _get_optimal_workers, _cleanup_resource_fn

    if _modules_loaded:
        return

    # FIX 2: Ensure project dir is on sys.path
    _project_dir = str(Path(__file__).resolve().parent)
    if _project_dir not in sys.path:
        sys.path.insert(0, _project_dir)
        logger.info("[Celery] Added to sys.path: %s", _project_dir)

    from utils import get_optimal_worker_count, cleanup_resource
    _get_optimal_workers  = get_optimal_worker_count
    _cleanup_resource_fn  = cleanup_resource

    from FileIngestor import FileIngestor
    _file_ingestor_cls = FileIngestor

    from DLA import DLA
    _dla_cls = DLA

    from PageProcessor import PageProcessor
    _page_processor_cls = PageProcessor

    _modules_loaded = True
    logger.info("[Celery Worker] All pipeline modules loaded.")


# ---------------------------------------------------------------------------
# FIX 3: DLA crash-safe wrapper
# ---------------------------------------------------------------------------
# Module-level lock: serialises DLA across all greenlets in this worker process.
# PaddleOCR C++ inference is NOT thread-safe — concurrent calls cause segfault.
# Each job gets its own DLA instance, but the lock ensures only one runs at a time.


def _run_dla_safe(job_id: str, image_paths: list, project_dir: Path,
                  total_pages: int) -> bool:
    """
    Runs DLA without any lock – safe because each prefork process is isolated.
    """
    try:
        dla = _dla_cls()
        dla.run_vision_pipeline(
            image_paths, project_dir,
            filter_dup=True, merge_visual=False,
        )
        # Cleanup labeled directory (optional, saves space)
        try:
            _cleanup_resource_fn(project_dir / "labeled", force_cleanup=True)
        except Exception:
            pass
        del dla
        gc.collect()
        logger.info("[Job %s] DLA completed successfully.", job_id)
        return True

    except Exception as dla_exc:
        logger.warning(
            "[Job %s] DLA failed — continuing with full-page OCR.\nReason: %s\n%s",
            job_id, str(dla_exc), traceback.format_exc(),
        )
        _create_empty_dla_skeleton(project_dir, image_paths)
        gc.collect()
        return False


def _create_empty_dla_skeleton(project_dir: Path, image_paths: list):
    """
    Empty dirs + JSON so PageProcessor can run without DLA output.
    ContentMasker reads non_text_pairs.json — empty list = no masking.
    """
    try:
        import json
        for d in ["ignore_bounding_box", "cropped_objects", "labeled"]:
            (project_dir / d).mkdir(parents=True, exist_ok=True)
        for i in range(len(image_paths)):
            page_dir = project_dir / "ignore_bounding_box" / f"page_{i}"
            page_dir.mkdir(parents=True, exist_ok=True)
            jp = page_dir / "non_text_pairs.json"
            if not jp.exists():
                with open(jp, "w", encoding="utf-8") as f:
                    json.dump([], f)
        logger.info("[DLA skeleton] Created empty skeleton for %d pages.", len(image_paths))
    except Exception as e:
        logger.warning("[DLA skeleton] Failed (non-fatal): %s", str(e))


# ---------------------------------------------------------------------------
# Webhook helper
# ---------------------------------------------------------------------------
def _post_webhook(job_id: str, status: str, filename: str,
                  output_path: Optional[str], error: Optional[str]):
    if not WEBHOOK_URL:
        return
    payload = {
        "job_id": job_id, "status": status, "filename": filename,
        "output_path": output_path, "error": error,
        "timestamp": datetime.utcnow().isoformat(),
    }
    try:
        resp = http_requests.post(WEBHOOK_URL, json=payload, timeout=10)
        logger.info("[Webhook] %s notified (%d)", status, resp.status_code)
    except Exception as e:
        logger.warning("[Webhook] Notification failed (non-fatal): %s", str(e))


# ---------------------------------------------------------------------------
# Custom Celery Task base class
# ---------------------------------------------------------------------------
class OCRTask(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error("[Celery] Task %s FAILED: %s", task_id, str(exc))
        if args and len(args) >= 2:
            job_id   = args[0]
            filename = Path(args[1]).name
            _post_webhook(job_id, "failed", filename, None, str(exc))
            # Write failure to Postgres directly from the worker
            _pg_write_job(
                job_id=job_id, filename=filename, status="failed",
                total_pages=None, output_size_bytes=None,
                elapsed_sec=None, error=str(exc),
            )

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        logger.warning("[Celery] Task %s retrying: %s", task_id, str(exc))


# ---------------------------------------------------------------------------
# CORE OCR PIPELINE TASK
# ---------------------------------------------------------------------------
@celery_app.task(
    bind=True,
    base=OCRTask,
    name="tasks.process_document_task",
    max_retries=3,
    default_retry_delay=30,
    acks_late=True,
    reject_on_worker_lost=True,
    time_limit=600,
    soft_time_limit=540,
)
def process_document_task(
    self,
    job_id: str,
    file_path_str: str,
    job_dir_str: str,
    original_filename: str = "",
) -> dict:
    """
    Full 3-step OCR pipeline as a Celery task.

    Step 1 — FileIngestor  : document to page images
    Step 2 — DLA           : layout analysis  [crash-safe on Windows]
    Step 3 — PageProcessor : masking + OCR + enrichment

    Writes result to PostgreSQL directly on completion or failure.
    """
    _load_pipeline_modules()

    file_path = Path(file_path_str)
    job_dir   = Path(job_dir_str)
    step_name = "initialization"
    job_start = time.time()
    filename  = original_filename or file_path.name

    def _elapsed() -> float:
        return round(time.time() - job_start, 2)

    def _check_timeout(step: str):
        if MAX_JOB_DURATION > 0 and _elapsed() > MAX_JOB_DURATION:
            raise TimeoutError(
                f"Job exceeded {MAX_JOB_DURATION}s at step '{step}' "
                f"(elapsed: {_elapsed():.0f}s)"
            )

    def _progress(step: str, message: str,
                  pages_done: int = 0, total_pages: int = 0):
        meta = {"step": step, "message": message, "elapsed_sec": _elapsed()}
        if total_pages > 0:
            meta.update({
                "pages_done":  pages_done,
                "total_pages": total_pages,
                "percent":     round((pages_done / total_pages) * 100, 1),
            })
        self.update_state(state="PROGRESS", meta=meta)

    try:
        optimal_workers = _get_optimal_workers(
            ram_per_worker_gb=WORKER_RAM_GB,
            system_reserve_gb=SYSTEM_RESERVE_GB,
        )
        logger.info("[Job %s] Starting | workers=%d | file=%s | output_dir=%s",
                    job_id, optimal_workers, file_path.name, str(BASE_OUTPUT_DIR))

        # =================================================================
        # STEP 1 — File Ingestion
        # =================================================================
        step_name = "file_ingestion"
        _check_timeout(step_name)
        _progress(step_name, "Step 1/3: Ingesting file...")
        logger.info("[Job %s] Step 1/3 — Ingesting...", job_id)

        ingestor    = _file_ingestor_cls(str(job_dir))
        project_dir, image_paths = ingestor.process_input(file_path)

        if not image_paths:
            raise ValueError(
                "File ingestion produced no page images. "
                "File may be empty or corrupted."
            )
        total_pages = len(image_paths)
        logger.info("[Job %s] Ingested %d pages", job_id, total_pages)

        # =================================================================
        # STEP 2 — Document Layout Analysis
        # =================================================================
        step_name = "layout_analysis"
        _check_timeout(step_name)
        _progress(step_name,
                  f"Step 2/3: Layout analysis ({total_pages} pages)...",
                  0, total_pages)
        logger.info("[Job %s] Step 2/3 — DLA (%d pages)...", job_id, total_pages)

        dla_succeeded = _run_dla_safe(job_id, image_paths, project_dir, total_pages)

        if dla_succeeded:
            _progress(step_name,
                      f"Step 2/3: Layout complete ({total_pages} pages)",
                      total_pages, total_pages)
            try:
                intermediate_pdf = project_dir / file_path.with_suffix(".pdf").name
                if intermediate_pdf.exists():
                    _cleanup_resource_fn(intermediate_pdf, force_cleanup=True)
            except Exception as ce:
                logger.warning("[Job %s] Cleanup: %s", job_id, str(ce))
        else:
            _progress(step_name,
                      f"Step 2/3: Layout skipped — full-page OCR ({total_pages} pages)",
                      total_pages, total_pages)

        # =================================================================
        # STEP 3 — Masking + OCR + Enrichment
        # =================================================================
        step_name = "ocr_processing"
        _check_timeout(step_name)
        _progress(step_name, "Step 3/3: OCR and enrichment...", 0, total_pages)
        logger.info("[Job %s] Step 3/3 — OCR & enrichment...", job_id)
        

        page_processor = _page_processor_cls(str(project_dir), max_workers=optimal_workers)
        page_processor.process_and_mask()

        _check_timeout(step_name)
        final_md_path = page_processor.generate_final_markdown()

        if not final_md_path or not Path(final_md_path).exists():
            raise FileNotFoundError("Pipeline completed but no output markdown was generated.")

        output_size = Path(final_md_path).stat().st_size
        if output_size == 0:
            logger.warning("[Job %s] Output markdown is empty (0 bytes)", job_id)

        # =================================================================
        # Copy result + cleanup
        # =================================================================
        step_name    = "result_copy"
        completed_md = COMPLETED_DIR / f"{job_id}_{file_path.stem}.md"
        shutil.copy2(final_md_path, completed_md)

        del page_processor
        gc.collect()
        _cleanup_job_dir(job_dir)

        elapsed = _elapsed()

        # -----------------------------------------------------------------
        # Write SUCCESS record to PostgreSQL directly from the worker.
        # This guarantees the row exists regardless of client polling.
        # -----------------------------------------------------------------
        _pg_write_job(
            job_id=job_id,
            filename=filename,
            status="completed",
            total_pages=total_pages,
            output_size_bytes=output_size,
            elapsed_sec=elapsed,
            error=None,
        )

        result = {
            "status":            "completed",
            "output_path":       str(completed_md),
            "download_url":      f"/download/{job_id}",
            "completed_at":      datetime.utcnow().isoformat(),
            "total_pages":       total_pages,
            "output_size_bytes": output_size,
            "elapsed_sec":       elapsed,
            "dla_succeeded":     dla_succeeded,
            "error":             None,
        }
        logger.info("[Job %s] Done in %.1fs | DLA=%s | pages=%d | size=%d bytes",
                    job_id, elapsed,
                    "OK" if dla_succeeded else "SKIPPED",
                    total_pages, output_size)
        _post_webhook(job_id, "completed", filename, str(completed_md), None)
        return result

    except SoftTimeLimitExceeded:
        err = f"Soft time limit exceeded at step '{step_name}'"
        logger.error("[Job %s] %s", job_id, err)
        _cleanup_job_dir(job_dir)
        # Write to Postgres before retrying
        _pg_write_job(job_id=job_id, filename=filename, status="failed",
                      total_pages=None, output_size_bytes=None,
                      elapsed_sec=_elapsed(), error=err)
        raise self.retry(exc=SoftTimeLimitExceeded(err), countdown=60)

    except TimeoutError as exc:
        logger.error("[Job %s] TIMEOUT at '%s': %s", job_id, step_name, str(exc))
        _cleanup_job_dir(job_dir)
        _pg_write_job(job_id=job_id, filename=filename, status="failed",
                      total_pages=None, output_size_bytes=None,
                      elapsed_sec=_elapsed(), error=str(exc))
        raise

    except Exception as exc:
        err_msg = str(exc)
        logger.error("[Job %s] FAILED at '%s': %s\n%s",
                     job_id, step_name, err_msg, traceback.format_exc())
        _cleanup_job_dir(job_dir)

        # Write FAILURE to Postgres directly from the worker
        _pg_write_job(
            job_id=job_id,
            filename=filename,
            status="failed",
            total_pages=None,
            output_size_bytes=None,
            elapsed_sec=_elapsed(),
            error=f"[{step_name}] {err_msg}",
        )

        transient = ["timeout", "connection", "503", "502", "network", "rate"]
        if (any(k in err_msg.lower() for k in transient)
                and self.request.retries < self.max_retries):
            backoff = 30 * (2 ** self.request.retries)
            logger.info("[Job %s] Transient — retry in %ds (%d/%d)",
                        job_id, backoff, self.request.retries + 1, self.max_retries)
            raise self.retry(exc=exc, countdown=backoff)

        _post_webhook(job_id, "failed", filename, None, err_msg)
        raise


def _cleanup_job_dir(directory: Path):
    try:
        if directory and directory.exists():
            shutil.rmtree(directory)
            logger.info("[Cleanup] Removed: %s", directory.name)
    except Exception as e:
        logger.warning("[Cleanup] Failed for %s: %s", str(directory), str(e))