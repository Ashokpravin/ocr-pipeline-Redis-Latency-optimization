"""
================================================================================
celery_app.py
CustomOCR Pipeline v4.0 | Celery + Redis Application Configuration
================================================================================
FIX: load_dotenv() now uses the path of THIS FILE (celery_app.py) to locate
     the .env file, instead of relying on the current working directory.
     This means Celery can be started from any directory and will always
     find the correct .env with OCR_OUTPUT_DIR=./output.

DEPLOYMENT:
  App 1 (FastAPI):       uvicorn ocr_app:app --host 0.0.0.0 --port 8050
  App 2 (Celery Worker): celery -A celery_app worker --pool=solo --concurrency=1 --loglevel=info -Q ocr_queue
  App 3 (Flower):        celery -A celery_app flower --port=5555

CRITICAL: NEVER run Celery gevent pool in same process as FastAPI.
================================================================================
"""

import os
from pathlib import Path
from celery import Celery
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# FIX: Load .env from the directory where THIS FILE lives, not from cwd.
# Path(__file__) = .../ocr-pipeline-fastapi-.../celery_app.py
# .parent        = .../ocr-pipeline-fastapi-.../   <- where .env lives
# This works regardless of which directory Celery is started from.
# ---------------------------------------------------------------------------
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=True)
else:
    load_dotenv(override=True)   # fallback to cwd search

# ---------------------------------------------------------------------------
# Redis connections
# ---------------------------------------------------------------------------
REDIS_URL          = os.getenv("REDIS_URL",          "redis://redis.app-dependencies.svc.cluster.local:6379/0")
REDIS_RESULT_URL   = os.getenv("REDIS_RESULT_URL",   "redis://redis.app-dependencies.svc.cluster.local:6379/1")
CELERY_CONCURRENCY = int(os.getenv("CELERY_CONCURRENCY", "8"))

# ---------------------------------------------------------------------------
# Celery application
# ---------------------------------------------------------------------------
celery_app = Celery(
    "ocr_pipeline",
    broker=REDIS_URL,
    backend=REDIS_RESULT_URL,
    include=["tasks"],
)

celery_app.conf.update(
    worker_concurrency=CELERY_CONCURRENCY,
    worker_pool="prefork",
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_time_limit=600,
    task_soft_time_limit=540,
    task_reject_on_worker_lost=True,
    worker_max_tasks_per_child=50,
    result_expires=86400,
    task_ignore_result=False,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_default_queue="ocr_queue",
    task_routes={
        "tasks.process_document_task": {"queue": "ocr_queue"},
    },
)

if __name__ == "__main__":
    celery_app.start()