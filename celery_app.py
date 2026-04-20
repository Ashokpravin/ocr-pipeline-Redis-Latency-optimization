"""
================================================================================
celery_app.py
CustomOCR Pipeline v4.1 | Celery + Redis Application Configuration
================================================================================
FIX v4.1: Added Redis connection pool caps and disabled gossip/mingle/events.
  Without these caps, 8 prefork workers flood Redis with connections on startup,
  causing the Redis pod to OOM-crash, which then cascades and kills FastAPI too.
================================================================================
"""

import os
from pathlib import Path
from celery import Celery
from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=True)
else:
    load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Redis connections
# ---------------------------------------------------------------------------
REDIS_URL          = os.getenv("REDIS_URL",          "redis://redis.app-dependencies.svc.cluster.local:6379/0")
REDIS_RESULT_URL   = os.getenv("REDIS_RESULT_URL",   "redis://redis.app-dependencies.svc.cluster.local:6379/1")
CELERY_CONCURRENCY = int(os.getenv("CELERY_CONCURRENCY", "2"))

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

    # =========================================================================
    # REDIS OOM FIXES — these 5 lines prevent the Redis pod from being killed
    # =========================================================================

    # 1. Cap the total number of Redis connections this worker can hold open.
    #    Without this, 8 prefork workers each open unlimited connections and
    #    the Redis pod runs out of memory and is killed by Kubernetes.
    redis_max_connections=20,
    broker_pool_limit=10,

    # 2. Disable worker-to-worker gossip and the mingle startup handshake.
    #    With only one worker pod these generate pure Redis traffic overhead.
    worker_disable_mingle=True,

    # 3. Disable real-time task state events.
    #    Events cause every worker subprocess to continuously write heartbeat
    #    records to Redis, causing memory spikes on the Redis pod.
    #    NOTE: disabling this means Flower won't show live task progress,
    #    but your pipeline will be stable. Re-enable if you need Flower.
    worker_send_task_events=False,
    task_send_sent_event=False,

    # 4. Retry broker connection on startup instead of crashing immediately.
    broker_connection_retry_on_startup=True,
    # =========================================================================
)

if __name__ == "__main__":
    celery_app.start()