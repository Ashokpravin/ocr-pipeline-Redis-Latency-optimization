"""
================================================================================
celery_entry.py  —  Katonic entry point for Celery Worker Deployment
================================================================================
HOW TO USE:
  In Katonic App Deployment UI:
    Main file path: celery_entry     (NOT startup_celery.sh)
    Port:           8050             (doesn't matter — Celery doesn't serve HTTP)
    PVC mount:      /app/output      (SAME PVC as FastAPI deployment)
    Public URL:     No

WHY THIS EXISTS:
  Katonic's "Main file path" runs uvicorn <module>:app — it expects a Python
  module, not a shell script. This file runs all the same dependency setup as
  app.py and then replaces itself with the Celery worker process.
================================================================================
"""

import os
import sys

# ── Step 1: Run full dependency setup (same as app.py) ───────────────────────
# This installs headless OpenCV, LibreOffice, downloads PaddleX model, etc.
try:
    from app import _setup
    _setup()
    print("[celery_entry] ✓ Dependency setup complete", flush=True)
except Exception as e:
    print(f"[celery_entry] ⚠ Setup error (continuing): {e}", flush=True)

# Install xvfb for headless X11 support
_run("apt-get install -y -qq xvfb")   

# ── Step 2: Replace this process with Celery worker ──────────────────────────
# os.execvp replaces the current Python process entirely with the Celery worker.
# The worker inherits all environment variables set in Katonic UI.
concurrency = os.getenv("CELERY_CONCURRENCY", "8")

print(f"[celery_entry] Starting Celery worker (prefork, concurrency={concurrency})", flush=True)
print(f"[celery_entry] REDIS_URL={os.getenv('REDIS_URL', 'NOT SET')}", flush=True)
print(f"[celery_entry] REDIS_RESULT_URL={os.getenv('REDIS_RESULT_URL', 'NOT SET')}", flush=True)

os.execvp(sys.executable, [
    sys.executable, "-m", "celery",
    "-A", "celery_app", "worker",
    "--pool=prefork",
    f"--concurrency={concurrency}",
    "--loglevel=info",
    "-Q", "ocr_queue",
])

# ── Fallback: Katonic needs an "app" object if execvp somehow fails ───────────
# os.execvp above replaces this process before uvicorn ever tries to import app.
# This code is unreachable under normal operation.
from fastapi import FastAPI
app = FastAPI(title="Celery Worker Entry")

@app.get("/")
def status():
    return {"status": "celery_worker", "note": "This endpoint should never be reached"}