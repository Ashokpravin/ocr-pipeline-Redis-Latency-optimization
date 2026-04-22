"""
================================================================================
celery_entry.py  —  Katonic entry point for Celery Worker Deployment (v3.2)
================================================================================
HOW TO USE:
  In Katonic App Deployment UI:
    Main file path: celery_entry     (NOT startup_celery.sh)
    Port:           8050             (doesn't matter — Celery doesn't serve HTTP)
    Environment:    DEPLOY_TYPE=worker, REDIS_URL=redis://..., CELERY_CONCURRENCY=8

FIXES IN v3.2:
  1. Sets DEPLOY_TYPE=worker so marker file (.deps_ok_worker) doesn't conflict with API
  2. Verifies Redis is reachable before starting Celery worker (prevents crash loop)
  3. Logs Redis and model status clearly for debugging
================================================================================
"""

import os
import sys
import time

# =============================================================================
# FIX 1: Set DEPLOY_TYPE BEFORE importing _setup
# This ensures _setup() uses .deps_ok_worker marker instead of .deps_ok_api
# =============================================================================
os.environ["DEPLOY_TYPE"] = "worker"

# Step 1: Run full dependency setup (same as app.py)
try:
    from app import _setup
    _setup()
    print("[celery_entry] Dependency setup complete", flush=True)
except Exception as e:
    print(f"[celery_entry] Setup error (continuing): {e}", flush=True)

# =============================================================================
# FIX 2: Verify Redis is reachable before starting worker
# Prevents CrashLoopBackOff if Redis isn't ready yet
# =============================================================================
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_result_url = os.getenv("REDIS_RESULT_URL", "redis://localhost:6379/1")

print(f"[celery_entry] Checking Redis: {redis_url}", flush=True)

redis_ready = False
for attempt in range(15):  # Wait up to 75 seconds
    try:
        import redis
        r = redis.from_url(redis_url, socket_connect_timeout=5)
        r.ping()
        print(f"[celery_entry] Redis OK (attempt {attempt + 1})", flush=True)
        redis_ready = True
        break
    except Exception as e:
        print(f"[celery_entry] Waiting for Redis (attempt {attempt + 1}/15): {e}", flush=True)
        time.sleep(5)

if not redis_ready:
    print("[celery_entry] WARNING: Redis not reachable after 75s. Worker may fail to start.", flush=True)
    print("[celery_entry] Check that REDIS_URL is correct and Redis service is running.", flush=True)

# =============================================================================
# Step 2: Verify PaddleX model is present
# =============================================================================
paddlex_home = os.environ.get("PADDLEX_HOME", os.path.expanduser("~/.paddlex"))
model_yml = os.path.join(paddlex_home, "official_models", "PP-DocLayout_plus-L", "inference.yml")
if os.path.exists(model_yml):
    print(f"[celery_entry] PaddleX model: present", flush=True)
else:
    print(f"[celery_entry] PaddleX model: MISSING at {model_yml}", flush=True)
    print(f"[celery_entry] The model will be downloaded on first task (may cause slow first job)", flush=True)

# =============================================================================
# Step 3: Replace this process with Celery worker
# =============================================================================
concurrency = os.getenv("CELERY_CONCURRENCY", "2")

print(f"[celery_entry] Starting Celery worker (prefork, concurrency={concurrency})", flush=True)
print(f"[celery_entry] REDIS_URL={redis_url}", flush=True)
print(f"[celery_entry] REDIS_RESULT_URL={redis_result_url}", flush=True)

# os.execvp replaces the current Python process entirely with the Celery worker
os.execvp(sys.executable, [
    sys.executable, "-m", "celery",
    "-A", "celery_app", "worker",
    "--pool=prefork",
    f"--concurrency={concurrency}",
    "--loglevel=info",
    "-Q", "ocr_queue",
])

# =============================================================================
# Fallback: Katonic needs an "app" object if execvp somehow fails
# os.execvp above replaces this process before uvicorn ever tries to import app.
# This code is unreachable under normal operation.
# =============================================================================
from fastapi import FastAPI
app = FastAPI(title="Celery Worker Entry")

@app.get("/")
def status():
    return {"status": "celery_worker", "note": "This endpoint should never be reached"}
