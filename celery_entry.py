# celery_entry.py
# Katonic entry point for Celery worker deployment
# Main file path in Katonic: celery_entry

import os
import subprocess
import sys

# Run the same dependency setup as app.py
from app import _setup
_setup()

# Now start Celery worker
concurrency = os.getenv("CELERY_CONCURRENCY", "8")
os.execvp("celery", [
    "celery", "-A", "celery_app", "worker",
    "--pool=prefork",
    f"--concurrency={concurrency}",
    "--loglevel=info",
    "-Q", "ocr_queue"
])