"""
load_test.py — CustomOCR Pipeline Local Load Test
Sends N jobs simultaneously and tracks all of them to completion.
Usage: python load_test.py --jobs 5 --file inputs/saudi_2030.pdf --token YOUR_TOKEN
"""

import argparse
import time
import json
import threading
import requests
from datetime import datetime
from pathlib import Path

BASE_URL = "http://localhost:8050"


def upload_job(token: str, file_path: str, job_num: int, results: dict):
    """Upload a single file and record the job_id."""
    headers = {"Authorization": f"Bearer {token}"}
    t_start = time.time()
    try:
        with open(file_path, "rb") as f:
            resp = requests.post(
                f"{BASE_URL}/process",
                headers=headers,
                files={"file": (Path(file_path).name, f, "application/pdf")},
                timeout=30,
            )
        elapsed = round(time.time() - t_start, 2)
        if resp.status_code == 200:
            job_id = resp.json()["job_id"]
            print(f"  [Job {job_num}] Uploaded in {elapsed}s → job_id: {job_id}")
            results[job_num] = {"job_id": job_id, "status": "queued", "upload_sec": elapsed}
        else:
            print(f"  [Job {job_num}] Upload FAILED: HTTP {resp.status_code} — {resp.text[:200]}")
            results[job_num] = {"job_id": None, "status": "upload_failed"}
    except Exception as e:
        print(f"  [Job {job_num}] Upload ERROR: {e}")
        results[job_num] = {"job_id": None, "status": "error", "error": str(e)}


def poll_until_done(token: str, job_id: str, job_num: int, timeout: int = 600) -> dict:
    """Poll GET /job/{job_id} until completed or failed."""
    headers = {"Authorization": f"Bearer {token}"}
    start   = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{BASE_URL}/job/{job_id}", headers=headers, timeout=10)
            if resp.status_code == 200:
                data   = resp.json()
                status = data.get("status", "unknown")
                pct    = data.get("progress", {}).get("percent", "—")
                print(f"  [Job {job_num}] {job_id[:8]}... status={status} progress={pct}%")
                if status in ("completed", "failed"):
                    return data
        except Exception as e:
            print(f"  [Job {job_num}] Poll error: {e}")
        time.sleep(5)
    return {"status": "timeout"}


def run_load_test(n_jobs: int, file_path: str, token: str):
    print(f"\n{'='*60}")
    print(f"  CustomOCR Load Test")
    print(f"  Jobs: {n_jobs} | File: {Path(file_path).name}")
    print(f"  Started: {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*60}\n")

    # ----------------------------------------------------------------
    # Phase 1: Upload all N jobs simultaneously using threads
    # ----------------------------------------------------------------
    print(f"[Phase 1] Uploading {n_jobs} jobs simultaneously...")
    upload_results = {}
    threads = []
    t_upload_start = time.time()

    for i in range(1, n_jobs + 1):
        t = threading.Thread(
            target=upload_job,
            args=(token, file_path, i, upload_results),
        )
        threads.append(t)

    # Start all threads at the same time
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    upload_duration = round(time.time() - t_upload_start, 2)
    successful_uploads = [r for r in upload_results.values() if r.get("job_id")]
    print(f"\n  Uploaded {len(successful_uploads)}/{n_jobs} jobs in {upload_duration}s")

    # ----------------------------------------------------------------
    # Phase 2: Watch Redis queue depth
    # ----------------------------------------------------------------
    print(f"\n[Phase 2] Checking Redis queue depth...")
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "ocr-redis", "redis-cli", "llen", "ocr_queue"],
            capture_output=True, text=True
        )
        print(f"  Redis ocr_queue depth right now: {result.stdout.strip()} tasks")
    except Exception:
        print("  (Could not check Redis — verify manually at http://localhost:8081)")

    # ----------------------------------------------------------------
    # Phase 3: Poll all jobs until completion
    # ----------------------------------------------------------------
    print(f"\n[Phase 3] Polling all jobs until completion...")
    t_processing_start = time.time()
    final_results = {}

    poll_threads = []
    for job_num, upload_info in upload_results.items():
        if not upload_info.get("job_id"):
            final_results[job_num] = upload_info
            continue
        job_id = upload_info["job_id"]
        t = threading.Thread(
            target=lambda jn=job_num, jid=job_id: final_results.update(
                {jn: poll_until_done(token, jid, jn)}
            )
        )
        poll_threads.append(t)

    for t in poll_threads:
        t.start()
    for t in poll_threads:
        t.join()

    total_duration = round(time.time() - t_processing_start, 2)

    # ----------------------------------------------------------------
    # Phase 4: Summary report
    # ----------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  LOAD TEST RESULTS")
    print(f"{'='*60}")

    completed = [r for r in final_results.values() if r.get("status") == "completed"]
    failed    = [r for r in final_results.values() if r.get("status") == "failed"]
    timeouts  = [r for r in final_results.values() if r.get("status") == "timeout"]

    print(f"  Total jobs      : {n_jobs}")
    print(f"  Completed       : {len(completed)}")
    print(f"  Failed          : {len(failed)}")
    print(f"  Timeouts        : {len(timeouts)}")
    print(f"  Total wall time : {total_duration}s")

    if completed:
        elapsed_times = [r.get("elapsed_sec", 0) for r in completed if r.get("elapsed_sec")]
        if elapsed_times:
            print(f"  Avg job time    : {round(sum(elapsed_times)/len(elapsed_times), 1)}s")
            print(f"  Fastest job     : {min(elapsed_times)}s")
            print(f"  Slowest job     : {max(elapsed_times)}s")

    print(f"\n  Job details:")
    for job_num in sorted(final_results.keys()):
        r = final_results[job_num]
        uid = upload_results.get(job_num, {}).get("job_id", "N/A")
        st  = r.get("status", "?")
        pg  = r.get("total_pages", "?")
        el  = r.get("elapsed_sec", "?")
        print(f"    Job {job_num}: {uid[:12] if uid else 'N/A'}... | {st} | pages={pg} | time={el}s")

    print(f"\n  Check Flower:   http://localhost:5555/tasks")
    print(f"  Check pgAdmin:  http://localhost:5050")
    print(f"  Check Redis:    http://localhost:8081")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CustomOCR Load Test")
    parser.add_argument("--jobs",  type=int, default=3, help="Number of simultaneous jobs")
    parser.add_argument("--file",  type=str, required=True, help="Path to test PDF file")
    parser.add_argument("--token", type=str, required=True, help="Auth token from .env")
    args = parser.parse_args()
    run_load_test(args.jobs, args.file, args.token)