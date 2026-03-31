"""
================================================================================
load_test_concurrent.py — CustomOCR Concurrency Comparison Load Test
================================================================================
Tests 3 scenarios back to back and compares throughput:
  Scenario A: Serial baseline   (1 job at a time, as measured before)
  Scenario B: Medium concurrency (4-8 jobs in parallel)
  Scenario C: High concurrency  (10-20 jobs in parallel)

For each scenario it measures:
  - Total wall time
  - Throughput (jobs/hour, pages/hour)
  - Queue wait time per job (time from upload to processing start)
  - Average OCR time per job
  - Model API utilisation (are we saturating the API?)

Usage:
  python load_test_concurrent.py --file inputs/saudi_2030.pdf --token YOUR_TOKEN
  python load_test_concurrent.py --file inputs/saudi_2030.pdf --token YOUR_TOKEN --scenarios A,B,C
  python load_test_concurrent.py --file inputs/saudi_2030.pdf --token YOUR_TOKEN --custom 15

  # To run only one scenario for quick test:
  python load_test_concurrent.py --file inputs/saudi_2030.pdf --token YOUR_TOKEN --scenarios B

IMPORTANT — Before running this:
  1. Stop your current Celery worker (Ctrl+C)
  2. The script will print the correct Celery restart command for each scenario
  3. You restart Celery with the right concurrency setting
  4. Then run the scenario

Why --pool=threads on Windows (not gevent):
  gevent monkey-patches the entire Python runtime at startup. On Windows this
  breaks asyncio and causes semaphore errors. --pool=threads uses real OS
  threads — safe on Windows, same concurrency effect for I/O-bound OCR calls.
  On Katonic Linux: use --pool=gevent (no monkey-patch issue there).
================================================================================
"""

import argparse
import statistics
import threading
import time
from datetime import datetime
from pathlib import Path

import requests

BASE_URL = "http://localhost:8050"


# =============================================================================
# CORE WORKERS
# =============================================================================

def upload_one(token, file_path, job_num, results, lock):
    headers = {"Authorization": f"Bearer {token}"}
    t0 = time.time()
    try:
        with open(file_path, "rb") as f:
            r = requests.post(
                f"{BASE_URL}/process", headers=headers,
                files={"file": (Path(file_path).name, f, "application/pdf")},
                timeout=30,
            )
        elapsed = round(time.time() - t0, 3)
        if r.status_code == 200:
            jid = r.json()["job_id"]
            with lock:
                print(f"  UPLOAD {job_num:03d} | OK {elapsed}s | {jid}")
            results[job_num] = {"job_id": jid, "upload_sec": elapsed,
                                "upload_ts": time.time(), "status": "queued"}
        else:
            with lock:
                print(f"  UPLOAD {job_num:03d} | FAIL HTTP {r.status_code}")
            results[job_num] = {"job_id": None, "status": "upload_failed",
                                "error": r.text[:200]}
    except Exception as e:
        with lock:
            print(f"  UPLOAD {job_num:03d} | ERROR {e}")
        results[job_num] = {"job_id": None, "status": "error", "error": str(e)}


def poll_one(token, job_id, job_num, final_results, lock, poll_sec=3, timeout=600):
    headers  = {"Authorization": f"Bearer {token}"}
    t_start  = time.time()
    first_processing_ts = None

    while time.time() - t_start < timeout:
        try:
            r = requests.get(f"{BASE_URL}/job/{job_id}", headers=headers, timeout=10)
            if r.status_code == 200:
                d      = r.json()
                status = d.get("status", "unknown")

                # Record first time we see processing (queue wait ends here)
                if status == "processing" and first_processing_ts is None:
                    first_processing_ts = time.time()
                    with lock:
                        print(f"  POLL   {job_num:03d} | {job_id[:10]}... STARTED processing")

                if status in ("completed", "failed"):
                    d["first_processing_ts"] = first_processing_ts
                    final_results[job_num] = d
                    with lock:
                        el = d.get("elapsed_sec", "?")
                        print(f"  POLL   {job_num:03d} | {job_id[:10]}... {status.upper()} ({el}s)")
                    return
        except Exception as e:
            with lock:
                print(f"  POLL   {job_num:03d} | poll error: {e}")
        time.sleep(poll_sec)

    final_results[job_num] = {"status": "timeout", "job_id": job_id}
    with lock:
        print(f"  POLL   {job_num:03d} | TIMEOUT after {timeout}s")


# =============================================================================
# SCENARIO RUNNER
# =============================================================================

def run_scenario(name: str, n_jobs: int, file_path: str, token: str,
                 poll_sec: int = 3) -> dict:
    sep = "-" * 60
    print(f"\n{sep}")
    print(f"  SCENARIO {name} | {n_jobs} simultaneous jobs")
    print(f"  Time: {datetime.now().strftime('%H:%M:%S')}")
    print(sep)

    lock           = threading.Lock()
    upload_results = {}
    final_results  = {}

    # --- Upload all jobs simultaneously ---
    print(f"\n  Uploading {n_jobs} jobs simultaneously...")
    t_upload = time.time()
    uthreads = [
        threading.Thread(
            target=upload_one,
            args=(token, file_path, i, upload_results, lock),
            daemon=True,
        )
        for i in range(1, n_jobs + 1)
    ]
    for t in uthreads: t.start()
    for t in uthreads: t.join()

    upload_duration = round(time.time() - t_upload, 2)
    successful      = [(n, r) for n, r in upload_results.items() if r.get("job_id")]
    upload_ts_map   = {n: r["upload_ts"] for n, r in successful}

    print(f"\n  {len(successful)}/{n_jobs} uploaded in {upload_duration}s")

    if not successful:
        return {"scenario": name, "n_jobs": n_jobs, "completed": 0, "failed": n_jobs}

    # --- Pre-fill failures ---
    for n, r in upload_results.items():
        if not r.get("job_id"):
            final_results[n] = r

    # --- Poll all jobs concurrently ---
    print(f"\n  Polling {len(successful)} jobs (every {poll_sec}s)...")
    t_wall_start = time.time()

    pthreads = [
        threading.Thread(
            target=poll_one,
            args=(token, r["job_id"], n, final_results, lock, poll_sec),
            daemon=True,
        )
        for n, r in successful
    ]
    for t in pthreads: t.start()
    for t in pthreads: t.join()

    wall_time = round(time.time() - t_wall_start, 2)

    # --- Compute metrics ---
    completed    = [r for r in final_results.values() if r.get("status") == "completed"]
    failed       = [r for r in final_results.values() if r.get("status") == "failed"]
    timeouts_    = [r for r in final_results.values() if r.get("status") == "timeout"]

    ocr_times    = [r["elapsed_sec"] for r in completed if r.get("elapsed_sec")]
    page_counts  = [r["total_pages"]  for r in completed if r.get("total_pages")]

    # Queue wait = time from upload to first "processing" state
    queue_waits  = []
    for n, r in final_results.items():
        fp_ts = r.get("first_processing_ts")
        up_ts = upload_ts_map.get(n)
        if fp_ts and up_ts:
            queue_waits.append(round(fp_ts - up_ts, 2))

    total_pages  = sum(page_counts)
    throughput_jobs_hr  = round(len(completed) / wall_time * 3600, 0) if wall_time > 0 else 0
    throughput_pages_hr = round(total_pages / wall_time * 3600, 0)    if wall_time > 0 else 0

    metrics = {
        "scenario":            name,
        "n_jobs":              n_jobs,
        "completed":           len(completed),
        "failed":              len(failed),
        "timeouts":            len(timeouts_),
        "wall_time_sec":       wall_time,
        "throughput_jobs_hr":  throughput_jobs_hr,
        "throughput_pages_hr": throughput_pages_hr,
        "total_pages":         total_pages,
        "ocr_avg_sec":         round(statistics.mean(ocr_times),   1) if ocr_times   else None,
        "ocr_min_sec":         min(ocr_times)                          if ocr_times   else None,
        "ocr_max_sec":         max(ocr_times)                          if ocr_times   else None,
        "ocr_stdev_sec":       round(statistics.stdev(ocr_times),  1) if len(ocr_times) > 1 else None,
        "queue_wait_avg_sec":  round(statistics.mean(queue_waits), 1) if queue_waits else None,
        "queue_wait_max_sec":  max(queue_waits)                        if queue_waits else None,
    }

    print(f"\n  Scenario {name} complete | {len(completed)}/{n_jobs} OK | wall={wall_time}s")
    return metrics


# =============================================================================
# PRINT COMPARISON TABLE
# =============================================================================

def print_comparison(results: list):
    sep = "=" * 62
    print(f"\n{sep}")
    print(f"  CONCURRENCY COMPARISON REPORT")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(sep)

    # Header
    print(f"\n  {'Metric':<28} ", end="")
    for r in results:
        print(f"  {r['scenario']:>12}", end="")
    print()
    print(f"  {'-'*28} ", end="")
    for _ in results:
        print(f"  {'------------':>12}", end="")
    print()

    rows = [
        ("Concurrent jobs",         "n_jobs",              "{}"),
        ("Completed",               "completed",           "{}"),
        ("Failed",                  "failed",              "{}"),
        ("Total wall time (s)",     "wall_time_sec",       "{}s"),
        ("Throughput (jobs/hr)",    "throughput_jobs_hr",  "{}"),
        ("Throughput (pages/hr)",   "throughput_pages_hr", "{}"),
        ("OCR avg per job (s)",     "ocr_avg_sec",         "{}s"),
        ("OCR min (s)",             "ocr_min_sec",         "{}s"),
        ("OCR max (s)",             "ocr_max_sec",         "{}s"),
        ("OCR stdev (s)",           "ocr_stdev_sec",       "{}s"),
        ("Queue wait avg (s)",      "queue_wait_avg_sec",  "{}s"),
        ("Queue wait max (s)",      "queue_wait_max_sec",  "{}s"),
    ]

    for label, key, fmt in rows:
        print(f"  {label:<28} ", end="")
        for r in results:
            val = r.get(key)
            cell = fmt.format(val) if val is not None else "  —"
            print(f"  {cell:>12}", end="")
        print()

    # Speedup vs first scenario
    if len(results) > 1:
        base_wall = results[0].get("wall_time_sec", 1)
        print(f"\n  {'Speedup vs Scenario A':<28} ", end="")
        for r in results:
            speedup = round(base_wall / r["wall_time_sec"], 2) if r.get("wall_time_sec") else "—"
            print(f"  {str(speedup)+'x':>12}", end="")
        print()

    print(f"\n  Notes:")
    print(f"  - Wall time = time from first upload to last job completion")
    print(f"  - OCR time  = time inside Celery worker (per job)")
    print(f"  - Queue wait = time job sat in Redis before Celery picked it up")
    print(f"  - Throughput scales with concurrency up to model API rate limit")
    print(f"\n  Celery commands used for each scenario:")
    for r in results:
        n = r["n_jobs"]
        c = min(n, 8)   # thread pool safe limit on Windows
        print(f"    Scenario {r['scenario']} ({n} jobs): "
              f"celery -A celery_app worker --pool=threads --concurrency={c} --loglevel=info -Q ocr_queue")
    print(f"\n  For Katonic (gevent pool, Linux):")
    print(f"    celery -A celery_app worker --pool=gevent --concurrency=24 --loglevel=info -Q ocr_queue")
    print(sep)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="CustomOCR Concurrency Comparison Load Test"
    )
    parser.add_argument("--file",      required=True, help="Path to test PDF")
    parser.add_argument("--token",     required=True, help="AUTH_TOKEN from .env")
    parser.add_argument("--url",       default="http://localhost:8050")
    parser.add_argument("--scenarios", default="A,B,C",
                        help="Scenarios to run: A=serial(2), B=medium(5), C=high(10)")
    parser.add_argument("--custom",    type=int, default=None,
                        help="Run a single custom job count instead of A/B/C")
    parser.add_argument("--poll",      type=int, default=3)
    args = parser.parse_args()

    if not Path(args.file).exists():
        print(f"ERROR: File not found: {args.file}")
        exit(1)

    global BASE_URL
    BASE_URL = args.url

    # Scenario definitions: name -> job count
    scenario_map = {"A": 2, "B": 5, "C": 10}

    if args.custom:
        scenarios_to_run = [("CUSTOM", args.custom)]
    else:
        selected = [s.strip().upper() for s in args.scenarios.split(",")]
        scenarios_to_run = [(s, scenario_map[s]) for s in selected if s in scenario_map]

    all_results = []

    for scenario_name, n_jobs in scenarios_to_run:
        # Advise the correct Celery command for this scenario
        c = min(n_jobs, 8)
        print(f"\n{'!'*62}")
        print(f"  BEFORE RUNNING SCENARIO {scenario_name} ({n_jobs} jobs):")
        print(f"  1. Stop your Celery worker (Ctrl+C in its terminal)")
        print(f"  2. Restart it with concurrency={c}:")
        print(f"")
        print(f"     celery -A celery_app worker --pool=threads "
              f"--concurrency={c} --loglevel=info -Q ocr_queue")
        print(f"")
        print(f"  3. Wait for 'celery@DESKTOP ready.' in the Celery terminal")
        print(f"  4. Press Enter here to start the scenario")
        print(f"{'!'*62}")
        input(f"  >> Press Enter when Celery worker is ready for Scenario {scenario_name}...")

        metrics = run_scenario(
            name=scenario_name,
            n_jobs=n_jobs,
            file_path=args.file,
            token=args.token,
            poll_sec=args.poll,
        )
        all_results.append(metrics)

        if len(scenarios_to_run) > 1 and scenario_name != scenarios_to_run[-1][0]:
            print(f"\n  Waiting 15s before next scenario (let queue drain)...")
            time.sleep(15)

    if all_results:
        print_comparison(all_results)


if __name__ == "__main__":
    main()