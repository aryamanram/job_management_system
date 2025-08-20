from __future__ import annotations
import json
import random
import time
from pathlib import Path
from typing import Dict, Any

from .metadata import WorkerMetadata
from .stores import JobStore

WORKER_META_NAME = "worker-metadata.json"
RESULTS_NAME = "results.json"


def _worker_meta_key(job_id: str) -> str:
    return f"{job_id}/{WORKER_META_NAME}"


def _results_key(job_id: str) -> str:
    return f"{job_id}/{RESULTS_NAME}"


def _write_local(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_job_once(store: JobStore, work_root: Path, job_id: str, worker_id: str) -> None:
    """
    Mimic running a job:
      - sleep 10 seconds
      - 7/10 chance of 'successful', 3/10 'failure'
      - upload results.json
      - update worker-metadata.json
      - mirror both files in local work dir
    """
    job_dir = work_root / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    # --- Simulate execution ---
    time.sleep(10)
    succeeded = random.random() < 0.7

    if succeeded:
        status = "successful"
        results = {
            "summary": "Mock computation finished successfully.",
            "random_numbers": [random.randint(0, 9999) for _ in range(8)],
            "notes": "Replace with real model outputs later.",
        }
    else:
        status = "failure"
        results = {
            "error_code": "E_RUN_001",
            "message": "Mock failure: job could not be read / could not be run.",
            "hint": "This is simulated; replace with real error reporting later.",
        }

    # --- Upload results.json to store ---
    store.put_text(_results_key(job_id), json.dumps(results, indent=2))
    # Mirror locally
    _write_local(job_dir / RESULTS_NAME, results)

    # --- Update worker-metadata.json to final state ---
    # Keep original claimed_at/worker_id if the file exists; otherwise write minimal.
    try:
        existing = store.get_text(_worker_meta_key(job_id)) or ""
        md_obj = json.loads(existing) if existing else {}
        md_obj["status"] = status
        md_obj.setdefault("worker_id", worker_id)
        md_obj.setdefault("claimed_at", WorkerMetadata.in_progress(worker_id).claimed_at)
        md_json = json.dumps(md_obj, indent=2)
    except Exception:
        md_json = WorkerMetadata.in_progress(worker_id).to_json()  # fallback
        md_obj = json.loads(md_json)
        md_obj["status"] = status
        md_json = json.dumps(md_obj, indent=2)

    store.put_text(_worker_meta_key(job_id), md_json)
    (job_dir / WORKER_META_NAME).write_text(md_json, encoding="utf-8")
