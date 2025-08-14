from __future__ import annotations
from pathlib import Path
from typing import Optional

from .metadata import WorkerMetadata, parse_worker_metadata, write_local_worker_metadata
from .stores import JobStore

WORKER_META_NAME = "worker-metadata.json"

def _worker_meta_key(job_id: str) -> str:
    return f"{job_id}/{WORKER_META_NAME}"

def _is_claimable(store: JobStore, job_id: str) -> bool:
    """True if no worker-metadata.json exists, or if it exists but is unusable."""
    key = _worker_meta_key(job_id)
    if not store.object_exists(key):
        return True
    text = store.get_text(key)
    md = parse_worker_metadata(text or "")
    if md and md.status in ("in-progress", "failed", "completed"):
        return False
    return False

def claim_and_pull_one(store: JobStore, work_root: Path, worker_id: str) -> Optional[str]:
    """
    Find first claimable job, mark it in S3/local with worker-metadata ("in-progress"),
    then download the full job into work_root/<job_id>/.
    Returns job_id or None if nothing to claim.
    """
    work_root.mkdir(parents=True, exist_ok=True)

    for job_id in store.list_job_ids():
        if not _is_claimable(store, job_id):
            continue

        # Claim by writing worker-metadata.json at the job root
        md = WorkerMetadata.in_progress(worker_id)
        store.put_text(_worker_meta_key(job_id), md.to_json())

        # Pull the whole job folder locally
        dest = work_root / job_id
        store.download_prefix(job_id, dest)

        # Ensure local copy also has the worker-metadata.json
        write_local_worker_metadata(dest / WORKER_META_NAME, md)

        return job_id

    return None
