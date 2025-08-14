from __future__ import annotations
import argparse
import getpass
import os
import re
import socket
import sys
import time
from pathlib import Path

from . import get_store
from .worker import claim_and_pull_one

def _load_dotenv(path: Path = Path(__file__).parent.parent / ".env") -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        raw = re.sub(r"^export\s+", "", raw, flags=re.I)
        if "=" in raw:
            k, v = raw.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Worker: poll S3 (or local) for jobs, claim, and pull.")
    p.add_argument("--backend", choices=("s3","local"), default=os.getenv("JOB_BACKEND","s3"))

    # S3 settings
    p.add_argument("--bucket", help="Bucket name (RUNPOD_S3_BUCKET if omitted).")
    p.add_argument("--endpoint-url", help="Custom S3 endpoint (S3_ENDPOINT_URL if omitted).")
    p.add_argument("--region", help="AWS/RunPod region (AWS_REGION if omitted).")

    # Local testing backend
    p.add_argument("--root-dir", default="jobs", help="Local jobs root (for --backend local).")

    # Runtime
    p.add_argument("--workdir", default="work", help="Local directory to place pulled jobs.")
    p.add_argument("--once", action="store_true", help="Claim/pull at most one job then exit.")
    p.add_argument("--interval", type=int, default=5, help="Poll interval (seconds) when looping.")
    p.add_argument("--worker-id", help="Custom worker ID (defaults to host:user).")
    return p

def main() -> None:
    _load_dotenv()
    args = _build_parser().parse_args()

    worker_id = args.worker_id or f"{socket.gethostname()}:{getpass.getuser()}"

    if args.backend == "s3":
        bucket = args.bucket or os.getenv("RUNPOD_S3_BUCKET")
        if not bucket:
            sys.exit("--bucket is required for S3 worker (or set RUNPOD_S3_BUCKET).")
        store = get_store(
            "s3",
            bucket=bucket,
            endpoint_url=args.endpoint_url or os.getenv("S3_ENDPOINT_URL"),
            region_name=args.region or os.getenv("AWS_REGION") or "us-east-1",
        )
    else:
        store = get_store("local", root_dir=args.root_dir)

    work_root = Path(args.workdir).resolve()

    def loop_once() -> bool:
        job_id = claim_and_pull_one(store, work_root, worker_id)
        if job_id:
            print(f"[worker] claimed and pulled job: {job_id} -> {work_root / job_id}")
            return True
        print("[worker] no claimable jobs found")
        return False

    if args.once:
        loop_once()
        return

    while True:
        loop_once()
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
