#!/usr/bin/env python
"""
CLI entry-point: bundle kernel+data, then hand off to the selected Writer.

Run `python -m job_submission.submit_job --help` for usage.
"""
from __future__ import annotations

import argparse
import shutil
import os
import tempfile
import uuid
import sys
from pathlib import Path
import re

def _load_dotenv(path: Path = Path(__file__).parent.parent / ".env") -> None:
    """
    Populate os.environ from a .env file containing lines like
        export VAR=value
    An existing real environment variable always wins.
    """
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

_load_dotenv()

from .writer import get_writer

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Submit a kernel+data job.")
    p.add_argument("--kernel", required=True, help="Path to kernel file or folder.")
    p.add_argument("--data", required=True, help="Path to data file or folder.")
    p.add_argument(
    "--backend",
    choices=("local", "s3"),
    default=os.getenv("JOB_BACKEND", "s3"),
    help="Destination storage backend (default: s3).",
)

    # Local backend
    p.add_argument("--root-dir", default="jobs", help="Local root directory.")

    # S3 backend 
    p.add_argument("--bucket", help="RunPod S3 bucket name.")
    p.add_argument("--endpoint-url", help="Custom endpoint URL.")
    p.add_argument("--region", help="AWS region (RunPod uses us-* style).")

    return p

def _prepare_job_dir(kernel: Path, data: Path) -> Path:
    """Copy inputs into an isolated temp dir so the writer sees one root."""
    tmp_dir = Path(tempfile.mkdtemp(prefix="job_"))

    # ---- kernel -----------------------------------------------------------
    if kernel.is_dir():
        shutil.copytree(kernel, tmp_dir / "kernel")
    else:
        shutil.copy2(kernel, tmp_dir / f"kernel{kernel.suffix}")   # keep .txt …

    # ---- data -------------------------------------------------------------
    if data.is_dir():
        shutil.copytree(data, tmp_dir / "data")
    else:
        shutil.copy2(data, tmp_dir / f"data{data.suffix}")         # … or .json

    return tmp_dir

def main() -> None:
    args = _build_parser().parse_args()

    if args.backend == "s3" and not (args.bucket or os.getenv("RUNPOD_S3_BUCKET")):
        sys.exit("--bucket is required when backend is 's3'. "
                 "Provide --bucket or set RUNPOD_S3_BUCKET in .env")

    # Bundle the job into a temp folder:
    job_dir = _prepare_job_dir(Path(args.kernel), Path(args.data))

    if args.backend == "s3":
        writer = get_writer(
            "s3",
            bucket=args.bucket or os.getenv("RUNPOD_S3_BUCKET"),
            endpoint_url=args.endpoint_url or os.getenv("S3_ENDPOINT_URL"),
            region_name=args.region or os.getenv("AWS_REGION", "us-east-1"),
        )
    else:
        writer = get_writer("local", root_dir=args.root_dir)

    location = writer.write(job_dir)
    print(f"Job uploaded to:  {location}")


if __name__ == "__main__":
    main()