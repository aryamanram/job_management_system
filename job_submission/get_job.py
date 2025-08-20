from __future__ import annotations
from pathlib import Path

import argparse
import os
import re
import sys
import shutil

try:
    import boto3
    from botocore.client import Config as _BotoConfig
except Exception:
    boto3 = None
    _BotoConfig = None


def _load_dotenv(path: Path = Path(__file__).parent.parent / ".env") -> None:
    """Populate os.environ from a .env file (export VAR=... lines supported)."""
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

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Fetch a job folder by UUID from S3 (or local) into ./pulled_jobs/<UUID>."
    )
    p.add_argument("--uuid", required=True, help="Job UUID (the prefix/folder name in the bucket).")
    p.add_argument(
        "--backend",
        choices=("s3", "local"),
        default=os.getenv("JOB_BACKEND", "s3"),
        help="Source backend (default: s3).",
    )

    # Local backend
    p.add_argument("--root-dir", default="jobs", help="Local jobs root (for --backend local).")

    # S3 backend
    p.add_argument("--bucket", help="S3 bucket (default: RUNPOD_S3_BUCKET from .env).")
    p.add_argument("--endpoint-url", help="S3 endpoint (default: S3_ENDPOINT_URL from .env).")
    p.add_argument("--region", help="S3 region (default: AWS_REGION from .env).")

    # Destination
    p.add_argument("--outdir", default="pulled_jobs", help="Destination parent dir (default: pulled_jobs).")

    return p

def _download_s3_prefix(
    bucket: str,
    endpoint_url: str | None,
    region_name: str | None,
    job_uuid: str,
    dest: Path,
) -> None:
    if not boto3 or not _BotoConfig:
        sys.exit("boto3 is required for S3 downloads but is not available.")

    cfg = _BotoConfig(
        s3={"addressing_style": "path"},
        retries={"max_attempts": 10, "mode": "standard"},
    )

    session = boto3.session.Session()
    s3 = session.resource("s3", endpoint_url=endpoint_url, region_name=region_name, config=cfg)
    bkt = s3.Bucket(bucket)

    prefix = f"{job_uuid}/"
    found_any = False

    for obj in bkt.objects.filter(Prefix=prefix):
        # obj.key may equal "uuid/" (the folder marker) — skip those
        if obj.key.endswith("/") or obj.key == prefix:
            found_any = True
            continue

        found_any = True
        rel = obj.key[len(prefix) :]  # path inside the job folder
        dest_path = dest / rel
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        bkt.download_file(obj.key, str(dest_path))

    if not found_any:
        raise SystemExit(
            f"No objects found under prefix '{prefix}' in bucket '{bucket}'. "
            "Double-check the UUID and bucket."
        )

def _copy_local_tree(root_dir: Path, job_uuid: str, dest: Path) -> None:
    src = root_dir / job_uuid
    if not src.exists():
        raise SystemExit(f"Local job directory not found: {src}")
    # Python 3.8+: dirs_exist_ok to merge/overwrite
    shutil.copytree(src, dest, dirs_exist_ok=True)

def main() -> None:
    args = _build_parser().parse_args()

    job_uuid = args.uuid
    out_parent = Path(args.outdir).resolve()
    dest = out_parent / job_uuid
    dest.mkdir(parents=True, exist_ok=True)

    if args.backend == "local":
        _copy_local_tree(Path(args.root_dir), job_uuid, dest)
        print(f"Pulled job to: {dest}")
        return

    # S3 path
    bucket = args.bucket or os.getenv("RUNPOD_S3_BUCKET")
    if not bucket:
        sys.exit("--bucket is required for S3 (or set RUNPOD_S3_BUCKET in .env).")

    endpoint = args.endpoint_url or os.getenv("S3_ENDPOINT_URL")
    region = args.region or os.getenv("AWS_REGION") or "us-east-1"

    _download_s3_prefix(bucket, endpoint, region, job_uuid, dest)
    print(f"Pulled job to: {dest}")

if __name__ == "__main__":
    main()
