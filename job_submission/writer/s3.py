from __future__ import annotations
import uuid
from pathlib import Path
import boto3
from botocore.client import Config as _BotoConfig
from boto3.s3.transfer import TransferConfig
from .base import Writer

class S3Writer(Writer):
    def __init__(
        self,
        bucket: str | None,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        multipart_threshold_mb: int = 128,
    ):
        if not bucket:
            raise ValueError("Bucket name missing (use --bucket or env var).")

        cfg = _BotoConfig(
            s3={"addressing_style": "path"},        # ← changed
            retries={"max_attempts": 10, "mode": "standard"},
        )

        session = boto3.session.Session()
        self._s3 = session.resource(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region_name,
            config=cfg,
        )
        self._bucket = self._s3.Bucket(bucket)
        self._xfer_cfg = TransferConfig(
            multipart_threshold=multipart_threshold_mb * 1024 * 1024
        )

    def write(self, local_path: str | Path, key_prefix: str | None = None) -> str:
        base = Path(local_path).resolve()
        job_id = key_prefix or uuid.uuid4().hex

        for fp in base.rglob("*"):
            if fp.is_file():
                key = f"{job_id}/{fp.relative_to(base).as_posix()}"
                self._bucket.upload_file(str(fp), key, Config=self._xfer_cfg)

        return f"s3://{self._bucket.name}/{job_id}"