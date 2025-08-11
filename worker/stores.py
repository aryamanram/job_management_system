from __future__ import annotations
import os
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

# --- Common interface --------------------------------------------------------

class JobStore(ABC):
    """Abstract access to a collection of job folders keyed by job_id/."""

    @abstractmethod
    def list_job_ids(self) -> Iterable[str]:
        """Yield top-level prefixes (job IDs)."""

    @abstractmethod
    def object_exists(self, key: str) -> bool:
        """Return True if the object exists at *key*."""

    @abstractmethod
    def get_text(self, key: str) -> Optional[str]:
        """Return object body as text, or None if missing."""

    @abstractmethod
    def put_text(self, key: str, text: str) -> None:
        """Write object body as text (overwrite if exists)."""

    @abstractmethod
    def download_prefix(self, prefix: str, dest_dir: Path) -> None:
        """Download all objects under *prefix*/ into *dest_dir*."""

# --- S3 implementation -------------------------------------------------------

@dataclass
class S3JobStore(JobStore):
    bucket: str
    endpoint_url: str | None = None
    region_name: str | None = None

    def __post_init__(self):
        import boto3
        from botocore.client import Config as _BotoConfig

        cfg = _BotoConfig(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 10, "mode": "standard"},
        )
        session = boto3.session.Session()
        self._s3_res = session.resource(
            "s3", endpoint_url=self.endpoint_url, region_name=self.region_name, config=cfg
        )
        self._s3_cli = session.client(
            "s3", endpoint_url=self.endpoint_url, region_name=self.region_name, config=cfg
        )
        self._bucket = self._s3_res.Bucket(self.bucket)

    def list_job_ids(self) -> Iterable[str]:
        # List top-level "directories" (CommonPrefixes) by using Delimiter="/"
        paginator = self._s3_cli.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                pref = cp.get("Prefix", "")
                if pref.endswith("/"):
                    yield pref[:-1]  # drop trailing slash

    def object_exists(self, key: str) -> bool:
        import botocore
        try:
            self._s3_cli.head_object(Bucket=self.bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as e:
            status = int(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            if status == 404:
                return False
            raise

    def get_text(self, key: str) -> Optional[str]:
        import botocore
        try:
            obj = self._s3_res.Object(self.bucket, key)
            body = obj.get()["Body"].read()
            return body.decode("utf-8")
        except botocore.exceptions.ClientError as e:
            status = int(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
            if status == 404:
                return None
            raise

    def put_text(self, key: str, text: str) -> None:
        obj = self._s3_res.Object(self.bucket, key)
        obj.put(Body=text.encode("utf-8"))

    def download_prefix(self, prefix: str, dest_dir: Path) -> None:
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        for obj in self._bucket.objects.filter(Prefix=f"{prefix}/"):
            rel = obj.key[len(prefix) + 1 :]  # strip "prefix/"
            if not rel:
                continue
            local_path = dest_dir / rel
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self._bucket.download_file(obj.key, str(local_path))

# --- Local implementation (handy for testing) --------------------------------

@dataclass
class LocalJobStore(JobStore):
    root_dir: str = "jobs"

    def __post_init__(self):
        self.root = Path(self.root_dir).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def list_job_ids(self) -> Iterable[str]:
        for child in sorted(self.root.iterdir()):
            if child.is_dir():
                yield child.name

    def object_exists(self, key: str) -> bool:
        return (self.root / key).exists()

    def get_text(self, key: str) -> Optional[str]:
        p = self.root / key
        if not p.exists():
            return None
        return p.read_text(encoding="utf-8")

    def put_text(self, key: str, text: str) -> None:
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")

    def download_prefix(self, prefix: str, dest_dir: Path) -> None:
        src = self.root / prefix
        if not src.exists():
            return
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        shutil.copytree(src, dest_dir)
