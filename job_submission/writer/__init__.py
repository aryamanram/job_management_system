import os
from typing import Literal

from .local import LocalWriter

try:
    from .s3 import S3Writer  # noqa: F401  (may fail if boto3 is not installed)
except ModuleNotFoundError:
    S3Writer = None  # type: ignore


def get_writer(backend: Literal["local", "s3"] = "local", **kwargs):  
    """Factory that produces the right Writer subclass."""
    if backend == "local":
        return LocalWriter(**kwargs)
    if backend == "s3":
        if S3Writer is None:
            raise ImportError("boto3 is required for the S3 backend: pip install boto3")
        return S3Writer(**kwargs)
    raise ValueError(f"Unknown backend: {backend}")
