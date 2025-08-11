from __future__ import annotations
from typing import Literal
from .stores import S3JobStore, LocalJobStore

def get_store(backend: Literal["s3", "local"] = "s3", **kwargs):
    if backend == "s3":
        return S3JobStore(**kwargs)
    if backend == "local":
        return LocalJobStore(**kwargs)
    raise ValueError(f"Unknown backend: {backend}")