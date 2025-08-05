import shutil
import uuid
from pathlib import Path

from .base import Writer


class LocalWriter(Writer):
    """Fallback backend that copies into a local <root_dir>/<job-id>/."""

    def __init__(self, root_dir: str = "jobs"):
        self.root_dir = Path(root_dir).expanduser().resolve()
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def write(self, local_path: str | Path, key_prefix: str | None = None) -> str:
        local_path = Path(local_path).resolve()
        job_id = key_prefix or uuid.uuid4().hex
        dest = self.root_dir / job_id
        if dest.exists():
            raise FileExistsError(f"{dest} already exists; refusing to overwrite.")
        shutil.copytree(local_path, dest)
        return str(dest)
