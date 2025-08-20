from __future__ import annotations
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional

# was: Literal["in-progress", "failed", "completed"]
Status = Literal["in-progress", "successful", "failure"]

@dataclass
class WorkerMetadata:
    status: Status
    claimed_at: str
    worker_id: str

    @staticmethod
    def in_progress(worker_id: str) -> "WorkerMetadata":
        return WorkerMetadata(
            status="in-progress",
            claimed_at=datetime.now(timezone.utc).isoformat(),
            worker_id=worker_id,
        )

    def to_json(self) -> str:
        return json.dumps(
            {"status": self.status, "claimed_at": self.claimed_at, "worker_id": self.worker_id},
            indent=2,
        )

def parse_worker_metadata(text: str) -> Optional[WorkerMetadata]:
    try:
        obj = json.loads(text)
        st = obj.get("status")
        if st in ("in-progress", "successful", "failure"):
            return WorkerMetadata(
                status=st,
                claimed_at=obj.get("claimed_at", ""),
                worker_id=obj.get("worker_id", ""),
            )
    except Exception:
        pass
    return None

def write_local_worker_metadata(path: Path, md: WorkerMetadata) -> None:
    path.write_text(md.to_json(), encoding="utf-8")
