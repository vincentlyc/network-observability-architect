from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TelemetryRecord:
    workspace_id: str
    cluster_id: str
    job_id: str
    job_name: str
    run_id: str
    task_run_id: str
    task_name: str
    is_driver: bool
    bytes_sent: int
    bytes_received: int
    execution_ts: datetime
    workload_category: str
    environment: str
    business_domain: str

    @property
    def total_bytes(self) -> int:
        return self.bytes_sent + self.bytes_received
