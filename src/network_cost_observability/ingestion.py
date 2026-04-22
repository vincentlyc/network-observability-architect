from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Iterable

from .models import TelemetryRecord

COLUMN_ALIASES = {
    "job_run_id": "run_id",
    "task_key": "task_name",
    "driver_flag": "is_driver",
    "date": "execution_ts",
}

REQUIRED_COLUMNS = {
    "workspace_id",
    "cluster_id",
    "job_id",
    "job_name",
    "run_id",
    "task_run_id",
    "task_name",
    "is_driver",
    "bytes_sent",
    "bytes_received",
    "execution_ts",
    "workload_category",
    "environment",
    "business_domain",
}


def _normalize_keys(row: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in row.items():
        mapped = COLUMN_ALIASES.get(key.strip(), key.strip())
        normalized[mapped] = value.strip() if isinstance(value, str) else value
    return normalized


def _as_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "y"}


def _parse_ts(value: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported timestamp format: {value}")


def read_telemetry_csv(path: str | Path) -> list[TelemetryRecord]:
    rows: list[TelemetryRecord] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV contains no header row")

        normalized_headers = {_normalize_keys({name: ""}).popitem()[0] for name in reader.fieldnames}
        missing = REQUIRED_COLUMNS - normalized_headers
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        for raw_row in reader:
            row = _normalize_keys(raw_row)
            rows.append(
                TelemetryRecord(
                    workspace_id=row["workspace_id"],
                    cluster_id=row["cluster_id"],
                    job_id=row["job_id"],
                    job_name=row["job_name"],
                    run_id=row["run_id"],
                    task_run_id=row["task_run_id"],
                    task_name=row["task_name"],
                    is_driver=_as_bool(row["is_driver"]),
                    bytes_sent=int(float(row["bytes_sent"])),
                    bytes_received=int(float(row["bytes_received"])),
                    execution_ts=_parse_ts(row["execution_ts"]),
                    workload_category=row["workload_category"],
                    environment=row["environment"],
                    business_domain=row["business_domain"],
                )
            )
    return rows


def mask_sensitive_names(records: Iterable[TelemetryRecord]) -> list[TelemetryRecord]:
    """Optional masking policy for production exports."""
    masked: list[TelemetryRecord] = []
    for r in records:
        masked_name = r.job_name if not r.job_name.startswith("secret_") else "masked_job"
        masked.append(
            TelemetryRecord(
                workspace_id=r.workspace_id,
                cluster_id=r.cluster_id,
                job_id=r.job_id,
                job_name=masked_name,
                run_id=r.run_id,
                task_run_id=r.task_run_id,
                task_name=r.task_name,
                is_driver=r.is_driver,
                bytes_sent=r.bytes_sent,
                bytes_received=r.bytes_received,
                execution_ts=r.execution_ts,
                workload_category=r.workload_category,
                environment=r.environment,
                business_domain=r.business_domain,
            )
        )
    return masked
