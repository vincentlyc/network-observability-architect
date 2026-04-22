from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from statistics import mean

from .models import TelemetryRecord


def _top(counter: Counter, n: int = 10) -> list[dict[str, float | str]]:
    return [{"name": name, "total_bytes": total} for name, total in counter.most_common(n)]


def _weekday(dt: date) -> str:
    return dt.strftime("%A")


def compute_observability_metrics(records: list[TelemetryRecord]) -> dict:
    total_traffic = sum(r.total_bytes for r in records)

    by_job = Counter()
    by_task = Counter()
    by_cluster = Counter()
    by_env = Counter()
    by_domain = Counter()
    by_day = Counter()
    by_weekday = Counter()

    driver_bytes = 0
    executor_bytes = 0

    run_totals: defaultdict[str, int] = defaultdict(int)

    for record in records:
        by_job[record.job_name] += record.total_bytes
        by_task[record.task_name] += record.total_bytes
        by_cluster[record.cluster_id] += record.total_bytes
        by_env[record.environment] += record.total_bytes
        by_domain[record.business_domain] += record.total_bytes
        day_str = record.execution_ts.date().isoformat()
        by_day[day_str] += record.total_bytes
        by_weekday[_weekday(record.execution_ts.date())] += record.total_bytes

        run_totals[record.run_id] += record.total_bytes

        if record.is_driver:
            driver_bytes += record.total_bytes
        else:
            executor_bytes += record.total_bytes

    daily_values = list(by_day.values())
    daily_avg = mean(daily_values) if daily_values else 0
    burst_days = [
        {"day": day, "total_bytes": total}
        for day, total in by_day.items()
        if daily_avg > 0 and total >= (1.5 * daily_avg)
    ]

    sorted_jobs = by_job.most_common()
    top5_share = (sum(v for _, v in sorted_jobs[:5]) / total_traffic) if total_traffic else 0.0

    return {
        "summary": {
            "record_count": len(records),
            "total_bytes": total_traffic,
            "total_gb": round(total_traffic / (1024**3), 4),
            "avg_run_bytes": int(mean(run_totals.values())) if run_totals else 0,
        },
        "top_jobs": _top(by_job),
        "top_tasks": _top(by_task),
        "top_clusters": _top(by_cluster),
        "traffic_by_environment": dict(by_env),
        "traffic_by_business_domain": dict(by_domain),
        "daily_trend": dict(by_day),
        "weekday_trend": dict(by_weekday),
        "driver_executor_split": {
            "driver_bytes": driver_bytes,
            "executor_bytes": executor_bytes,
            "driver_pct": round((driver_bytes / total_traffic) * 100, 2) if total_traffic else 0,
            "executor_pct": round((executor_bytes / total_traffic) * 100, 2) if total_traffic else 0,
        },
        "concentration": {
            "top_5_job_share": round(top5_share, 4),
            "is_highly_concentrated": top5_share >= 0.75,
        },
        "burst_days": burst_days,
    }
