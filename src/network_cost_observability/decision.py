from __future__ import annotations


def generate_recommendations(metrics: dict) -> list[dict[str, str]]:
    recommendations: list[dict[str, str]] = []

    top_jobs = metrics.get("top_jobs", [])
    split = metrics.get("driver_executor_split", {})
    concentration = metrics.get("concentration", {})
    burst_days = metrics.get("burst_days", [])

    if concentration.get("is_highly_concentrated"):
        recommendations.append(
            {
                "priority": "high",
                "theme": "workload-concentration",
                "recommendation": "Top 5 jobs dominate traffic. Create optimization backlog focused on those pipelines first.",
            }
        )

    if split.get("driver_pct", 0) >= 35:
        recommendations.append(
            {
                "priority": "medium",
                "theme": "driver-pressure",
                "recommendation": "Driver traffic is elevated. Review collect/broadcast patterns and central coordinator IO paths.",
            }
        )

    if burst_days:
        recommendations.append(
            {
                "priority": "high",
                "theme": "burst-detection",
                "recommendation": "Burst traffic days detected. Align batch windows, autoscaling, and ingestion cadence to smooth spikes.",
            }
        )

    if top_jobs:
        hottest = top_jobs[0]
        recommendations.append(
            {
                "priority": "high",
                "theme": "priority-pipeline",
                "recommendation": f"Start with job '{hottest['name']}' as highest traffic driver and run deep shuffle/egress review.",
            }
        )

    if not recommendations:
        recommendations.append(
            {
                "priority": "low",
                "theme": "steady-state",
                "recommendation": "No major anti-pattern detected. Continue trend monitoring and monthly architecture review.",
            }
        )

    return recommendations
