from __future__ import annotations

import json
from pathlib import Path

from .analytics import compute_observability_metrics
from .decision import generate_recommendations
from .ingestion import mask_sensitive_names, read_telemetry_csv


def run_observability_pipeline(input_csv: str, output_json: str, mask_names: bool = False) -> dict:
    records = read_telemetry_csv(input_csv)
    if mask_names:
        records = mask_sensitive_names(records)

    metrics = compute_observability_metrics(records)
    recommendations = generate_recommendations(metrics)

    artifact = {
        "metrics": metrics,
        "recommendations": recommendations,
    }

    out = Path(output_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    return artifact
