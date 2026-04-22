# Enterprise Network Cost Observability Platform

A production-oriented framework for turning raw workload telemetry into decision-ready network cost insights across Databricks, Spark, lakehouse, and multi-cloud analytics platforms.

## What this project provides

- A **standard schema** for workload/network telemetry.
- An **attribution model** that maps traffic to jobs, tasks, clusters, environments, and business domains.
- A **metrics engine** for trend analysis, concentration ratio, burst-day detection, and driver-vs-executor contribution.
- A **decision layer** that turns analytics into optimization recommendations.
- A simple **CLI workflow** to run end-to-end analysis from CSV input.

## Repository layout

```text
src/network_cost_observability/
  models.py          # telemetry schema and validation helpers
  ingestion.py       # ingestion + standardization pipeline
  analytics.py       # observability and concentration metrics
  decision.py        # recommendation engine
  pipeline.py        # orchestration helper
  cli.py             # command-line interface
examples/
  sample_telemetry.csv
```

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
network-cost-observe analyze \
  --input examples/sample_telemetry.csv \
  --output reports/summary.json
```

## Input schema

Expected columns (auto-normalized aliases are supported):

- `workspace_id`
- `cluster_id`
- `job_id`
- `job_name`
- `run_id`
- `task_run_id`
- `task_name`
- `is_driver`
- `bytes_sent`
- `bytes_received`
- `execution_ts`
- `workload_category`
- `environment`
- `business_domain`

## Output

The CLI writes a JSON artifact that includes:

- total traffic summary
- top jobs / tasks / clusters
- driver vs executor split
- daily and weekday trends
- concentration ratio (top-N share)
- burst day detection
- optimization recommendations

## Why this matters

This project creates a shared evidence model across platform engineering, FinOps, architecture, and leadership so teams can prioritize high-impact optimization work instead of anecdotal tuning.
