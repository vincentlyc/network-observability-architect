from __future__ import annotations

import argparse

from .pipeline import run_observability_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Network Cost Observability CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    analyze = sub.add_parser("analyze", help="Analyze telemetry CSV and emit summary JSON")
    analyze.add_argument("--input", required=True, help="Path to telemetry CSV")
    analyze.add_argument("--output", required=True, help="Path to output JSON")
    analyze.add_argument("--mask-names", action="store_true", help="Mask sensitive job names")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "analyze":
        artifact = run_observability_pipeline(args.input, args.output, args.mask_names)
        summary = artifact["metrics"]["summary"]
        print(
            "Analysis complete:",
            f"records={summary['record_count']}",
            f"total_gb={summary['total_gb']}",
        )


if __name__ == "__main__":
    main()
