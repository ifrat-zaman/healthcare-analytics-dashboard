"""pipeline.py — Orchestrate the full ETL pipeline: extract → transform → load → query.

Usage:
    python3 scripts/pipeline.py
    python3 scripts/pipeline.py --skip-extract
"""

import argparse
import logging
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

EXPECTED_RAW = [
    PROJECT_ROOT / "data" / "raw" / name
    for name in ["patients.csv", "services_weekly.csv", "staff_schedule.csv", "staff.csv"]
]
EXPECTED_PROCESSED = [
    PROJECT_ROOT / "data" / "processed" / name
    for name in [
        "patients.parquet",
        "services_weekly.parquet",
        "staff_schedule.parquet",
        "staff.parquet",
    ]
]
EXPECTED_QUERY_RESULTS = [
    PROJECT_ROOT / "reports" / "query_results" / name
    for name in [
        "avg_los_by_service_and_age_group.csv",
        "weekly_occupancy_by_service.csv",
        "refusal_analysis.csv",
        "event_impact_comparison.csv",
        "staff_attendance_by_role_and_service.csv",
        "staffing_vs_satisfaction.csv",
    ]
]


def run_step(label: str, module_path: str) -> None:
    """Import and call main() from the given script module.

    Args:
        label: Human-readable step name for logging.
        module_path: Dotted module path relative to scripts/ (e.g. 'extract').

    Raises:
        SystemExit: Re-raises if the step exits with a non-zero code.
        Exception: Re-raises any unhandled exception from the step.
    """
    logger.info("=" * 60)
    logger.info("STEP START: %s", label)
    t0 = time.monotonic()

    scripts_dir = PROJECT_ROOT / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))

    try:
        import importlib
        mod = importlib.import_module(module_path)
        mod.main()
    except SystemExit as exc:
        if exc.code != 0:
            logger.error("STEP FAILED: %s — SystemExit(%s)", label, exc.code)
            raise
    except Exception as exc:
        logger.error("STEP FAILED: %s — %s: %s", label, type(exc).__name__, exc)
        raise

    elapsed = time.monotonic() - t0
    logger.info("STEP DONE: %s (%.1fs)", label, elapsed)


def validate_files(paths: list[Path], category: str) -> None:
    """Assert that all expected output files exist.

    Args:
        paths: List of expected file paths.
        category: Label for the group (e.g. 'raw CSVs').

    Raises:
        AssertionError: If any file is missing.
    """
    missing = [p for p in paths if not p.exists()]
    assert not missing, (
        f"Missing {category} files: {[str(p) for p in missing]}"
    )
    logger.info("File check OK — %d %s files present.", len(paths), category)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed namespace with skip_extract flag.
    """
    parser = argparse.ArgumentParser(description="Hospital analytics ETL pipeline")
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip the extract step (use existing data/raw/ CSVs).",
    )
    return parser.parse_args()


def main() -> None:
    """Run the full pipeline in sequence."""
    args = parse_args()
    t_start = time.monotonic()
    logger.info("Pipeline starting. skip_extract=%s", args.skip_extract)

    if not args.skip_extract:
        run_step("Extract", "extract")
    else:
        logger.info("Skipping extract step — using existing raw CSVs.")

    validate_files(EXPECTED_RAW, "raw CSV")

    run_step("Transform", "transform")
    validate_files(EXPECTED_PROCESSED, "processed Parquet")

    run_step("Load", "load")

    run_step("Query", "query")
    validate_files(EXPECTED_QUERY_RESULTS, "query result CSV")

    elapsed = time.monotonic() - t_start
    logger.info("=" * 60)
    logger.info("Pipeline complete in %.1fs. Launch the dashboard with:", elapsed)
    logger.info("  streamlit run scripts/dashboard.py")


if __name__ == "__main__":
    main()
