"""transform.py — Clean raw CSVs and engineer features, writing Parquet files.

Reads from data/raw/, applies type coercions and derived columns,
and writes data/processed/ as Parquet files.
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    """Parse dates, derive length_of_stay and age_group, set types.

    Args:
        df: Raw patients DataFrame.

    Returns:
        Transformed patients DataFrame.
    """
    df = df.copy()

    df["arrival_date"] = pd.to_datetime(df["arrival_date"])
    df["departure_date"] = pd.to_datetime(df["departure_date"])

    df["length_of_stay"] = (df["departure_date"] - df["arrival_date"]).dt.days

    df["age_group"] = pd.cut(
        df["age"],
        bins=[-1, 17, 39, 64, 200],
        labels=["0-17", "18-39", "40-64", "65+"],
    ).astype(str)

    df["service"] = df["service"].astype("category")
    df["satisfaction"] = df["satisfaction"].astype(int)

    logger.info(
        "patients transformed — shape=%s, los range=[%d, %d], age_groups=%s",
        df.shape,
        df["length_of_stay"].min(),
        df["length_of_stay"].max(),
        df["age_group"].unique().tolist(),
    )
    return df


def transform_services_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Derive occupancy_rate, refusal_rate, demand_gap, is_event, set types.

    Args:
        df: Raw services_weekly DataFrame.

    Returns:
        Transformed services_weekly DataFrame.
    """
    df = df.copy()

    df["occupancy_rate"] = (
        df["patients_admitted"] / df["available_beds"] * 100
    ).round(2)

    df["refusal_rate"] = (
        df["patients_refused"] / df["patients_request"] * 100
    ).replace([np.inf, -np.inf], np.nan).fillna(0).round(2)

    df["demand_gap"] = df["patients_request"] - df["available_beds"]

    df["is_event"] = df["event"] != "none"

    df["service"] = df["service"].astype("category")
    df["event"] = df["event"].astype("category")

    logger.info(
        "services_weekly transformed — shape=%s, occupancy_rate range=[%.2f, %.2f]",
        df.shape,
        df["occupancy_rate"].min(),
        df["occupancy_rate"].max(),
    )
    return df


def transform_staff_schedule(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Set types, derive weekly summary.

    Args:
        df: Raw staff_schedule DataFrame.

    Returns:
        Tuple of (transformed staff_schedule, staff_weekly_summary) DataFrames.
    """
    df = df.copy()

    df["role"] = df["role"].astype("category")
    df["service"] = df["service"].astype("category")
    df["present"] = df["present"].astype(bool)

    staff_weekly_summary = (
        df.groupby(["week", "service"], observed=True)["present"]
        .sum()
        .reset_index()
        .rename(columns={"present": "staff_present_count"})
    )
    staff_weekly_summary["staff_present_count"] = staff_weekly_summary[
        "staff_present_count"
    ].astype(int)

    logger.info(
        "staff_schedule transformed — shape=%s, attendance_rate=%.1f%%",
        df.shape,
        df["present"].mean() * 100,
    )
    return df, staff_weekly_summary


def transform_staff(df: pd.DataFrame) -> pd.DataFrame:
    """Set category types.

    Args:
        df: Raw staff DataFrame.

    Returns:
        Transformed staff DataFrame.
    """
    df = df.copy()
    df["role"] = df["role"].astype("category")
    df["service"] = df["service"].astype("category")
    logger.info("staff transformed — shape=%s", df.shape)
    return df


def validate(
    patients: pd.DataFrame,
    services: pd.DataFrame,
) -> None:
    """Run all required validation assertions.

    Args:
        patients: Transformed patients DataFrame.
        services: Transformed services_weekly DataFrame.

    Raises:
        AssertionError: If any validation fails.
    """
    assert "length_of_stay" in patients.columns, "length_of_stay missing from patients"
    assert patients["length_of_stay"].between(1, 14).all(), (
        f"length_of_stay out of range: min={patients['length_of_stay'].min()}, "
        f"max={patients['length_of_stay'].max()}"
    )
    assert "age_group" in patients.columns, "age_group missing from patients"
    assert patients.isnull().sum().sum() == 0, (
        f"patients has {patients.isnull().sum().sum()} nulls"
    )

    assert "occupancy_rate" in services.columns, "occupancy_rate missing from services_weekly"
    assert "refusal_rate" in services.columns, "refusal_rate missing from services_weekly"
    assert "demand_gap" in services.columns, "demand_gap missing from services_weekly"
    assert services.isnull().sum().sum() == 0, (
        f"services_weekly has {services.isnull().sum().sum()} nulls"
    )

    logger.info("All transform validation assertions passed.")


def main() -> None:
    """Read raw CSVs, transform, validate, and write Parquet files."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    try:
        patients_raw = pd.read_csv(RAW_DIR / "patients.csv")
        services_raw = pd.read_csv(RAW_DIR / "services_weekly.csv")
        schedule_raw = pd.read_csv(RAW_DIR / "staff_schedule.csv")
        staff_raw = pd.read_csv(RAW_DIR / "staff.csv")
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Raw CSV not found — run extract.py first. Detail: {exc}"
        ) from exc

    patients = transform_patients(patients_raw)
    services = transform_services_weekly(services_raw)
    schedule, weekly_summary = transform_staff_schedule(schedule_raw)
    staff = transform_staff(staff_raw)

    validate(patients, services)

    # Write Parquet — use string dtype for categoricals to avoid schema issues downstream
    patients.to_parquet(PROCESSED_DIR / "patients.parquet", index=False)
    services.to_parquet(PROCESSED_DIR / "services_weekly.parquet", index=False)
    schedule.to_parquet(PROCESSED_DIR / "staff_schedule.parquet", index=False)
    weekly_summary.to_parquet(PROCESSED_DIR / "staff_weekly_summary.parquet", index=False)
    staff.to_parquet(PROCESSED_DIR / "staff.parquet", index=False)

    logger.info("All Parquet files written to data/processed/.")


if __name__ == "__main__":
    main()
