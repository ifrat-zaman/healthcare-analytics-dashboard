"""load.py — Load processed Parquet files into PostgreSQL.

Drops and recreates all four tables on every run (idempotent),
then bulk-loads from the processed Parquet files.
"""

import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# ---------------------------------------------------------------------------
# Connection defaults (Homebrew PostgreSQL — no password, port 5433)
# ---------------------------------------------------------------------------
DB_NAME = os.getenv("DB_NAME", "hospital_analytics")
DB_USER = os.getenv("DB_USER", "ifratzaman")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")


def get_engine():
    """Build and return a SQLAlchemy engine.

    Returns:
        sqlalchemy.Engine connected to hospital_analytics.
    """
    if DB_PASSWORD:
        url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    else:
        url = f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    connect_args = {"sslmode": "require"} if DB_HOST not in ("localhost", "127.0.0.1") else {}
    engine = create_engine(url, connect_args=connect_args)
    logger.info("Connected to %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
    return engine


DDL = """
DROP TABLE IF EXISTS staff_schedule CASCADE;
DROP TABLE IF EXISTS staff CASCADE;
DROP TABLE IF EXISTS services_weekly CASCADE;
DROP TABLE IF EXISTS patients CASCADE;

CREATE TABLE patients (
    patient_id          TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    age                 INTEGER NOT NULL,
    age_group           TEXT NOT NULL,
    arrival_date        DATE NOT NULL,
    departure_date      DATE NOT NULL,
    length_of_stay      INTEGER NOT NULL,
    service             TEXT NOT NULL,
    satisfaction        INTEGER NOT NULL
);

CREATE TABLE services_weekly (
    id                  SERIAL PRIMARY KEY,
    week                INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    service             TEXT NOT NULL,
    available_beds      INTEGER NOT NULL,
    patients_request    INTEGER NOT NULL,
    patients_admitted   INTEGER NOT NULL,
    patients_refused    INTEGER NOT NULL,
    patient_satisfaction INTEGER NOT NULL,
    staff_morale        INTEGER NOT NULL,
    event               TEXT NOT NULL,
    occupancy_rate      NUMERIC(6,2) NOT NULL,
    refusal_rate        NUMERIC(6,2) NOT NULL,
    demand_gap          INTEGER NOT NULL,
    is_event            BOOLEAN NOT NULL
);

CREATE TABLE staff (
    staff_id    TEXT PRIMARY KEY,
    staff_name  TEXT NOT NULL,
    role        TEXT NOT NULL,
    service     TEXT NOT NULL
);

CREATE TABLE staff_schedule (
    id          SERIAL PRIMARY KEY,
    week        INTEGER NOT NULL,
    staff_id    TEXT NOT NULL,
    staff_name  TEXT NOT NULL,
    role        TEXT NOT NULL,
    service     TEXT NOT NULL,
    present     BOOLEAN NOT NULL
);
"""


def create_tables(engine) -> None:
    """Drop and recreate all four tables.

    Args:
        engine: SQLAlchemy engine.
    """
    with engine.begin() as conn:
        conn.execute(text(DDL))
    logger.info("Tables dropped and recreated.")


def load_parquet(engine) -> None:
    """Bulk-insert all four Parquet files into the database.

    Args:
        engine: SQLAlchemy engine.

    Raises:
        FileNotFoundError: If a required Parquet file is missing.
    """
    # patients
    patients = pd.read_parquet(PROCESSED_DIR / "patients.parquet")
    # Convert category columns to string for psycopg2
    for col in patients.select_dtypes(["category"]).columns:
        patients[col] = patients[col].astype(str)
    # Ensure date columns are proper Python dates
    patients["arrival_date"] = pd.to_datetime(patients["arrival_date"]).dt.date
    patients["departure_date"] = pd.to_datetime(patients["departure_date"]).dt.date
    patients.to_sql("patients", engine, if_exists="append", index=False, method="multi")
    logger.info("Loaded patients — %d rows", len(patients))

    # services_weekly (drop auto-id column if present)
    services = pd.read_parquet(PROCESSED_DIR / "services_weekly.parquet")
    for col in services.select_dtypes(["category"]).columns:
        services[col] = services[col].astype(str)
    services["is_event"] = services["is_event"].astype(bool)
    services.to_sql("services_weekly", engine, if_exists="append", index=False, method="multi")
    logger.info("Loaded services_weekly — %d rows", len(services))

    # staff (must be loaded before staff_schedule due to FK)
    staff = pd.read_parquet(PROCESSED_DIR / "staff.parquet")
    for col in staff.select_dtypes(["category"]).columns:
        staff[col] = staff[col].astype(str)
    staff.to_sql("staff", engine, if_exists="append", index=False, method="multi")
    logger.info("Loaded staff — %d rows", len(staff))

    # staff_schedule
    schedule = pd.read_parquet(PROCESSED_DIR / "staff_schedule.parquet")
    for col in schedule.select_dtypes(["category"]).columns:
        schedule[col] = schedule[col].astype(str)
    schedule["present"] = schedule["present"].astype(bool)
    schedule.to_sql("staff_schedule", engine, if_exists="append", index=False, method="multi")
    logger.info("Loaded staff_schedule — %d rows", len(schedule))


def count_rows(engine, table: str) -> int:
    """Return the row count of a table.

    Args:
        engine: SQLAlchemy engine.
        table: Table name.

    Returns:
        Integer row count.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        return result.scalar()


def validate(engine) -> None:
    """Assert row counts match source data.

    Args:
        engine: SQLAlchemy engine.

    Raises:
        AssertionError: If any count is wrong.
    """
    counts = {
        "patients": (1000, count_rows(engine, "patients")),
        "services_weekly": (208, count_rows(engine, "services_weekly")),
        "staff": (110, count_rows(engine, "staff")),
        "staff_schedule": (6552, count_rows(engine, "staff_schedule")),
    }
    for table, (expected, actual) in counts.items():
        assert actual == expected, (
            f"{table}: expected {expected} rows, got {actual}"
        )
        logger.info("%s — %d rows OK", table, actual)
    logger.info("All load validation assertions passed.")


def main() -> None:
    """Orchestrate the load pipeline."""
    engine = get_engine()
    create_tables(engine)
    load_parquet(engine)
    validate(engine)
    logger.info("load.py complete.")


if __name__ == "__main__":
    main()
