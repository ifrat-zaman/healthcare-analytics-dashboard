"""query.py — Execute analytical SQL queries and export results to CSV.

Reads sql/analysis_queries.sql, splits on -- [query_N: name] sentinels,
runs each query against PostgreSQL, and writes reports/query_results/<name>.csv.
"""

import logging
import os
import re
import sys
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
SQL_FILE = PROJECT_ROOT / "sql" / "analysis_queries.sql"
RESULTS_DIR = PROJECT_ROOT / "reports" / "query_results"

DB_NAME = os.getenv("DB_NAME", "hospital_analytics")
DB_USER = os.getenv("DB_USER", "ifratzaman")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")

SENTINEL = re.compile(r"--\s*\[query_\d+:\s*(\w+)\]")


def get_engine():
    """Build and return a SQLAlchemy engine.

    Returns:
        sqlalchemy.Engine connected to hospital_analytics.
    """
    if DB_PASSWORD:
        url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    else:
        url = f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def parse_queries(sql_text: str) -> list[tuple[str, str]]:
    """Split SQL file into (name, sql) pairs using sentinel comments.

    Args:
        sql_text: Full contents of the SQL file.

    Returns:
        List of (query_name, sql_string) tuples in file order.
    """
    parts: list[tuple[str, str]] = []
    current_name: str | None = None
    current_lines: list[str] = []

    for line in sql_text.splitlines():
        m = SENTINEL.search(line)
        if m:
            if current_name is not None:
                sql = "\n".join(current_lines).strip()
                if sql:
                    parts.append((current_name, sql))
            current_name = m.group(1)
            current_lines = []
        else:
            current_lines.append(line)

    if current_name is not None:
        sql = "\n".join(current_lines).strip()
        if sql:
            parts.append((current_name, sql))

    return parts


def run_queries(engine, queries: list[tuple[str, str]]) -> None:
    """Execute each query, validate row count > 0, and export to CSV.

    Args:
        engine: SQLAlchemy engine.
        queries: List of (name, sql) tuples.

    Raises:
        AssertionError: If any query returns 0 rows.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    for name, sql in queries:
        try:
            df = pd.read_sql(text(sql), engine)
        except Exception as exc:
            raise RuntimeError(f"Query '{name}' failed: {exc}") from exc

        assert len(df) > 0, f"Query '{name}' returned 0 rows — check the SQL or database."

        out_path = RESULTS_DIR / f"{name}.csv"
        df.to_csv(out_path, index=False)
        logger.info("Query '%s' — %d rows → %s", name, len(df), out_path)


def main() -> None:
    """Parse SQL file, run all queries, export CSVs."""
    try:
        sql_text = SQL_FILE.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"SQL file not found at '{SQL_FILE}'. Ensure sql/analysis_queries.sql exists."
        ) from exc

    queries = parse_queries(sql_text)
    logger.info("Parsed %d queries from %s", len(queries), SQL_FILE)

    engine = get_engine()
    run_queries(engine, queries)

    logger.info("query.py complete — %d CSV files written to reports/query_results/.", len(queries))


if __name__ == "__main__":
    main()
