"""extract.py — Download the hospital-beds-management dataset from Kaggle.

Downloads jaderz/hospital-beds-management via kagglehub, copies the four
CSV files to data/raw/, and validates shapes and null counts.
"""

import logging
import shutil
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Bootstrap: add the scripts directory to sys.path so auth is importable
# whether the script is run from the project root or the scripts/ folder.
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent
sys.path.insert(0, str(SCRIPTS_DIR))

import auth  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

RAW_DIR = PROJECT_ROOT / "data" / "raw"

# Expected CSV filenames inside the Kaggle dataset directory
EXPECTED_FILES = [
    "patients.csv",
    "services_weekly.csv",
    "staff_schedule.csv",
    "staff.csv",
]


def download_dataset() -> Path:
    """Download the Kaggle dataset and return the local directory path.

    Returns:
        Path to the directory containing the downloaded CSV files.

    Raises:
        RuntimeError: If kagglehub fails to download the dataset.
    """
    import kagglehub  # imported here so auth env-vars are set first

    logger.info("Downloading dataset jaderz/hospital-beds-management via kagglehub …")
    try:
        dataset_dir = kagglehub.dataset_download("jaderz/hospital-beds-management")
    except Exception as exc:
        raise RuntimeError(f"kagglehub download failed: {exc}") from exc

    dataset_path = Path(dataset_dir)
    logger.info("Dataset downloaded to: %s", dataset_path)
    return dataset_path


def find_csv_files(dataset_path: Path) -> dict[str, Path]:
    """Locate each expected CSV inside the downloaded dataset directory tree.

    Args:
        dataset_path: Root directory returned by kagglehub.

    Returns:
        Mapping of filename → full path.

    Raises:
        FileNotFoundError: If any expected CSV is missing.
    """
    found: dict[str, Path] = {}
    all_csvs = list(dataset_path.rglob("*.csv"))
    logger.info("All CSVs found in download: %s", [p.name for p in all_csvs])

    for name in EXPECTED_FILES:
        matches = [p for p in all_csvs if p.name == name]
        if not matches:
            raise FileNotFoundError(
                f"Expected file '{name}' not found in dataset directory '{dataset_path}'. "
                f"Available files: {[p.name for p in all_csvs]}"
            )
        found[name] = matches[0]

    return found


def copy_to_raw(csv_paths: dict[str, Path]) -> None:
    """Copy the located CSVs into data/raw/.

    Args:
        csv_paths: Mapping of filename → source path.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for name, src in csv_paths.items():
        dest = RAW_DIR / name
        shutil.copy2(src, dest)
        logger.info("Copied %s → %s", src, dest)


def load_and_validate() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read the four raw CSVs and run shape + null-count assertions.

    Returns:
        Tuple of (patients, services_weekly, staff_schedule, staff) DataFrames.

    Raises:
        AssertionError: If any shape or null-count check fails.
    """
    patients = pd.read_csv(RAW_DIR / "patients.csv")
    services_weekly = pd.read_csv(RAW_DIR / "services_weekly.csv")
    staff_schedule = pd.read_csv(RAW_DIR / "staff_schedule.csv")
    staff = pd.read_csv(RAW_DIR / "staff.csv")

    for name, df in [
        ("patients", patients),
        ("services_weekly", services_weekly),
        ("staff_schedule", staff_schedule),
        ("staff", staff),
    ]:
        logger.info("%s — shape=%s, columns=%s", name, df.shape, list(df.columns))

    # --- shape assertions ---
    assert patients.shape == (1000, 7), (
        f"patients shape mismatch: expected (1000, 7), got {patients.shape}"
    )
    assert services_weekly.shape == (208, 10), (
        f"services_weekly shape mismatch: expected (208, 10), got {services_weekly.shape}"
    )
    assert staff_schedule.shape == (6552, 6), (
        f"staff_schedule shape mismatch: expected (6552, 6), got {staff_schedule.shape}"
    )
    assert staff.shape == (110, 4), (
        f"staff shape mismatch: expected (110, 4), got {staff.shape}"
    )

    # --- null assertions ---
    for name, df in [
        ("patients", patients),
        ("services_weekly", services_weekly),
        ("staff_schedule", staff_schedule),
        ("staff", staff),
    ]:
        nulls = df.isnull().sum().sum()
        assert nulls == 0, f"{name} has {nulls} null values — expected 0"
        logger.info("%s — null check passed (0 nulls)", name)

    logger.info("All validation assertions passed.")
    return patients, services_weekly, staff_schedule, staff


def main() -> None:
    """Orchestrate the extraction pipeline."""
    auth.load_kaggle_credentials()
    auth.validate_credentials()

    dataset_path = download_dataset()
    csv_paths = find_csv_files(dataset_path)
    copy_to_raw(csv_paths)
    load_and_validate()

    logger.info("extract.py complete — all four CSVs in data/raw/ and validated.")


if __name__ == "__main__":
    main()
