"""auth.py — Load Kaggle API credentials from Kaggle.txt and set environment variables.

Reads the project-root Kaggle.txt file, parses the username and API token,
and populates os.environ so that kagglehub can authenticate transparently.
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Resolve project root relative to this file (scripts/ → project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
KAGGLE_TXT = PROJECT_ROOT / "Kaggle.txt"


def load_kaggle_credentials(credentials_path: Path = KAGGLE_TXT) -> None:
    """Read Kaggle.txt and set KAGGLE_USERNAME and KAGGLE_KEY environment variables.

    Args:
        credentials_path: Path to the Kaggle credentials file. Defaults to
            Kaggle.txt in the project root.

    Raises:
        FileNotFoundError: If credentials_path does not exist.
        ValueError: If KAGGLE_USERNAME or KAGGLE_KEY cannot be parsed or are empty.
    """
    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Kaggle credentials file not found at '{credentials_path}'. "
            "Create Kaggle.txt in the project root with your username and API token."
        )

    username: str = ""
    api_key: str = ""

    try:
        with credentials_path.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                # Skip blank lines and comments
                if not line or line.startswith("#"):
                    continue

                lower = line.lower()

                if lower.startswith("kaggle username:"):
                    username = line.split(":", 1)[1].strip()
                elif lower.startswith("api token:"):
                    api_key = line.split(":", 1)[1].strip()
    except OSError as exc:
        raise OSError(f"Failed to read credentials file '{credentials_path}': {exc}") from exc

    if not username:
        raise ValueError(
            "KAGGLE_USERNAME is missing or empty in Kaggle.txt. "
            "Add a line: 'Kaggle username: <your_username>'"
        )
    if not api_key:
        raise ValueError(
            "KAGGLE_KEY is missing or empty in Kaggle.txt. "
            "Add a line: 'Api token: <your_api_token>'"
        )

    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"] = api_key

    logger.info("Kaggle credentials loaded — username='%s', key length=%d", username, len(api_key))


def validate_credentials() -> None:
    """Assert that KAGGLE_USERNAME and KAGGLE_KEY are set and non-empty.

    Raises:
        ValueError: If either environment variable is absent or empty.
    """
    missing = [
        var
        for var in ("KAGGLE_USERNAME", "KAGGLE_KEY")
        if not os.environ.get(var)
    ]
    if missing:
        raise ValueError(
            f"The following Kaggle environment variables are not set or empty: {missing}. "
            "Call load_kaggle_credentials() before running the pipeline."
        )
    logger.info("Kaggle credentials validated successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_kaggle_credentials()
    validate_credentials()
    logger.info("auth.py: credentials OK.")
