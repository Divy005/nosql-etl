"""Configuration loader: reads .env and exposes typed settings."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root (two levels up from this file)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def get_pg_dsn() -> str:
    """Build a PostgreSQL connection string from environment variables."""
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "nasa_analytics")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def get_mongo_uri() -> str:
    """Return the MongoDB connection URI."""
    return os.getenv("MONGO_URI", "mongodb://localhost:27017/")


def get_mongo_db() -> str:
    """Return the MongoDB database name."""
    return os.getenv("MONGO_DB", "nasa_etl")


def get_data_dir() -> Path:
    """Return the directory containing the NASA log files."""
    return Path(os.getenv("DATA_DIR", str(_PROJECT_ROOT / "data")))


def default_input_files() -> list[Path]:
    """Return the default input file paths (Jul then Aug, in order)."""
    data_dir = get_data_dir()
    return [
        data_dir / "NASA_access_log_Jul95",
        data_dir / "NASA_access_log_Aug95",
    ]
