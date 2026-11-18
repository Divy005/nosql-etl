"""Shared pytest fixtures and command-line options."""

import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from controller.config import get_pg_dsn
from pipelines import canonical_engine_name

SCHEMA_SQL = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--engine",
        action="store",
        default="dummy",
        help="Engine name to use for contract tests (default: dummy).",
    )


@pytest.fixture
def engine_name(request: pytest.FixtureRequest) -> str:
    # Accept aliases such as --engine mongo; tests always see the canonical name.
    return canonical_engine_name(request.config.getoption("--engine"))


@pytest.fixture
def pg_conn() -> Iterator[psycopg.Connection]:
    """A PostgreSQL connection whose search_path is a throw-away schema holding db/schema.sql.

    ``schema.sql`` starts with ``DROP TABLE IF EXISTS``; with only the temporary schema on the
    search_path those statements can never touch the ``public`` tables that hold real runs.
    The test is skipped when PostgreSQL is not reachable.
    """
    try:
        connection = psycopg.connect(get_pg_dsn(), autocommit=False, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL service unavailable")

    schema = f"etl_test_{uuid.uuid4().hex[:8]}"
    connection.execute(f'CREATE SCHEMA "{schema}"')
    connection.execute(f'SET search_path TO "{schema}"')
    connection.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    connection.commit()  # the SET must be committed or a later rollback would undo it
    try:
        yield connection
    finally:
        connection.rollback()
        connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        connection.commit()
        connection.close()
