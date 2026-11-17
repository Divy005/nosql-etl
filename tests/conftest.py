"""Shared pytest fixtures and command-line options."""

import pytest

from pipelines import canonical_engine_name


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
