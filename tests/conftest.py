"""Shared pytest fixtures and command-line options."""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--engine",
        action="store",
        default="dummy",
        help="Engine name to use for contract tests (default: dummy).",
    )


@pytest.fixture
def engine_name(request: pytest.FixtureRequest) -> str:
    return request.config.getoption("--engine")
