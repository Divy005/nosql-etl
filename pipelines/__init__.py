"""Pipeline engine registry and factory."""

from pipelines.base import Engine
from pipelines.dummy.engine import DummyEngine

_REGISTRY: dict[str, type[Engine]] = {
    "dummy": DummyEngine,
}


def register_engine(name: str, engine_cls: type[Engine]) -> None:
    """Register an engine implementation class under a given name."""
    _REGISTRY[name.lower()] = engine_cls


def get_engine(name: str) -> Engine:
    """Instantiate and return the engine registered for the given name.

    Raises:
        NotImplementedError: If the engine is recognized but not yet implemented.
        ValueError: If the engine name is completely unknown.
    """
    key = name.lower()
    if key in ("hive", "pig", "mapreduce") and key not in _REGISTRY:
        raise NotImplementedError(
            f"Engine '{name}' is part of the project specification but has not been "
            f"implemented yet. Please select 'mongodb' or 'dummy'."
        )

    if key not in _REGISTRY:
        # Dynamically attempt import of mongodb if requested
        if key == "mongodb":
            try:
                from pipelines.mongodb.engine import MongoDBEngine

                register_engine("mongodb", MongoDBEngine)
                return MongoDBEngine()
            except ImportError as e:
                raise ImportError(f"Could not load MongoDBEngine: {e}") from e

        available = ", ".join(list(_REGISTRY.keys()) + ["mongodb", "hive", "pig", "mapreduce"])
        raise ValueError(f"Unknown engine '{name}'. Available engines: {available}")

    return _REGISTRY[key]()


def list_available_engines() -> list[str]:
    """Return a list of all recognized engine names."""
    return ["mongodb", "hive", "pig", "mapreduce", "dummy"]
