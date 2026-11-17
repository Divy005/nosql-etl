"""Pipeline engine registry and factory."""

from pipelines.base import Engine
from pipelines.dummy.engine import DummyEngine

_REGISTRY: dict[str, type[Engine]] = {
    "dummy": DummyEngine,
}


# Shorthands accepted on the command line (--pipeline / --engine). Each maps to ONE
# canonical name, and only the canonical name is ever stored (etl_runs.pipeline and the
# result tables), so a run is never recorded under two different names. The canonical
# names follow the project statement: "MongoDB", "Hive", "Pig", "MapReduce".
ENGINE_ALIASES: dict[str, str] = {
    "mongo": "mongodb",
    "mr": "mapreduce",
    "map-reduce": "mapreduce",
}


def canonical_engine_name(name: str) -> str:
    """Return the canonical engine name for a name or alias (case-insensitive)."""
    key = name.strip().lower()
    return ENGINE_ALIASES.get(key, key)


def register_engine(name: str, engine_cls: type[Engine]) -> None:
    """Register an engine implementation class under a given name."""
    _REGISTRY[name.lower()] = engine_cls


def get_engine(name: str) -> Engine:
    """Instantiate and return the engine registered for the given name.

    Raises:
        NotImplementedError: If the engine is recognized but not yet implemented.
        ValueError: If the engine name is completely unknown.
    """
    key = canonical_engine_name(name)
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
    """Return the canonical engine names (aliases are listed in ENGINE_ALIASES)."""
    return ["mongodb", "hive", "pig", "mapreduce", "dummy"]
