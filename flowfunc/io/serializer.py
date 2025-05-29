# flowfunc/ Předpokládejme, že toto je v novém souboru, např. `flowfunc/io/serialization.py`

import json
import pickle
from collections.abc import Callable
from pathlib import Path
from typing import Any

from flowfunc.core.exceptions import FlowFuncCoreError


def serialize_json(data: Any, path: Path) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def serialize_text(data: Any, path: Path) -> None:
    with open(path, "w") as f:
        f.write(str(data))


def serialize_pickle(data: Any, path: Path) -> None:
    with open(path, "wb") as f:
        pickle.dump(data, f)


def lookup(key: str | Path) -> Callable[[Any, Path], None] | None:
    """Returns the appropriate serializer function based on the file extension."""
    serializers: dict[str, Callable[[Any, Path], None]] = {
        ".json": serialize_json,
        ".txt": serialize_text,
        ".pkl": serialize_pickle,
    }
    suffix = key.suffix if isinstance(key, Path) else key
    return serializers.get(suffix.lower())


class SerializerError(FlowFuncCoreError):
    pass
