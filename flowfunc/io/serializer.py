# flowfunc/ Předpokládejme, že toto je v novém souboru, např. `flowfunc/io/serialization.py`

import json
import pickle
from collections.abc import Callable
from pathlib import Path
from typing import Any


def serialize_json(data: Any, path: Path) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def serialize_text(data: Any, path: Path) -> None:
    with open(path, "w") as f:
        f.write(str(data))


def serialize_pickle(data: Any, path: Path) -> None:
    with open(path, "wb") as f:
        pickle.dump(data, f)


_SERIALIZERS: dict[str, Callable[[Any, Path], None]] = {
    ".json": serialize_json,
    ".txt": serialize_text,
    ".pkl": serialize_pickle,
}
