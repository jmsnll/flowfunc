import json
import pickle
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from flowfunc.exceptions import SerializerError

type DumperFuncType = Callable[[Any, Path, dict[str, Any]], None]
type LoaderFuncType = Callable[[Path, dict[str, Any]], Any]


def _serialize_json(data: Any, path: Path, **kwargs) -> None:
    options = {"indent": 2, "default": str, **kwargs}  # overridable
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, **options)


def _deserialize_json(path: Path, **kwargs) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f, **kwargs)


def _serialize_text(data: Any, path: Path, **kwargs) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(str(data))


def _deserialize_text(path: Path, **kwargs) -> str:
    with path.open("r", encoding="utf-8") as f:
        return f.read()


def _serialize_pickle(data: Any, path: Path, **kwargs) -> None:
    with path.open("wb") as f:
        pickle.dump(data, f, **kwargs)


def _deserialize_pickle(path: Path, **kwargs) -> Any:
    with path.open("rb") as f:
        return pickle.load(f, **kwargs)


def _serialize_yaml(data: Any, path: Path, **kwargs) -> None:
    options = {"indent": 2, "sort_keys": False, "allow_unicode": True, **kwargs}
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(data, f, **options)


def _deserialize_yaml(path: Path, **kwargs) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class Serializer:
    """A consistent interface for dumping and loading data for a specific format."""

    def __init__(
        self,
        name: str,
        dumper: DumperFuncType | None,
        loader: LoaderFuncType | None,
        default_extension: str,
    ) -> None:
        self._name = name
        self._dumper = dumper
        self._loader = loader
        self.default_extension = default_extension

    def dump(self, data: Any, path: Path, **kwargs) -> None:
        """Serializes and writes data to the given path."""
        if not self.can_dump:
            raise SerializerError(
                f"Serializer '{self._name}' does not support dumping (suffix: {path.suffix})."
            )
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._dumper(data, path, **kwargs)  # type: ignore
        except Exception as e:
            raise SerializerError(
                f"Error dumping data to {path} using {self._name} serializer: {e}"
            ) from e

    def load(self, path: Path, **kwargs) -> Any:
        """Loads and deserializes data from the given path."""
        if not self.can_load:
            raise SerializerError(
                f"Serializer '{self._name}' does not support loading (suffix: {path.suffix})."
            )
        try:
            return self._loader(path, **kwargs)  # type: ignore
        except FileNotFoundError:
            raise
        except Exception as e:
            raise SerializerError(
                f"Error loading data from {path} using {self.name} serializer: {e}"
            ) from e

    @property
    def name(self) -> str:
        return self._name

    @property
    def can_dump(self) -> bool:
        return self._dumper is not None

    @property
    def can_load(self) -> bool:
        return self._loader is not None


# Define instances of the Serializer for each supported format.
# This makes the capabilities (dump/load) explicit for each extension.
_SERIALIZER_REGISTRY: dict[str, Serializer] = {
    ".json": Serializer(
        name="json",
        dumper=_serialize_json,
        loader=_deserialize_json,
        default_extension=".json",
    ),
    ".jsonl": Serializer(
        name="json",
        dumper=_serialize_json,
        loader=_deserialize_json,
        default_extension=".jsonl",
    ),  # Alias for JSON logs
    ".pkl": Serializer(
        name="pickle",
        dumper=_serialize_pickle,
        loader=_deserialize_pickle,
        default_extension=".pkl",
    ),
    ".pickle": Serializer(
        name="pickle",
        dumper=_serialize_pickle,
        loader=_deserialize_pickle,
        default_extension=".pickle",
    ),  # Alias for pickle
    ".txt": Serializer(
        name="text",
        dumper=_serialize_text,
        loader=_deserialize_text,
        default_extension=".txt",
    ),
    ".yaml": Serializer(
        name="yaml",
        dumper=_serialize_yaml,
        loader=_deserialize_yaml,
        default_extension=".yaml",
    ),
    ".yml": Serializer(
        name="yaml",
        dumper=_serialize_yaml,
        loader=_deserialize_yaml,
        default_extension=".yml",
    ),  # Alias for yaml
}


def lookup_serializer(key: str | Path) -> Serializer | None:
    """
    Returns a Serializer object with .dump() and .load() methods
    based on the file extension or key.

    Args:
        key: A Path object, or a string representing a file suffix (e.g., ".json").

    Returns:
        An instance of Serializer if found, otherwise None.
    """
    if isinstance(key, Path):
        suffix = key.suffix.lower()
    elif isinstance(key, str):
        suffix = key.lower()
        if not suffix.startswith("."):
            suffix = "." + suffix
    else:
        raise TypeError("lookup_serializer key must be a Path or string suffix.")

    return _SERIALIZER_REGISTRY.get(suffix)
