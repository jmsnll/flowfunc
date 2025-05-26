import logging
import os
import re
from collections.abc import Callable
from copy import deepcopy
from typing import Any

from flowfunc.config.config_source import ConfigSource
from flowfunc.config.file_config_source import FileConfigSource
from flowfunc.env import get_prefix_env
from flowfunc.locations import CONFIG_DIR
from flowfunc.locations import data_dir
from flowfunc.toml import TOMLFile

logger = logging.getLogger(__name__)
_default_config = None


def boolean_validator(val: str) -> bool:
    return val in {"true", "false", "1", "0"}


def boolean_normalizer(val: str) -> bool:
    return val.lower() in ["true", "1"]


def int_normalizer(val: str) -> int:
    return int(val)


class Config:
    default_config = {
        "data-dir": str(data_dir()),
        "requests": {
            "max-retries": 0,
        },
        "thread-pooling": {
            "max-workers": 4,
        },
        "logging": {
            "level": logging.INFO,
        },
    }

    def __init__(self, use_environment: bool = True) -> None:
        self._config = deepcopy(self.default_config)
        self._use_environment = use_environment

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    @property
    def config_source(self) -> ConfigSource:
        return self._config_source

    def set_config_source(self, config_source: ConfigSource) -> Config:
        self._config_source = config_source
        return self

    def merge(self, config: dict[str, Any]) -> None:
        from flowfunc.utils.helpers import merge_dicts

        merge_dicts(self._config, config)

    def all(self) -> dict[str, Any]:
        def _all(config: dict[str, Any], parent_key: str = "") -> dict[str, Any]:
            all_ = {}

            for key in config:
                value = self.get(parent_key + key)
                if isinstance(value, dict):
                    if parent_key != "":
                        current_parent = parent_key + key + "."
                    else:
                        current_parent = key + "."
                    all_[key] = _all(config[key], parent_key=current_parent)
                    continue

                all_[key] = value

            return all_

        return _all(self.config)

    def raw(self) -> dict[str, Any]:
        return self._config

    @property
    def installer_max_workers(self) -> int:
        try:
            default_max_workers = (os.cpu_count() or 1) + 4
        except NotImplementedError:
            default_max_workers = 5

        desired_max_workers = self.get("thread-pooling.max-workers")
        if desired_max_workers is None:
            return default_max_workers
        return min(default_max_workers, int(desired_max_workers))

    def get(self, setting_name: str, default: Any = None) -> Any:
        """Retrieve a setting value."""
        keys = setting_name.split(".")

        # Looking in the environment if the setting is set via a {ENV_PREFIX}_* environment variable
        if self._use_environment:
            env = "_".join(k.upper().replace("-", "_") for k in keys)
            env_value = get_prefix_env(env)
            if env_value is not None:
                return self.process(self._get_normalizer(setting_name)(env_value))

        value = self._config

        for key in keys:
            if key not in value:
                return self.process(default)

            value = value[key]

        if self._use_environment and isinstance(value, dict):
            # this is a configuration table, it is likely that we missed env vars
            # in order to capture them recurse, eg: requests.headers.include
            return {k: self.get(f"{setting_name}.{k}") for k in value}

        return self.process(value)

    def process(self, value: Any) -> Any:
        if not isinstance(value, str):
            return value

        def resolve_from_config(match: re.Match[str]) -> Any:
            key = match.group(1)
            config_value = self.get(key)
            if config_value:
                return config_value

            # The key doesn't exist in the config but might be resolved later,
            # so we keep it as a format variable.
            return f"{{{key}}}"

        return re.sub(r"{(.+?)}", resolve_from_config, value)

    @staticmethod
    def _get_normalizer(name: str) -> Callable[[str], Any]:
        if name in {}:
            return boolean_normalizer

        if name in {
            "thread-pooling.max-workers",
            "requests.max-retries",
        }:
            return int_normalizer

        return lambda val: val

    @classmethod
    def create(cls, reload: bool = False) -> Config:
        global _default_config

        if _default_config is None or reload:
            _default_config = cls()

            # Load global config
            config_file = TOMLFile(CONFIG_DIR / "config.toml")
            if config_file.exists():
                logger.debug("Loading configuration file %s", config_file.path)
                _default_config.merge(config_file.read())

            _default_config.set_config_source(FileConfigSource(config_file))

        return _default_config
