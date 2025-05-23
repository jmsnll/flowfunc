from __future__ import annotations

from typing import TYPE_CHECKING

from pipeflow.__version__ import __version__

if TYPE_CHECKING:
    from pipeflow.config.config import Config


class Pipeflow:
    VERSION = __version__

    def __init__(
        self,
        config: Config,
        disable_cache: bool = False,
    ) -> None:
        self._config = config
        self._disable_cache = disable_cache

    @property
    def config(self) -> Config:
        return self._config

    def set_config(self, config: Config) -> Pipeflow:
        self._config = config
        return self
