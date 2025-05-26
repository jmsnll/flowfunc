from __future__ import annotations

from typing import TYPE_CHECKING

from flowfunc.__version__ import __version__

if TYPE_CHECKING:
    from flowfunc.config.config import Config


class FlowFunc:
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

    def set_config(self, config: Config) -> FlowFunc:
        self._config = config
        return self
