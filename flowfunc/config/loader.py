import logging
from pathlib import Path
from typing import Any

import toml

from flowfunc.locations import project_root

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILE_NAME = "pyproject.toml"
DEFAULT_FLOWFUNC_CONFIG_KEY = "tool.flowfunc"


class ConfigLoaderError(Exception):
    """Custom exception for configuration loading errors."""


def load_flowfunc_toml(
    config_file_path: Path | None = None,
    flowfunc_config_key: str = DEFAULT_FLOWFUNC_CONFIG_KEY,
) -> dict[str, Any]:
    """
    Loads the FlowFunc specific configuration from a TOML file (typically pyproject.toml).

    Args:
        config_file_path: Optional path to the TOML configuration file.
                          If None, defaults to 'pyproject.toml' in the project root.
        flowfunc_config_key: The dot-separated key to access FlowFunc's
                             configuration within the TOML file (e.g., "tool.flowfunc").

    Returns:
        A dictionary containing the FlowFunc configuration, or an empty dict if not found.

    Raises:
        ConfigLoaderError: If the file cannot be read or parsed, or if the TOML is malformed.
    """
    if config_file_path is None:
        try:
            # This assumes project_root() correctly identifies the root directory
            # where pyproject.toml is expected.
            root_dir = project_root()
            config_file_path = root_dir / DEFAULT_CONFIG_FILE_NAME
        except Exception as e:  # Catch potential errors from project_root() itself
            logger.warning(
                f"Could not determine project root to find default configuration file. "
                f"Please specify config_file_path. Error: {e}"
            )
            return {}

    logger.debug(
        f"Attempting to load FlowFunc configuration from: {config_file_path} using key: '{flowfunc_config_key}'"
    )

    if not config_file_path.exists():
        logger.warning(
            f"Configuration file not found: {config_file_path}. "
            f"No FlowFunc configuration will be loaded."
        )
        return {}

    try:
        data = toml.load(config_file_path)

        # Navigate through the nested keys (e.g., "tool.flowfunc")
        keys = flowfunc_config_key.split(".")
        current_level_config = data
        for key in keys:
            if isinstance(current_level_config, dict):
                current_level_config = current_level_config.get(key, {})
            else:  # Should not happen if structure is as expected
                current_level_config = {}
                break

        if not isinstance(
            current_level_config, dict
        ):  # Ensure the final resolved config is a dict
            logger.warning(
                f"Expected a dictionary at key '{flowfunc_config_key}' in {config_file_path}, "
                f"but found type {type(current_level_config)}. Returning empty config."
            )
            return {}

        logger.info(
            f"Successfully loaded FlowFunc configuration from {config_file_path} "
            f"under key '{flowfunc_config_key}'."
        )
        return current_level_config

    except toml.TomlDecodeError as e:
        raise ConfigLoaderError(
            f"Error decoding TOML file {config_file_path}: {e}"
        ) from e
    except OSError as e:  # Catch file reading issues
        raise ConfigLoaderError(
            f"Could not read configuration file {config_file_path}: {e}"
        ) from e
    except Exception as e:  # Catch any other unexpected errors during loading
        raise ConfigLoaderError(
            f"An unexpected error occurred while loading configuration from {config_file_path}: {e}"
        ) from e
