import logging
from pathlib import Path
from typing import Any

from flowfunc.config.loader import load_flowfunc_toml
from flowfunc.exceptions import ConfigLoaderError
from flowfunc.exceptions import RunEnvironmentManagerError
from flowfunc.locations import ensure
from flowfunc.locations import project_root

logger = logging.getLogger(__name__)


class RunEnvironmentManager:
    """Manages the setup of the run environment, including directories and project configuration."""

    def __init__(self, config_file_path: Path | None = None) -> None:
        self._project_root = project_root()
        self._config_file_path = config_file_path

        try:
            self.project_config: dict[str, Any] = load_flowfunc_toml(config_file_path)
        except ConfigLoaderError as e:
            logger.warning(f"Config load failed: {e}. Using defaults.")
            self.project_config = {}

        runs_dir_name = self.project_config.get("runs_directory", ".flowfunc_runs")
        self.runs_base_dir: Path = self._project_root / runs_dir_name

    def setup_run_directories(
        self, workflow_name: str, run_id: str
    ) -> tuple[Path, Path]:
        """
        Creates and returns the run-specific directory and output directory.

        Returns:
            tuple[Path, Path]: (run_dir, output_dir)
        """
        if not workflow_name:
            raise RunEnvironmentManagerError(
                "Workflow name and run ID must be provided."
            )

        try:
            safe_name = workflow_name.replace(" ", "_").lower()
            run_dir = self.runs_base_dir / safe_name / run_id
            output_dir = run_dir / "outputs"

            ensure(run_dir)
            ensure(output_dir)

            logger.info(f"Run dir: {run_dir}")
            logger.info(f"Output dir: {output_dir}")
            return run_dir, output_dir
        except Exception as e:
            raise RunEnvironmentManagerError(
                f"Failed to set up run directories for {workflow_name}/{run_id}: {e}"
            ) from e
