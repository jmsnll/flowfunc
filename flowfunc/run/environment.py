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
            self.project_config: dict[str, Any] = load_flowfunc_toml(
                config_file_path=self._config_file_path
            )
        except ConfigLoaderError as e:
            logger.warning(
                f"Could not load project configuration: {e}. Proceeding with default settings."
            )
            self.project_config = {}

        self.runs_base_dir_name: str = self.project_config.get(
            "runs_directory", ".flowfunc_runs"
        )
        self.runs_base_dir: Path = self._project_root / self.runs_base_dir_name

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
                "Workflow name cannot be empty for setting up run directories."
            )
        if not run_id:
            raise RunEnvironmentManagerError(
                "Run ID cannot be empty for setting up run directories."
            )

        try:
            # Sanitize workflow_name for directory creation if needed
            safe_workflow_name = workflow_name.replace(" ", "_").lower()
            run_dir = self.runs_base_dir / safe_workflow_name / run_id
            output_dir = run_dir / "outputs"  # Consistent with Summary.output_dir logic

            ensure(run_dir)  # Creates directory if it doesn't exist
            ensure(output_dir)  # Creates output directory

            logger.info(f"Run directory set up at: {run_dir}")
            logger.info(f"Output directory set up at: {output_dir}")
            return run_dir, output_dir
        except Exception as e:
            raise RunEnvironmentManagerError(
                f"Failed to set up run directories for {workflow_name}/{run_id}: {e}"
            ) from e
