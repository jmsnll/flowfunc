import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import cast

from pydantic import BaseModel
from pydantic import DirectoryPath
from pydantic import Field

from flowfunc import locations
from flowfunc.io import serializer
from flowfunc.workflow.schema import FlowFuncPipelineModel
from flowfunc.workflow.schema import GlobalInputItem
from flowfunc.workflow.schema import PipefuncCacheConfigUsed
from flowfunc.workflow.schema import PipelineConfigModel
from flowfunc.workflow.schema import PipelineOutputItem
from flowfunc.workflow.schema import RunInfoModel

logger = logging.getLogger(__name__)


class ArtifactPaths(BaseModel):
    """Defines the standard directory paths for a given workflow run."""

    run_dir: DirectoryPath = Field(
        description="The root directory for this specific run."
    )
    workflow_output_dir: DirectoryPath = Field(
        description="Directory where workflow outputs will be saved."
    )
    cache_dir: Path = Field(
        description="Directory for pipefunc's run-specific cache, if used."
    )

    class Config:
        arbitrary_types_allowed = True


def sanitize_workflow_name(workflow_name: str) -> str:
    return (
        "".join(
            c if c.isalnum() or c in ["-", "_"] else "_" for c in workflow_name
        ).strip("_")
        or "unnamed_workflow"
    )


def generate_unique_run_id(run_name: str | None = None) -> str:
    """
    Generates a unique ID for the run, adding a prefix is provided.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]
    prefix = sanitize_workflow_name(run_name) if run_name else "run"
    run_id = f"{prefix}_{timestamp}_{unique_suffix}"
    logger.debug(f"Generated run ID: {run_id}")
    return run_id


def setup_directories(
    run_id: str,
    workflow_name: str,
    run_directory: str,
) -> ArtifactPaths:
    """
    Determines and creates necessary directories for a workflow run.
    Returns a RunArtifactPaths Pydantic model.
    Raises PathSetupError if directory creation fails.
    """
    logger.info(
        f"Setting up run directories for run ID: {run_id}, workflow: '{workflow_name}'"
    )

    global_runs_base_dir = locations.project_root() / run_directory
    logger.debug(
        f"Using project output root: {global_runs_base_dir} (from pyproject/default: {run_directory})"
    )

    sanitized_workflow_name = sanitize_workflow_name(workflow_name)
    logger.debug(f"Sanitized workflow name for path: {sanitized_workflow_name}")

    run_dir_path = global_runs_base_dir / sanitized_workflow_name / run_id
    workflow_output_dir_path = run_dir_path / "outputs"

    cache_dir_path = run_dir_path / ".pipefunc_cache"

    try:
        run_dir_path.mkdir(parents=True, exist_ok=True)
        workflow_output_dir_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Run directory: {run_dir_path}")
        logger.info(f"Workflow output directory: {workflow_output_dir_path}")
        logger.info(f"Cache directory: {cache_dir_path}")

        return ArtifactPaths(
            run_dir=run_dir_path,
            workflow_output_dir=workflow_output_dir_path,
            cache_dir=cache_dir_path,
        )
    except OSError as e:
        raise OSError(
            f"Failed to create directory structure at {run_dir_path}: {e}"
        ) from e
    except Exception as e:
        raise Exception(
            f"Failed to initialize run paths for {run_dir_path}: {e}"
        ) from e


def load_initial_inputs_from_sources(
    inputs_json_str_option: str | None,
    inputs_file_option: str | None,
) -> dict[str, Any]:
    """
    Loads initial inputs from a JSON string or a JSON file.
    Raises ValueError for parsing issues or file problems.
    """
    logger.debug(
        f"Attempting to load initial inputs (file: {inputs_file_option}, string provided: {bool(inputs_json_str_option)})"
    )

    if inputs_file_option:
        return _load_inputs_from_file(Path(inputs_file_option))

    if inputs_json_str_option:
        return _load_inputs_from_json(inputs_json_str_option)

    logger.info(
        "No input file or JSON string provided; returning empty dictionary for initial inputs."
    )
    return {}


def _load_inputs_from_file(file_path: Path) -> dict[str, Any]:
    logger.info(f"Loading inputs from file: {file_path}")

    if not file_path.exists() or not file_path.is_file():
        raise ValueError(f"Input file not found or not a valid file: {file_path}")

    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in input file {file_path}") from e
    except OSError as e:
        raise ValueError(f"Could not read input file {file_path}") from e

    if not isinstance(data, dict):
        raise ValueError(
            f"Inputs from file {file_path} must be a JSON object (dictionary)."
        )

    logger.info(f"Successfully loaded inputs from file {file_path}.")
    return data


def _load_inputs_from_json(json_str: str) -> dict[str, Any]:
    logger.info("Loading inputs from JSON string option.")
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON in --inputs parameter") from e

    if not isinstance(data, dict):
        raise ValueError(
            "Inputs from --inputs parameter must be a JSON object (dictionary)."
        )

    logger.info("Successfully loaded inputs from JSON string option.")
    return data


def resolve_inputs(
    initial_inputs: dict[str, Any],
    global_input_definitions: dict[str, GlobalInputItem] | None,
    pipeline_expected_input_names: tuple[str, ...],
    pipeline_required_input_names: list[str],
) -> dict[str, Any]:
    """
    Applies defaults to initial inputs and validates against required inputs.
    Returns the final effective input dictionary.
    Raises EnvironmentError if validation fails.
    """
    logger.debug("Resolving effective inputs...")
    resolved = initial_inputs.copy()
    expected_set = set(pipeline_expected_input_names)

    if global_input_definitions:
        for name, definition in global_input_definitions.items():
            if (
                name not in resolved
                and definition.default is not None
                and name in expected_set
            ):
                resolved[name] = definition.default
                logger.debug(
                    f"Applied default for global input '{name}': {definition.default!r}"
                )
            elif name not in expected_set:
                logger.debug(
                    f"Skipping default for '{name}'; not expected by pipefunc."
                )
    else:
        logger.debug("No global input definitions found in workflow model.")

    if missing := sorted(set(pipeline_required_input_names) - resolved.keys()):
        logger.error(f"Validation failed. Missing required inputs: {missing}")
        raise ValueError(f"Validation failed. Missing required inputs: {missing}")

    logger.info("Effective inputs resolved and validated successfully.")
    return resolved


def determine_pipefunc_cache_kwargs(
    pipeline_config: PipelineConfigModel | None,
    run_cache_dir: Path = None,
) -> dict[str, Any] | None:
    """
    Determines the cache_kwargs for pipefunc based on workflow config and CLI options.
    """
    logger.debug("Determining pipefunc cache kwargs...")

    if not pipeline_config:
        logger.debug("No pipeline_config found in workflow model.")
        return None

    cache_type = (
        pipeline_config.cache_type.value if pipeline_config.cache_type else None
    )
    cache_kwargs = dict(pipeline_config.cache_kwargs or {})

    if cache_type:
        logger.debug(f"Workflow defines cache_type: {cache_type}")
    if cache_kwargs:
        logger.debug(f"Workflow defines cache_kwargs: {cache_kwargs}")

    if cache_type == "disk":
        existing_dir = cache_kwargs.get("cache_dir")

        if run_cache_dir or not existing_dir:
            cache_kwargs["cache_dir"] = str(run_cache_dir)
            reason = (
                "forced by CLI" if run_cache_dir else "defaulting to run-specific dir"
            )
            logger.info(f"Using run-specific disk cache: {run_cache_dir} ({reason})")
        else:
            logger.info(f"Using disk cache_dir from YAML: {existing_dir}")

    elif cache_type:
        logger.info(f"Using cache_type '{cache_type}' with kwargs: {cache_kwargs}")
    else:
        logger.info(
            "No cache_type specified; no caching will be configured by pipefunc."
        )
        return None

    return cache_kwargs


def _serialize_output(
    data_to_save: Any,
    output_name: str,
    target_path: Path,
) -> str:
    """
    Internal logic to serialize and save a single output.
    Returns the relative path string of the saved file.
    Raises OutputSerializationError or underlying OS/IO errors.
    """
    file_suffix = target_path.suffix.lower()
    project_root = locations.project_root()

    if not (serializer_func := serializer.lookup(file_suffix)):
        msg = (
            f"Unsupported extension '{file_suffix}' for output '{output_name}' at '{target_path}'. "
            "No serializer found."
        )
        logger.error(msg)
        raise OSError(msg)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    relative_saved_path = str(target_path.relative_to(project_root))

    logger.debug(
        f"Attempting to save output '{output_name}' to '{relative_saved_path}'"
    )
    serializer_func(data_to_save, target_path)
    logger.info(f"Successfully saved output '{output_name}' to '{relative_saved_path}'")

    return relative_saved_path


def persist_workflow_outputs(
    workflow_results: dict[str, Any],
    workflow_output_definitions: list[PipelineOutputItem | str] | None,
    target_output_directory: Path,
) -> dict[str, str]:
    """
    Saves declared workflow outputs to disk based on their definitions.
    Returns a manifest of {output_name: relative_saved_path}.
    Individual serialization errors are logged but do not stop other outputs from being processed.
    """
    logger.info("Persisting workflow outputs...")
    manifest: dict[str, str] = {}

    if not workflow_output_definitions or not workflow_results:
        logger.debug(
            "No output definitions or no pipefunc results; nothing to persist."
        )
        return manifest

    for output_definition in workflow_output_definitions:
        if isinstance(output_definition, str):
            # Output declared by name only, not explicitly saved by this function
            if output_definition in workflow_results:
                logger.warning(
                    f"Output '{output_definition}' declared by name only: Value present in results, but not saved."
                )
            continue

        output_item = cast("PipelineOutputItem", output_definition)
        if not output_item.path:
            logger.debug(
                f"Output '{output_item.name}' has no 'path' defined; skipping persistence."
            )
            continue
        if output_item.name not in workflow_results:
            logger.warning(
                f"Output '{output_item.name}' defined with path '{output_item.path}' but not found in pipefunc results; skipping."
            )
            continue

        # Ensure path is not absolute, resolve relative to target_output_directory
        target_save_path = (target_output_directory / output_item.path).resolve()

        try:
            relative_path_str = _serialize_output(
                workflow_results[output_item.name].output,
                output_name=output_item.name,
                target_path=target_save_path,
            )
            manifest[output_item.name] = relative_path_str
        except Exception as e:
            raise OSError(
                f"Serialization/DiskIO failed for output '{output_item.name}' to '{target_save_path}': {e}"
            ) from e

    logger.info(f"Output persistence complete. Manifest: {manifest}")
    return manifest


def prepare_run_info_model(
    run_id: str,
    status: str,  # "SUCCESS" or "FAILED"
    start_time: datetime,
    end_time: datetime,
    flowfunc_version_str: str,
    workflow_model_instance: FlowFuncPipelineModel,
    workflow_file_cli_arg: str,  # Original path string given to CLI
    initial_user_inputs: dict[str, Any],
    final_effective_inputs: dict[str, Any],
    persisted_outputs_manifest: dict[str, str],
    effective_cache_config: PipefuncCacheConfigUsed
    | None,  # The Pydantic model instance
    run_artifacts_dir_abs_path: Path,  # Absolute path to the run's root artifact directory
) -> RunInfoModel:
    """Constructs the RunInfoModel Pydantic object from all collected run data."""
    logger.debug(f"Preparing RunInfoModel for run ID: {run_id}")

    try:
        workflow_file_path = str(
            Path(workflow_file_cli_arg).resolve().relative_to(locations.project_root())
        )
    except ValueError:
        logger.warning(
            f"Workflow file '{workflow_file_cli_arg}' is outside project root '{locations.project_root()}'. Using absolute path for run_info."
        )
        workflow_file_path = str(Path(workflow_file_cli_arg).resolve())

    try:
        run_artifacts_base_dir_rel_path = str(
            run_artifacts_dir_abs_path.relative_to(locations.project_root())
        )
    except ValueError:
        logger.warning(
            f"Run artifacts directory '{run_artifacts_dir_abs_path}' is outside project root '{locations.project_root()}'. Using absolute path for run_info."
        )
        run_artifacts_base_dir_rel_path = str(run_artifacts_dir_abs_path)

    cache_config_to_log = effective_cache_config or PipefuncCacheConfigUsed(
        cache_type=None, cache_kwargs={}
    )

    # Create the Pydantic model instance
    # Pydantic will perform validation based on the RunInfoModel schema
    run_info = RunInfoModel(
        run_id=run_id,
        flowfunc_version=flowfunc_version_str,
        workflow_metadata_name=workflow_model_instance.metadata.name,
        workflow_metadata_version=workflow_model_instance.metadata.version,
        workflow_file_relative_path=workflow_file_path,
        status=status,
        start_time_utc=start_time.astimezone(),  # Ensure timezone aware for ISO format
        end_time_utc=end_time.astimezone(),  # Ensure timezone aware for ISO format
        duration_seconds=round((end_time - start_time).total_seconds(), 3),
        user_provided_inputs=initial_user_inputs,
        effective_inputs_used_by_pipefunc=final_effective_inputs,
        flowfunc_persisted_outputs=persisted_outputs_manifest,
        pipefunc_cache_config_used=cache_config_to_log,
        run_artifacts_base_dir_relative=run_artifacts_base_dir_rel_path,
    )
    logger.info(f"RunInfoModel prepared for run ID: {run_id}")
    return run_info
