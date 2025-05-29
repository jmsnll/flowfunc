from __future__ import annotations

from click import ClickException
from tomlkit.exceptions import TOMLKitError


class FlowfuncError(Exception):
    """Base exception for all flowfunc application errors."""

    pass


class FlowFuncConsoleError(FlowfuncError, ClickException):
    """Custom exception for flowfunc console errors."""

    pass


class ConfigLoaderError(FlowfuncError):
    """Custom exception for configuration loading errors."""

    pass


class CallableImportError(FlowfuncError):
    """Raised when a callable string cannot be imported."""

    pass


class WorkflowDefinitionLoaderError(FlowfuncError):
    """Custom exception for workflow definition loading errors."""

    pass


class PipelineBuildError(FlowfuncError):
    """Raised when building the pipeline from the definition fails."""

    pass


class PipelineExecutionError(FlowfuncError):
    """Custom exception for pipeline execution errors."""

    pass


class InputProviderError(FlowfuncError):
    """Custom exception for input providing errors."""

    pass


class InputResolverError(FlowfuncError):
    """Custom exception for input resolution errors."""

    pass


class OutputPersisterError(FlowfuncError):
    """Custom exception for output persisting errors."""

    pass


class SummaryPersistenceError(FlowfuncError):
    """Custom exception for summary persistence errors."""

    pass


class RunEnvironmentManagerError(FlowfuncError):
    """Custom exception for run environment setup errors."""

    pass


class WorkflowRunError(FlowfuncError):
    """Custom exception for errors during workflow run coordination."""

    pass


class SerializerError(FlowfuncError):
    """Custom exception for serializer errors."""

    pass


class TOMLError(TOMLKitError):
    """Custom exception for TOML errors."""

    pass
