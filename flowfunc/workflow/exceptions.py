from __future__ import annotations

from flowfunc.core.exceptions.base import FlowFuncCoreError


class WorkflowLoadError(FlowFuncCoreError):
    pass


class WorkflowSchemaValidationError(FlowFuncCoreError):
    pass


class PipelineBuildError(FlowFuncCoreError):
    pass


class CallableImportError(FlowFuncCoreError):
    pass
