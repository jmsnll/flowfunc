from __future__ import annotations

from pipeflow.core.exceptions.base import PipeflowCoreError


class WorkflowLoadError(PipeflowCoreError):
    pass


class PipelineBuildError(PipeflowCoreError):
    pass


class CallableImportError(PipeflowCoreError):
    pass
