# flowfunc/__init__.py (Updated)

"""
FlowFunc - A Python library to supercharge your `pipefunc` workflows.
"""

from flowfunc.__version__ import __version__
from flowfunc.run import Status
from flowfunc.run import Summary
from flowfunc.run import WorkflowRunCoordinator
from flowfunc.workflow_definition import WorkflowDefinition
from flowfunc.workflow_definition import WorkflowDefinitionLoader

__all__ = [
    "Status",
    "Summary",
    "WorkflowDefinition",
    "WorkflowDefinitionLoader",
    "WorkflowRunCoordinator",
    "__version__",
]
