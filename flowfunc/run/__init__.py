"""
Exposes the primary classes for managing and inspecting a workflow run.

This module provides the main entry point for executing a workflow,
the `WorkflowRunCoordinator`, and the data model for the run's result, `Summary`.
"""

from .coordinator import WorkflowRunCoordinator
from .summary_model import Status
from .summary_model import Summary

__all__ = [
    "Status",
    "Summary",
    "WorkflowRunCoordinator",
]
