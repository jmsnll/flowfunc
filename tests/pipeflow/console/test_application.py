# tests/console/test_application.py
import importlib
import inspect
import sys  # To temporarily add to sys.path if needed for discovery from non-installed package
from pathlib import Path

import pytest

# Import the COMMANDS list from your application module
from pipeflow.console.application import COMMANDS as APP_DEFINED_COMMAND_NAMES

# Import your base Command classes to exclude them from discovery
from pipeflow.console.commands.command import Command as PipeflowBaseCommand
from pipeflow.console.commands.command import WorkflowCommand as PipeflowWorkflowCommand

# Determine the path to the main 'pipeflow' package directory and 'commands' directory
# This is more robust for finding the commands directory, assuming tests might be run
# from different locations or before the package is formally installed.
try:
    import pipeflow

    PIPEFLOW_PACKAGE_ROOT = Path(pipeflow.__file__).parent
    COMMANDS_MODULE_PATH = PIPEFLOW_PACKAGE_ROOT / "console" / "commands"
    # Add the project root to sys.path to ensure modules inside 'pipeflow' are discoverable
    # This is often needed if 'pipeflow' is not installed in editable mode during testing
    PROJECT_ROOT = PIPEFLOW_PACKAGE_ROOT.parent
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

except (ImportError, AttributeError) as e:
    # Fallback or skip if the path cannot be determined reliably
    pytest.skip(
        f"Could not determine commands directory path: {e}. Ensure 'pipeflow' is discoverable.",
        allow_module_level=True,
    )


def discover_command_names_from_modules() -> set[str]:
    """
    Recursively scans the commands directory, imports modules, and extracts command names
    from classes inheriting from PipeflowBaseCommand.
    """
    discovered_command_names = set()

    if not COMMANDS_MODULE_PATH.is_dir():
        raise FileNotFoundError(
            f"Commands directory not found at inferred path: {COMMANDS_MODULE_PATH}"
        )

    for py_file in COMMANDS_MODULE_PATH.rglob("*.py"):  # Use rglob for recursive search
        # Construct the module import path relative to 'pipeflow.console.commands'
        relative_path = py_file.relative_to(COMMANDS_MODULE_PATH)

        # Create module path parts: e.g., generate/schema.py -> ['generate', 'schema']
        module_path_parts = list(relative_path.parts)
        module_path_parts[-1] = relative_path.stem  # Get filename without extension

        sub_module_string = ".".join(module_path_parts)
        module_import_path = f"pipeflow.console.commands.{sub_module_string}"

        try:
            module = importlib.import_module(module_import_path)
            for member_name, member_class in inspect.getmembers(
                module, inspect.isclass
            ):
                if (
                    member_class.__module__ == module_import_path
                ):  # Class defined in this module
                    if issubclass(
                        member_class, PipeflowBaseCommand
                    ) and member_class not in [
                        PipeflowBaseCommand,
                        PipeflowWorkflowCommand,
                    ]:
                        if hasattr(member_class, "name") and isinstance(
                            member_class.name, str
                        ):
                            discovered_command_names.add(member_class.name)
                        else:
                            print(
                                f"Warning: Command class {member_class} in {module_import_path} "
                                "is missing a 'name' attribute or it's not a string."
                            )

        except ImportError as e:
            pytest.fail(
                f"Failed to import command module: {module_import_path}. Original error: {e}"
            )
        except Exception as e:
            pytest.fail(f"Error inspecting module {module_import_path}: {e}")

    return discovered_command_names


def test_application_commands_list_is_complete_and_accurate():
    """
    Ensures the COMMANDS list in application.py accurately reflects all defined
    Command classes in the pipeflow.console.commands directory and its subdirectories.
    """
    defined_command_names_in_code = discover_command_names_from_modules()
    listed_command_names_in_app = set(APP_DEFINED_COMMAND_NAMES)

    missing_from_list = defined_command_names_in_code - listed_command_names_in_app
    assert not missing_from_list, (
        f"The following commands are defined in modules but MISSING from the COMMANDS list "
        f"in pipeflow/console/application.py: {sorted(list(missing_from_list))}"
    )

    missing_from_code = listed_command_names_in_app - defined_command_names_in_code
    assert not missing_from_code, (
        f"The following commands are listed in COMMANDS in pipeflow/console/application.py "
        f"but no corresponding command class/name attribute was found: {sorted(list(missing_from_code))}"
    )

    # This final assert is redundant if the two above pass but serves as a clear overall check.
    assert listed_command_names_in_app == defined_command_names_in_code, (
        "The set of command names in application.py's COMMANDS list does not exactly match "
        "the set of command names discovered from command modules."
    )
