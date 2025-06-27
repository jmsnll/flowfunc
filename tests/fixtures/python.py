import importlib  # For potentially invalidating caches if needed
import sys
from pathlib import Path

import pytest


@pytest.fixture
def temporary_python_module(tmp_path: Path):
    """
    A fixture to create a temporary Python module on the filesystem
    and add its directory to sys.path.
    """
    original_sys_path = list(sys.path)
    original_sys_modules = dict(sys.modules)

    # The base path to add to sys.path will be tmp_path itself.
    # If a subdir is used, it's a subdirectory *within* tmp_path,
    # but tmp_path remains the root for Python's import resolution.
    if str(tmp_path) not in sys.path:
        sys.path.insert(0, str(tmp_path))

    created_module_fqns = []

    def _create_module(module_fqn: str, content: str, subdir: str | None = None) -> str:
        """
        Creates a .py file with the given content.
        module_fqn: The fully qualified name of the module (e.g., 'my_module' or 'my_package.my_module').
        content: The Python code to write into the module.
        subdir: Optional subdirectory within tmp_path where the module/package root should be.

        Returns the fully qualified name of the created module.
        """
        # Determine the base directory for module creation (either tmp_path or tmp_path/subdir)
        package_root_dir = tmp_path
        if subdir:
            package_root_dir = tmp_path / subdir
            package_root_dir.mkdir(parents=True, exist_ok=True)

        parts = module_fqn.split(".")
        current_path = package_root_dir

        # Create package directories and __init__.py files if it's a nested module
        for i, part in enumerate(parts[:-1]):
            current_path = current_path / part
            current_path.mkdir(exist_ok=True)
            init_file = current_path / "__init__.py"
            if not init_file.exists():
                init_file.write_text(f"# Temporary __init__.py for {part}\n")

        # Create the final module file
        module_file_path = current_path / f"{parts[-1]}.py"
        module_file_path.write_text(content)

        created_module_fqns.append(module_fqn)
        return module_fqn

    yield _create_module

    # Teardown: Restore sys.path and sys.modules
    sys.path = original_sys_path

    # More robustly clean sys.modules
    modules_to_remove = [
        m
        for m in sys.modules
        if m in created_module_fqns
        or any(m.startswith(fqn + ".") for fqn in created_module_fqns)
    ]
    for module_name in modules_to_remove:
        if module_name in sys.modules:
            del sys.modules[module_name]

    # Invalidate importlib caches for the removed modules/paths (Python 3.7+)
    # This helps ensure that Python re-evaluates imports if paths are reused or modules redefined.
    importlib.invalidate_caches()
