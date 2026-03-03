import os.path
from pathlib import Path
import importlib.util


def import_and_execute_test_script(relative_path_to_test_script: str):
    """Imports and executes a module from a path relative to Ray repo root."""
    # get the ray folder
    ray_path = Path(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    )
    notebook_path = ray_path.joinpath(relative_path_to_test_script)
    assert notebook_path.exists()

    spec = importlib.util.spec_from_file_location("notebook_test", notebook_path)
    notebook_test_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(notebook_test_module)
