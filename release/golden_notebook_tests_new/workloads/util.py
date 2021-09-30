from pathlib import Path
import importlib.util


def import_and_execute_test_script(relative_path_to_test_script: str):
    # get the ray folder
    ray_path = next(
        x for x in Path(__file__).resolve().parents if str(x).endswith("/ray"))
    notebook_path = ray_path.joinpath(relative_path_to_test_script)
    assert notebook_path.exists()

    spec = importlib.util.spec_from_file_location("notebook_test",
                                                  notebook_path)
    notebook_test_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(notebook_test_module)
