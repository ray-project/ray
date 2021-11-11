from pathlib import Path
import tempfile

import pytest


@pytest.fixture(
    scope="function",
    params=["no_working_dir", "local_working_dir", "s3_working_dir"])
def working_dir_option(request):
    if request.param == "no_working_dir":
        yield {
            "working_dir": None,
            "entrypoint": "echo hello",
            "expected_logs": "hello\n",
        }
    elif request.param == "local_working_dir":
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)

            hello_file = path / "test.py"
            with hello_file.open(mode="w") as f:
                f.write("from test_module import run_test\n")
                f.write("print(run_test())")

            module_path = path / "test_module"
            module_path.mkdir(parents=True)

            test_file = module_path / "test.py"
            with test_file.open(mode="w") as f:
                f.write("def run_test():\n")
                f.write("    return 'Hello from test_module!'\n")

            init_file = module_path / "__init__.py"
            with init_file.open(mode="w") as f:
                f.write("from test_module.test import run_test\n")

            yield {
                "working_dir": tmp_dir,
                "entrypoint": "python test.py",
                "expected_logs": "Hello from test_module!\n",
            }
    elif request.param == "s3_working_dir":
        yield {
            "working_dir": "s3://runtime-env-test/script.zip",
            "entrypoint": "python script.py",
            "expected_logs": "Executing main() from script.py !!\n",
        }
    else:
        assert False, f"Unrecognized option: {request.param}."
