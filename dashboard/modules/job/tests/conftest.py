import pytest


@pytest.fixture(scope="function")
def local_working_dir():
    yield {
        "runtime_env": {},
        "entrypoint": "echo hello",
        "expected_stdout": "hello",
        "expected_stderr": ""
    }


@pytest.fixture(scope="function")
def s3_working_dir():
    yield {
        "runtime_env": {
            "working_dir": "s3://runtime-env-test/script.zip",
        },
        "entrypoint": "python script.py",
        "expected_stdout": "Executing main() from script.py !!",
        "expected_stderr": ""
    }
