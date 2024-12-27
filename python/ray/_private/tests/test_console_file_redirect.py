import os
import subprocess
import sys
from tempfile import NamedTemporaryFile
import pytest
from ray._private.test_utils import run_string_as_driver_stdout_stderr


@pytest.fixture
def temp_file():
    with NamedTemporaryFile() as f:
        yield f.name


@pytest.fixture
def cleanup_console_log_files():
    yield
    console_log_files = [
        f
        for f in os.listdir(os.getcwd())
        if f.startswith("console_") and f.endswith(".log")
    ]
    for f in console_log_files:
        os.remove(f)


def test_console_log_redirection(temp_file):
    """Test console log redirection to file and tail the log to output to console."""
    script = f"""
import sys
import os
from ray._private.console_file_redirect import setup_console_redirection_to_file

setup_console_redirection_to_file("{temp_file}")
os.write(sys.stdout.fileno(), "hello from stdout\\n".encode())
os.write(sys.stderr.fileno(), "hello from stderr\\n".encode())
    """
    try:
        subprocess.run(
            ["python", "-c", script], capture_output=True, text=True, timeout=1
        )
    except subprocess.TimeoutExpired as e:
        assert e.stdout.decode() == "hello from stdout\nhello from stderr\n"

    with open(temp_file, "r") as f:
        assert f.readlines() == ["hello from stdout\n", "hello from stderr\n"]


def test_console_redirect_run_with_scripts_and_env_var_e2e(cleanup_console_log_files):
    """
    Run a ray job and verify the console log redirection behavior.

    Run the job directly through a script with corresponding environment variable set.
    Expect the console logs to be redirected to a file and tail the log to output to
    console.
    """
    script = """
import os
import time
import ray

@ray.remote
def hello_world():
    print ("Hello world")
    return "hello world"

ray.init()
print(ray.get([hello_world.remote() for i in range(2)]))
time.sleep(1)
    """

    stdout_str, stderr_str = run_string_as_driver_stdout_stderr(
        script, env={"RAY_DRIVER_CONSOLE_LOG_TO_FILE": "1", "PYTHONUNBUFFERED": "1"}
    )
    # Check stdout to make sure the log lines are printed.
    assert "Started a local Ray instance." in stdout_str
    assert "Hello world" in stdout_str
    assert "['hello world', 'hello world']" in stdout_str

    # Check the console log file to make sure the log lines are persisted.
    console_log_files = [
        f
        for f in os.listdir(os.getcwd())
        if f.startswith("console_") and f.endswith(".log")
    ]
    assert len(console_log_files) == 1
    with open(console_log_files[0], "r") as f:
        lines = f.read()
        assert "Started a local Ray instance." in lines
        assert "Hello world" in lines
        assert "['hello world', 'hello world']" in lines


def test_console_redirect_run_with_scripts_no_env_var_e2e(cleanup_console_log_files):
    """
    Run a ray job and verify the console log redirection behavior.

    Run the job directly through a script without corresponding environment variable
    set. Expect the console logs not be redirected to a file.
    """
    script = """
import os
import time
import ray

@ray.remote
def hello_world():
    print ("Hello world")
    return "hello world"

ray.init()
print(ray.get([hello_world.remote() for i in range(2)]))
time.sleep(1)
    """

    stdout_str, stderr_str = run_string_as_driver_stdout_stderr(
        script, env={"PYTHONUNBUFFERED": "1"}
    )
    # Check stdout to make sure the log lines are printed.
    assert "Started a local Ray instance." in stdout_str + stderr_str
    assert "Hello world" in stdout_str + stderr_str
    assert "['hello world', 'hello world']" in stdout_str + stderr_str

    # Check the console log file to make sure the log lines are persisted.
    console_log_files = [
        f
        for f in os.listdir(os.getcwd())
        if f.startswith("console_") and f.endswith(".log")
    ]
    assert len(console_log_files) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
