import os
import subprocess
import sys
from unittest import mock

import pytest

from ray._private.runtime_env.context import (
    RuntimeEnvContext,
    build_windows_worker_command,
)
from ray.core.generated.common_pb2 import Language


@pytest.mark.parametrize(
    "py_executable",
    ["uv run --locked python", "python -X utf8", "C:\\missing dir\\python.exe -q"],
)
def test_executable_with_arguments_is_not_quoted(py_executable):
    cmd = build_windows_worker_command([], [py_executable], ["worker.py"])
    assert cmd == f"{py_executable} worker.py"


def test_existing_path_with_spaces_is_quoted(tmp_path):
    exe = tmp_path / "dir with space" / "python.exe"
    exe.parent.mkdir()
    exe.touch()
    cmd = build_windows_worker_command([], [str(exe)], ["worker.py"])
    assert cmd == [str(exe), "worker.py"]
    assert subprocess.list2cmdline(cmd) == f'"{exe}" worker.py'


def test_quoted_path_with_arguments_is_kept_as_written():
    py_executable = '"C:\\Program Files\\Python\\python.exe" -X utf8'
    cmd = build_windows_worker_command([], [py_executable], ["worker.py"])
    assert cmd == f"{py_executable} worker.py"


@pytest.mark.skipif(sys.platform != "win32", reason="Runs the command with cmd.exe.")
def test_cmd_exe_runs_quoted_path_with_arguments(tmp_path):
    script = tmp_path / "dir with space" / "echo args.cmd"
    script.parent.mkdir()
    script.write_text("@echo %*\n")
    cmd = build_windows_worker_command(
        [], [f'"{script}" --flag'], ["C:\\a b\\worker.py"]
    )
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == '--flag "C:\\a b\\worker.py"'


def test_executable_without_spaces_is_unchanged():
    cmd = build_windows_worker_command(["prefix"], ["python.exe"], ["worker.py"])
    assert cmd == ["prefix", "python.exe", "worker.py"]


def test_no_executable_is_unchanged():
    cmd = build_windows_worker_command([], [], ["worker.exe", "--flag"])
    assert cmd == ["worker.exe", "--flag"]


def test_prefix_and_arguments_are_still_quoted():
    cmd = build_windows_worker_command(
        ["pre fix"], ["uv run python"], ["C:\\a b\\worker.py", "--x=1", ""]
    )
    assert cmd == '"pre fix" uv run python "C:\\a b\\worker.py" --x=1 ""'


def test_empty_prefix_and_arguments_add_no_spaces():
    assert build_windows_worker_command([], ["uv run python"], []) == "uv run python"


@pytest.mark.parametrize(
    "py_executable,expected",
    [
        ("uv run --locked python", "uv run --locked python worker.py --a%26b"),
        ("python.exe", ["python.exe", "worker.py", "--a%26b"]),
    ],
)
def test_exec_worker_on_windows(py_executable, expected):
    context = RuntimeEnvContext(py_executable=py_executable)
    with mock.patch.dict(os.environ), mock.patch.object(
        sys, "platform", "win32"
    ), mock.patch("subprocess.Popen") as popen:
        context.exec_worker(["worker.py", "--a&b"], Language.PYTHON)

    popen.assert_called_once_with(expected, shell=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
