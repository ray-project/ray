import subprocess
import sys

import pytest

from ray._private.runtime_env.context import build_windows_worker_command


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
