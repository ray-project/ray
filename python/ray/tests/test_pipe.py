import subprocess
import sys

import pytest

from ray._private.pipe import Pipe


def test_parent_read_child_write():
    """Parent process reads data written by child process."""
    pipe = Pipe()
    writer_handle = pipe.make_writer_handle()

    code = f"""
from ray._private.pipe import Pipe
pipe = Pipe.from_writer_handle({writer_handle})
pipe.write("hello from child")
pipe.close()
"""
    # Use close_fds=False instead of pass_fds for cross-platform compatibility.
    # On Windows, pass_fds is not supported; close_fds=False allows inheritable
    # handles to be passed to child processes.
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        close_fds=False,
    )
    pipe.close_writer_handle()

    data = pipe.read(timeout_s=5)
    proc.wait()

    assert data == "hello from child"
    assert proc.returncode == 0
    pipe.close()


def test_sibling_processes_communicate():
    """Two sibling child processes communicate through a pipe.

    Parent creates pipe, spawns writer then reader.
    Data flow: writer child -> reader child
    """
    pipe = Pipe()

    # Create writer handle and spawn writer first
    writer_handle = pipe.make_writer_handle()
    writer_code = f"""
from ray._private.pipe import Pipe
pipe = Pipe.from_writer_handle({writer_handle})
pipe.write("hello from sibling")
pipe.close()
"""
    writer_proc = subprocess.Popen(
        [sys.executable, "-c", writer_code],
        close_fds=False,
    )
    pipe.close_writer_handle()

    # Now create reader handle and spawn reader
    reader_handle = pipe.make_reader_handle()
    reader_code = f"""
from ray._private.pipe import Pipe
pipe = Pipe.from_reader_handle({reader_handle})
data = pipe.read(timeout_s=5)
print(data, end='')
pipe.close()
"""
    reader_proc = subprocess.Popen(
        [sys.executable, "-c", reader_code],
        close_fds=False,
        stdout=subprocess.PIPE,
    )
    pipe.close_reader_handle()

    writer_proc.wait()
    stdout, _ = reader_proc.communicate(timeout=5)

    assert stdout.decode() == "hello from sibling"
    assert writer_proc.returncode == 0
    assert reader_proc.returncode == 0
    pipe.close()


def test_read_timeout():
    """Read raises RuntimeError on timeout when no data available."""
    pipe = Pipe()
    with pytest.raises(RuntimeError, match="Timed out"):
        pipe.read(timeout_s=0.1)
    pipe.close()


def test_operations_after_close():
    """Read/write on closed pipe raises RuntimeError."""
    pipe = Pipe()
    pipe.close()
    with pytest.raises(RuntimeError, match="already taken or closed"):
        pipe.read()

    pipe2 = Pipe()
    pipe2.close()
    with pytest.raises(RuntimeError, match="already taken or closed"):
        pipe2.write("data")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
