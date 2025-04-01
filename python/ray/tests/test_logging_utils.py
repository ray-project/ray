import os
import sys
import tempfile

import time
import pytest
import shutil
import multiprocessing


from ray._private import logging_utils


def test_redirect_with_fork():
    # In total 2 files for stdout files and 2 stderr files.
    _ROTATION_BYTES = 30
    _ROTATION_BACKUP_COUNT = 1

    # Use variable instead of `with` context because with context deletes directory at child process exit.
    temp_dir = tempfile.mkdtemp()
    stdout_path = os.path.join(temp_dir, "stdout.log")
    stderr_path = os.path.join(temp_dir, "stderr.log")

    def _redirection_subprocess(
        check_finished_event,
        write_finished_event,
    ):
        logging_utils.redirect_stdout_stderr_if_needed(
            stdout_path, stderr_path, _ROTATION_BYTES, _ROTATION_BACKUP_COUNT
        )
        print("First message to stdout.")
        print("First message to stderr.", file=sys.stderr)
        sys.stdout.write("Second message to stdout.\n")
        sys.stderr.write("Second message to stderr.\n")
        print("Third message to stdout.")
        print("Third message to stderr.", file=sys.stderr)

        # Wait until main process has checked output files, otherwise pytest also writes to stdout/stderr.
        write_finished_event.set()
        check_finished_event.wait()

    check_finished_event = (
        multiprocessing.Event()
    )  # parent process has checked over output files
    write_finished_event = (
        multiprocessing.Event()
    )  # child process has written all content
    proc = multiprocessing.Process(
        target=_redirection_subprocess,
        args=(
            check_finished_event,
            write_finished_event,
        ),
    )
    proc.start()

    write_finished_event.wait()

    # We don't have a way to synchronize and wait until redirection stream has flushed to files, so use sleep to wait.
    time.sleep(1)

    with open(stdout_path, "r") as stdout_file:
        stdout_content = stdout_file.read().strip()
        assert "Third message to stdout." == stdout_content, stdout_content
    with open(f"{stdout_path}.1", "r") as stdout_file:
        stdout_content = stdout_file.read().strip()
        assert "Second message to stdout." == stdout_content, stdout_content

    with open(stderr_path, "r") as stderr_file:
        stderr_content = stderr_file.read().strip()
        assert "Third message to stderr." == stderr_content, stderr_content
    with open(f"{stderr_path}.1", "r") as stderr_file:
        stderr_content = stderr_file.read().strip()
        assert "Second message to stderr." == stderr_content, stderr_content

    check_finished_event.set()
    proc.join()
    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
