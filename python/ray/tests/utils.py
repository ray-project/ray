from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import subprocess
import sys
import tempfile
import time

import ray


def _pid_alive(pid):
    """Check if the process with this PID is alive or not.

    Args:
        pid: The pid to check.

    Returns:
        This returns false if the process is dead. Otherwise, it returns true.
    """
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def wait_for_pid_to_exit(pid, timeout=20):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not _pid_alive(pid):
            return
        time.sleep(0.1)
    raise Exception("Timed out while waiting for process to exit.")


def run_and_get_output(command):
    with tempfile.NamedTemporaryFile() as tmp:
        p = subprocess.Popen(command, stdout=tmp, stderr=tmp)
        if p.wait() != 0:
            raise RuntimeError("ray start did not terminate properly")
        with open(tmp.name, "r") as f:
            result = f.readlines()
            return "\n".join(result)


def run_string_as_driver(driver_script):
    """Run a driver as a separate process.

    Args:
        driver_script: A string to run as a Python script.

    Returns:
        The script's output.
    """
    # Save the driver script as a file so we can call it using subprocess.
    with tempfile.NamedTemporaryFile() as f:
        f.write(driver_script.encode("ascii"))
        f.flush()
        out = ray.utils.decode(
            subprocess.check_output([sys.executable, f.name]))
    return out


def run_string_as_driver_nonblocking(driver_script):
    """Start a driver as a separate process and return immediately.

    Args:
        driver_script: A string to run as a Python script.

    Returns:
        A handle to the driver process.
    """
    # Save the driver script as a file so we can call it using subprocess. We
    # do not delete this file because if we do then it may get removed before
    # the Python process tries to run it.
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(driver_script.encode("ascii"))
        f.flush()
        return subprocess.Popen(
            [sys.executable, f.name], stdout=subprocess.PIPE)


def relevant_errors(error_type):
    return [info for info in ray.error_info() if info["type"] == error_type]


def wait_for_errors(error_type, num_errors, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(relevant_errors(error_type)) >= num_errors:
            return
        time.sleep(0.1)
    raise Exception("Timing out of wait.")


# TODO(rkn): Pytest actually has tools for capturing stdout and stderr, so we
# should use those, but they seem to conflict with Ray's use of faulthandler.
class CaptureOutputAndError(object):
    """Capture stdout and stderr of some span.

    This can be used as follows.

        captured = {}
        with CaptureOutputAndError(captured):
            # Do stuff.
        # Access captured["out"] and captured["err"].
    """

    def __init__(self, captured_output_and_error):
        if sys.version_info >= (3, 0):
            import io
            self.output_buffer = io.StringIO()
            self.error_buffer = io.StringIO()
        else:
            import cStringIO
            self.output_buffer = cStringIO.StringIO()
            self.error_buffer = cStringIO.StringIO()
        self.captured_output_and_error = captured_output_and_error

    def __enter__(self):
        sys.stdout.flush()
        sys.stderr.flush()
        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr
        sys.stdout = self.output_buffer
        sys.stderr = self.error_buffer

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr
        self.captured_output_and_error["out"] = self.output_buffer.getvalue()
        self.captured_output_and_error["err"] = self.error_buffer.getvalue()
