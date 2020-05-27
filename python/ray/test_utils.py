import asyncio
import errno
import json
import fnmatch
import os
import subprocess
import sys
import tempfile
import time
import socket

import ray

import psutil  # We must import psutil after ray because we bundle it with ray.

if sys.platform == "win32":
    import _winapi


class RayTestTimeoutException(Exception):
    """Exception used to identify timeouts from test utilities."""
    pass


def _pid_alive(pid):
    """Check if the process with this PID is alive or not.

    Args:
        pid: The pid to check.

    Returns:
        This returns false if the process is dead. Otherwise, it returns true.
    """
    no_such_process = errno.EINVAL if sys.platform == "win32" else errno.ESRCH
    alive = True
    try:
        if sys.platform == "win32":
            SYNCHRONIZE = 0x00100000  # access mask defined in <winnt.h>
            handle = _winapi.OpenProcess(SYNCHRONIZE, False, pid)
            try:
                alive = (_winapi.WaitForSingleObject(handle, 0) !=
                         _winapi.WAIT_OBJECT_0)
            finally:
                _winapi.CloseHandle(handle)
        else:
            os.kill(pid, 0)
    except OSError as ex:
        if ex.errno != no_such_process:
            raise
        alive = False
    return alive


def wait_for_pid_to_exit(pid, timeout=20):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not _pid_alive(pid):
            return
        time.sleep(0.1)
    raise RayTestTimeoutException(
        "Timed out while waiting for process {} to exit.".format(pid))


def wait_for_children_of_pid(pid, num_children=1, timeout=20):
    p = psutil.Process(pid)
    start_time = time.time()
    while time.time() - start_time < timeout:
        num_alive = len(p.children(recursive=False))
        if num_alive >= num_children:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException(
        "Timed out while waiting for process {} children to start "
        "({}/{} started).".format(pid, num_alive, num_children))


def wait_for_children_of_pid_to_exit(pid, timeout=20):
    children = psutil.Process(pid).children()
    if len(children) == 0:
        return

    _, alive = psutil.wait_procs(children, timeout=timeout)
    if len(alive) > 0:
        raise RayTestTimeoutException(
            "Timed out while waiting for process children to exit."
            " Children still alive: {}.".format([p.name() for p in alive]))


def kill_process_by_name(name, SIGKILL=False):
    for p in psutil.process_iter(attrs=["name"]):
        if p.info["name"] == name:
            if SIGKILL:
                p.kill()
            else:
                p.terminate()


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
            subprocess.check_output(
                [sys.executable, f.name], stderr=subprocess.STDOUT))
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


def flat_errors():
    errors = []
    for job_errors in ray.errors(all_jobs=True).values():
        errors.extend(job_errors)
    return errors


def relevant_errors(error_type):
    return [error for error in flat_errors() if error["type"] == error_type]


def wait_for_errors(error_type, num_errors, timeout=20):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if len(relevant_errors(error_type)) >= num_errors:
            return
        time.sleep(0.1)
    raise RayTestTimeoutException("Timed out waiting for {} {} errors.".format(
        num_errors, error_type))


def wait_for_condition(condition_predictor, timeout=30, retry_interval_ms=100):
    """A helper function that waits until a condition is met.

    Args:
        condition_predictor: A function that predicts the condition.
        timeout: Maximum timeout in seconds.
        retry_interval_ms: Retry interval in milliseconds.

    Return:
        Whether the condition is met within the timeout.
    """
    start = time.time()
    while time.time() - start <= timeout:
        if condition_predictor():
            return True
        time.sleep(retry_interval_ms / 1000.0)
    return False


def wait_until_succeeded_without_exception(func,
                                           exceptions,
                                           *args,
                                           timeout_ms=1000,
                                           retry_interval_ms=100):
    """A helper function that waits until a given function
        completes without exceptions.

    Args:
        func: A function to run.
        exceptions(tuple): Exceptions that are supposed to occur.
        args: arguments to pass for a given func
        timeout_ms: Maximum timeout in milliseconds.
        retry_interval_ms: Retry interval in milliseconds.

    Return:
        Whether exception occurs within a timeout.
    """
    if type(exceptions) != tuple:
        print("exceptions arguments should be given as a tuple")
        return False

    time_elapsed = 0
    start = time.time()
    while time_elapsed <= timeout_ms:
        try:
            func(*args)
            return True
        except exceptions:
            time_elapsed = (time.time() - start) * 1000
            time.sleep(retry_interval_ms / 1000.0)
    return False


def recursive_fnmatch(dirpath, pattern):
    """Looks at a file directory subtree for a filename pattern.

    Similar to glob.glob(..., recursive=True) but also supports 2.7
    """
    matches = []
    for root, dirnames, filenames in os.walk(dirpath):
        for filename in fnmatch.filter(filenames, pattern):
            matches.append(os.path.join(root, filename))
    return matches


def generate_internal_config_map(**kwargs):
    internal_config = json.dumps(kwargs)
    ray_kwargs = {
        "_internal_config": internal_config,
    }
    return ray_kwargs


@ray.remote(num_cpus=0)
class SignalActor:
    def __init__(self):
        self.ready_event = asyncio.Event()

    def send(self):
        self.ready_event.set()

    async def wait(self, should_wait=True):
        if should_wait:
            await self.ready_event.wait()


@ray.remote(num_cpus=0)
class Semaphore:
    def __init__(self, value=1):
        self._sema = asyncio.Semaphore(value=value)

    async def acquire(self):
        self._sema.acquire()

    async def release(self):
        self._sema.release()

    async def locked(self):
        return self._sema.locked()


@ray.remote
def _put(obj):
    return obj


def put_object(obj, use_ray_put):
    if use_ray_put:
        return ray.put(obj)
    else:
        return _put.remote(obj)


def wait_until_server_available(address,
                                timeout_ms=5000,
                                retry_interval_ms=100):
    ip_port = address.split(":")
    ip = ip_port[0]
    port = int(ip_port[1])
    time_elapsed = 0
    start = time.time()
    while time_elapsed <= timeout_ms:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        try:
            s.connect((ip, port))
        except Exception:
            time_elapsed = (time.time() - start) * 1000
            time.sleep(retry_interval_ms / 1000.0)
            s.close()
            continue
        s.close()
        return True
    return False


def get_other_nodes(cluster, exclude_head=False):
    """Get all nodes except the one that we're connected to."""
    return [
        node for node in cluster.list_all_nodes() if
        node._raylet_socket_name != ray.worker._global_node._raylet_socket_name
        and (exclude_head is False or node.head is False)
    ]


def get_non_head_nodes(cluster):
    """Get all non-head nodes."""
    return list(filter(lambda x: x.head is False, cluster.list_all_nodes()))
