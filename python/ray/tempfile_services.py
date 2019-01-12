import binascii
import collections
import datetime
import errno
import logging
import os
import shutil
import tempfile

import ray.utils

logger = logging.getLogger(__name__)
_incremental_dict = collections.defaultdict(lambda: 0)
_temp_root = None


def make_inc_temp(suffix="", prefix="", directory_name="/tmp/ray"):
    """Return a incremental temporary file name. The file is not created.

    Args:
        suffix (str): The suffix of the temp file.
        prefix (str): The prefix of the temp file.
        directory_name (str) : The base directory of the temp file.

    Returns:
        A string of file name. If there existing a file having the same name,
        the returned name will look like
        "{directory_name}/{prefix}.{unique_index}{suffix}"
    """
    directory_name = os.path.expanduser(directory_name)
    index = _incremental_dict[suffix, prefix, directory_name]
    # `tempfile.TMP_MAX` could be extremely large,
    # so using `range` in Python2.x should be avoided.
    while index < tempfile.TMP_MAX:
        if index == 0:
            filename = os.path.join(directory_name, prefix + suffix)
        else:
            filename = os.path.join(directory_name,
                                    prefix + "." + str(index) + suffix)
        index += 1
        if not os.path.exists(filename):
            _incremental_dict[suffix, prefix,
                              directory_name] = index  # Save the index.
            return filename

    raise FileExistsError(errno.EEXIST, "No usable temporary filename found")


def try_to_create_directory(directory_path):
    """Attempt to create a directory that is globally readable/writable.

    Args:
        directory_path: The path of the directory to create.
    """
    directory_path = os.path.expanduser(directory_path)
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise e
            logger.warning(
                "Attempted to create '{}', but the directory already "
                "exists.".format(directory_path))
        # Change the log directory permissions so others can use it. This is
        # important when multiple people are using the same machine.
    try:
        os.chmod(directory_path, 0o0777)
    except OSError as e:
        # Silently suppress the PermissionError that is thrown by the chmod.
        # This is done because the user attempting to change the permissions
        # on a directory may not own it. The chmod is attempted whether the
        # directory is new or not to avoid race conditions.
        # ray-project/ray/#3591
        if e.errno in [errno.EACCES, errno.EPERM]:
            pass
        else:
            raise


def get_temp_root():
    """Get the path of the temporary root. If not existing, it will be created.
    """
    global _temp_root

    date_str = datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")

    # Lazy creation. Avoid creating directories never used.
    if _temp_root is None:
        _temp_root = make_inc_temp(
            prefix="session_{date_str}_{pid}".format(
                pid=os.getpid(), date_str=date_str),
            directory_name="/tmp/ray")
    try_to_create_directory(_temp_root)
    return _temp_root


def set_temp_root(path):
    """Set the path of the temporary root. It will be created lazily."""
    global _temp_root
    _temp_root = path


def get_logs_dir_path():
    """Get a temp dir for logging."""
    logs_dir = os.path.join(get_temp_root(), "logs")
    try_to_create_directory(logs_dir)
    return logs_dir


def get_sockets_dir_path():
    """Get a temp dir for sockets."""
    sockets_dir = os.path.join(get_temp_root(), "sockets")
    try_to_create_directory(sockets_dir)
    return sockets_dir


def get_raylet_socket_name(suffix=""):
    """Get a socket name for raylet."""
    sockets_dir = get_sockets_dir_path()

    raylet_socket_name = make_inc_temp(
        prefix="raylet", directory_name=sockets_dir, suffix=suffix)
    return raylet_socket_name


def get_object_store_socket_name():
    """Get a socket name for plasma object store."""
    sockets_dir = get_sockets_dir_path()
    return make_inc_temp(prefix="plasma_store", directory_name=sockets_dir)


def get_ipython_notebook_path(port):
    """Get a new ipython notebook path"""

    notebook_filepath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "WebUI.ipynb")
    # We copy the notebook file so that the original doesn't get modified by
    # the user.
    notebook_name = make_inc_temp(
        suffix=".ipynb", prefix="ray_ui", directory_name=get_temp_root())
    shutil.copy(notebook_filepath, notebook_name)
    new_notebook_directory = os.path.dirname(notebook_name)
    token = ray.utils.decode(binascii.hexlify(os.urandom(24)))
    webui_url = ("http://localhost:{}/notebooks/{}?token={}".format(
        port, os.path.basename(notebook_name), token))
    return new_notebook_directory, webui_url, token


def new_log_files(name, redirect_output):
    """Generate partially randomized filenames for log files.

    Args:
        name (str): descriptive string for this log file.
        redirect_output (bool): True if files should be generated for logging
            stdout and stderr and false if stdout and stderr should not be
            redirected.

    Returns:
        If redirect_output is true, this will return a tuple of two
            filehandles. The first is for redirecting stdout and the second is
            for redirecting stderr. If redirect_output is false, this will
            return a tuple of two None objects.
    """
    if not redirect_output:
        return None, None

    # Create a directory to be used for process log files.
    logs_dir = get_logs_dir_path()
    # Create another directory that will be used by some of the RL algorithms.

    log_stdout = make_inc_temp(
        suffix=".out", prefix=name, directory_name=logs_dir)
    log_stderr = make_inc_temp(
        suffix=".err", prefix=name, directory_name=logs_dir)
    # Line-buffer the output (mode 1)
    log_stdout_file = open(log_stdout, "a", buffering=1)
    log_stderr_file = open(log_stderr, "a", buffering=1)
    return log_stdout_file, log_stderr_file


def new_redis_log_file(redirect_output, shard_number=None):
    """Create new logging files for redis"""
    if shard_number is None:
        redis_stdout_file, redis_stderr_file = new_log_files(
            "redis", redirect_output)
    else:
        redis_stdout_file, redis_stderr_file = new_log_files(
            "redis-shard_{}".format(shard_number), redirect_output)
    return redis_stdout_file, redis_stderr_file


def new_raylet_log_file(redirect_output):
    """Create new logging files for raylet."""
    raylet_stdout_file, raylet_stderr_file = new_log_files(
        "raylet", redirect_output=redirect_output)
    return raylet_stdout_file, raylet_stderr_file


def new_webui_log_file():
    """Create new logging files for web ui."""
    ui_stdout_file, ui_stderr_file = new_log_files(
        "webui", redirect_output=True)
    return ui_stdout_file, ui_stderr_file


def new_worker_redirected_log_file(worker_id):
    """Create new logging files for workers to redirect its output."""
    worker_stdout_file, worker_stderr_file = (new_log_files(
        "worker-" + ray.utils.binary_to_hex(worker_id), True))
    return worker_stdout_file, worker_stderr_file


def new_log_monitor_log_file():
    """Create new logging files for the log monitor."""
    log_monitor_stdout_file, log_monitor_stderr_file = new_log_files(
        "log_monitor", redirect_output=True)
    return log_monitor_stdout_file, log_monitor_stderr_file


def new_plasma_store_log_file(redirect_output):
    """Create new logging files for the plasma store."""
    plasma_store_stdout_file, plasma_store_stderr_file = new_log_files(
        "plasma_store", redirect_output)
    return plasma_store_stdout_file, plasma_store_stderr_file


def new_monitor_log_file(redirect_output):
    """Create new logging files for the monitor."""
    monitor_stdout_file, monitor_stderr_file = new_log_files(
        "monitor", redirect_output)
    return monitor_stdout_file, monitor_stderr_file


def new_raylet_monitor_log_file(redirect_output):
    """Create new logging files for the raylet monitor."""
    raylet_monitor_stdout_file, raylet_monitor_stderr_file = new_log_files(
        "raylet_monitor", redirect_output)
    return raylet_monitor_stdout_file, raylet_monitor_stderr_file
