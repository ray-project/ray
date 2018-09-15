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

RAY_TEMPDIR = 'RAY_TEMPDIR'
RAY_OBJECT_STORE_SOCKET_NAME = 'RAY_OBJECT_STORE_SOCKET_NAME'

_incremental_dict = collections.defaultdict(lambda: 0)


def make_inc_temp(suffix="", prefix="", dir=None):
    """Return a incremental temporary file name. The file is not created."""

    if dir is None:
        dir = tempfile.gettempdir()

    index = _incremental_dict[suffix, prefix, dir]
    for seq in range(tempfile.TMP_MAX):
        index += 1
        file = os.path.join(dir, prefix + str(index) + suffix)
        if not os.path.exists(file):
            _incremental_dict[suffix, prefix, dir] = index  # Save the index.
            return file

    raise FileExistsError(errno.EEXIST, "No usable temporary filename found")


def try_to_create_directory(directory_path):
    """Attempt to create a directory that is globally readable/writable.

    Args:
        directory_path: The path of the directory to create.
    """
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
        os.chmod(directory_path, 0o0777)


if RAY_TEMPDIR in os.environ:
    temp_root = os.environ[RAY_TEMPDIR]
    try_to_create_directory(temp_root)
else:
    # Change the default root by providing the directory named by the
    # TMPDIR environment variable.
    temp_root = tempfile.mkdtemp(prefix='ray_{pid}_'.format(pid=os.getpid()))


def get_logs_dir_path():
    """Get a temp dir for logging."""
    logs_dir = os.path.join(temp_root, 'logs')
    try_to_create_directory(logs_dir)
    return logs_dir


def get_rl_dir_path():
    """Get a temp dir that will be used by some of the RL algorithms."""
    rl_dir = os.path.join(temp_root, 'ray')
    try_to_create_directory(rl_dir)
    return rl_dir


def get_sockets_dir_path():
    """Get a temp dir for sockets"""
    sockets_dir = os.path.join(temp_root, 'sockets')
    try_to_create_directory(sockets_dir)
    return sockets_dir


def get_raylet_socket_name(suffix=None):
    """Get a socket name for raylet.

    This function could be unsafe. The socket name may
    refer to a file that did not exist at some point, but by the time
    you get around to creating it, someone else may have beaten you to
    the punch.
    """
    sockets_dir = get_sockets_dir_path()

    if suffix is None:
        raylet_socket_name = make_inc_temp(prefix='raylet_', dir=sockets_dir)
    else:
        raylet_socket_name = os.path.join(sockets_dir,
                                          'raylet_{}'.format(suffix))
    return raylet_socket_name


def get_object_store_socket_name():
    sockets_dir = get_sockets_dir_path()
    if RAY_OBJECT_STORE_SOCKET_NAME in os.environ:
        return os.path.join(sockets_dir,
                            os.environ[RAY_OBJECT_STORE_SOCKET_NAME])
    else:
        return make_inc_temp(prefix='plasma_store_', dir=sockets_dir)


def get_plasma_manager_socket_name():
    sockets_dir = get_sockets_dir_path()
    return make_inc_temp(prefix='plasma_manager_', dir=sockets_dir)


def get_local_scheduler_socket_name(suffix=None):
    """Get a socket name for local scheduler.

    This function could be unsafe. The socket name may
    refer to a file that did not exist at some point, but by the time
    you get around to creating it, someone else may have beaten you to
    the punch.
    """
    sockets_dir = get_sockets_dir_path()

    if suffix is None:
        raylet_socket_name = make_inc_temp(prefix='scheduler_',
                                           dir=sockets_dir)
    else:
        raylet_socket_name = os.path.join(sockets_dir,
                                          'scheduler_{}'.format(suffix))
    return raylet_socket_name


def get_random_ipython_notebook_path(port):
    """Get a new random ipython notebook path"""

    notebook_filepath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "WebUI.ipynb")
    # We copy the notebook file so that the original doesn't get modified by
    # the user.
    notebook_name = make_inc_temp(suffix='.ipynb', prefix='ray_ui_',
                                  dir=temp_root)
    new_notebook_filepath = os.path.join(get_logs_dir_path(), notebook_name)
    shutil.copy(notebook_filepath, new_notebook_filepath)
    new_notebook_directory = os.path.dirname(new_notebook_filepath)
    token = ray.utils.decode(binascii.hexlify(os.urandom(24)))
    webui_url = ("http://localhost:{}/notebooks/{}?token={}"
                 .format(port, notebook_name, token))
    return new_notebook_directory, webui_url, token


def get_random_temp_redis_config_path():
    redis_config_name = make_inc_temp(prefix='redis_conf_', dir=temp_root)
    return redis_config_name


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
    # TODO: Move it into where we need such a directory.
    get_rl_dir_path()

    date_str = datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    prefix = '{}-{}-'.format(name, date_str)

    log_stdout = make_inc_temp(suffix='.out', prefix=prefix, dir=logs_dir)
    log_stderr = make_inc_temp(suffix='.err', prefix=prefix, dir=logs_dir)
    # Line-buffer the output (mode 1)
    log_stdout_file = open(log_stdout, "a", buffering=1)
    log_stderr_file = open(log_stderr, "a", buffering=1)
    return log_stdout_file, log_stderr_file


def new_redis_log_file(redirect_output, shard_number=None):
    if shard_number is None:
        redis_stdout_file, redis_stderr_file = new_log_files(
            "redis", redirect_output)
    else:
        redis_stdout_file, redis_stderr_file = new_log_files(
            "redis-{}".format(shard_number), redirect_output)
    return redis_stdout_file, redis_stderr_file


def new_raylet_log_file(local_scheduler_index, redirect_output):
    raylet_stdout_file, raylet_stderr_file = new_log_files(
        "raylet_{}".format(local_scheduler_index),
        redirect_output=redirect_output)
    return raylet_stdout_file, raylet_stderr_file


def new_local_scheduler_log_file(local_scheduler_index, redirect_output):
    local_scheduler_stdout_file, local_scheduler_stderr_file = (
        new_log_files(
            "local_scheduler_{}".format(local_scheduler_index),
            redirect_output=redirect_output))
    return local_scheduler_stdout_file, local_scheduler_stderr_file


def new_webui_log_file():
    ui_stdout_file, ui_stderr_file = new_log_files("webui",
                                                   redirect_output=True)
    return ui_stdout_file, ui_stderr_file


def new_worker_log_file(local_scheduler_index, worker_index, redirect_output):
    worker_stdout_file, worker_stderr_file = new_log_files(
        "worker_{}_{}".format(local_scheduler_index, worker_index),
        redirect_output)
    return worker_stdout_file, worker_stderr_file


def new_log_monitor_log_file():
    log_monitor_stdout_file, log_monitor_stderr_file = new_log_files(
        "log_monitor", redirect_output=True)
    return log_monitor_stdout_file, log_monitor_stderr_file


def new_global_scheduler_log_file(redirect_output):
    global_scheduler_stdout_file, global_scheduler_stderr_file = (
        new_log_files("global_scheduler", redirect_output))
    return global_scheduler_stdout_file, global_scheduler_stderr_file


def new_plasma_store_log_file(local_scheduler_index, redirect_output):
    plasma_store_stdout_file, plasma_store_stderr_file = new_log_files(
        "plasma_store_{}".format(local_scheduler_index), redirect_output)
    return plasma_store_stdout_file, plasma_store_stderr_file


def new_plasma_manager_log_file(local_scheduler_index, redirect_output):
    plasma_manager_stdout_file, plasma_manager_stderr_file = new_log_files(
        "plasma_manager_{}".format(local_scheduler_index), redirect_output)
    return plasma_manager_stdout_file, plasma_manager_stderr_file


def new_monitor_log_file(redirect_output):
    monitor_stdout_file, monitor_stderr_file = new_log_files(
        "monitor", redirect_output)
    return monitor_stdout_file, monitor_stderr_file
