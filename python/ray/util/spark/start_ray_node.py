import fcntl
import logging
import os.path
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

from ray._private.ray_process_reaper import SIGTERM_GRACE_PERIOD_SECONDS
from ray.util.spark.cluster_init import (
    RAY_ON_SPARK_COLLECT_LOG_TO_PATH,
    RAY_ON_SPARK_START_RAY_PARENT_PID,
)

# Spark on ray implementation does not directly invoke `ray start ...` script to create
# ray node subprocess, instead, it creates a subprocess to run this
# `ray.util.spark.start_ray_node` module, and in this module it invokes `ray start ...`
# script to start ray node, the purpose of `start_ray_node` module is to set up a
# exit handler for cleaning ray temp directory when ray node exits.
# When spark driver python process dies, or spark python worker dies, because
# `start_ray_node` starts a daemon thread of `check_parent_alive`, it will detect
# parent process died event and then trigger cleanup work.


_logger = logging.getLogger(__name__)


if __name__ == "__main__":
    arg_list = sys.argv[1:]

    collect_log_to_path = os.environ[RAY_ON_SPARK_COLLECT_LOG_TO_PATH]

    temp_dir_arg_prefix = "--temp-dir="
    temp_dir = None

    for arg in arg_list:
        if arg.startswith(temp_dir_arg_prefix):
            temp_dir = arg[len(temp_dir_arg_prefix) :]

    if temp_dir is not None:
        temp_dir = os.path.normpath(temp_dir)
    else:
        # This case is for global mode Ray on spark cluster
        from ray.util.spark.cluster_init import _get_default_ray_tmp_dir

        temp_dir = _get_default_ray_tmp_dir()

    # Multiple Ray nodes might be launched in the same machine,
    # so set `exist_ok` to True
    os.makedirs(temp_dir, exist_ok=True)

    ray_cli_cmd = "ray"
    lock_file = temp_dir + ".lock"

    lock_fd = os.open(lock_file, os.O_RDWR | os.O_CREAT | os.O_TRUNC)

    # Mutilple ray nodes might start on the same machine, and they are using the
    # same temp directory, adding a shared lock representing current ray node is
    # using the temp directory.
    fcntl.flock(lock_fd, fcntl.LOCK_SH)

    process = subprocess.Popen(
        # 'ray start ...' command uses python that is set by
        # Shebang #! ..., the Shebang line is hardcoded in ray script,
        # it can't be changed to other python executable path.
        # to enforce using current python executable,
        # turn the subprocess command to
        # '`sys.executable` `which ray` start ...'
        [sys.executable, shutil.which(ray_cli_cmd), "start", *arg_list],
        text=True,
    )

    exit_handler_executed = False
    sigterm_handler_executed = False
    ON_EXIT_HANDLER_WAIT_TIME = 3

    def on_exit_handler():
        global exit_handler_executed

        if exit_handler_executed:
            # wait for exit_handler execution completed in other threads.
            time.sleep(ON_EXIT_HANDLER_WAIT_TIME)
            return

        exit_handler_executed = True

        try:
            # Wait for a while to ensure the children processes of the ray node all
            # exited.
            time.sleep(SIGTERM_GRACE_PERIOD_SECONDS + 0.5)

            if process.poll() is None:
                # "ray start ..." command process is still alive. Force to kill it.
                process.kill()

            # Release the shared lock, representing current ray node does not use the
            # temp dir.
            fcntl.flock(lock_fd, fcntl.LOCK_UN)

            try:
                # acquiring exclusive lock to ensure copy logs and removing dir safely.
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_acquired = True
            except BlockingIOError:
                # The file has active shared lock or exclusive lock, representing there
                # are other ray nodes running, or other node running cleanup temp-dir
                # routine. skip cleaning temp-dir, and skip copy logs to destination
                # directory as well.
                lock_acquired = False

            if lock_acquired:
                # This is the final terminated ray node on current spark worker,
                # start copy logs (including all local ray nodes logs) to destination.
                if collect_log_to_path:
                    try:
                        log_dir_prefix = os.path.basename(temp_dir)
                        if log_dir_prefix == "ray":
                            # global mode cluster case, append a timestamp to it to
                            # avoid name conflict with last Ray global cluster log dir.
                            log_dir_prefix = (
                                log_dir_prefix + f"-global-{int(time.time())}"
                            )
                        base_dir = os.path.join(
                            collect_log_to_path, log_dir_prefix + "-logs"
                        )
                        # Note: multiple Ray node launcher process might
                        # execute this line code, so we set exist_ok=True here.
                        os.makedirs(base_dir, exist_ok=True)
                        copy_log_dest_path = os.path.join(
                            base_dir,
                            socket.gethostname(),
                        )
                        ray_session_dir = os.readlink(
                            os.path.join(temp_dir, "session_latest")
                        )
                        shutil.copytree(
                            os.path.join(ray_session_dir, "logs"),
                            copy_log_dest_path,
                        )
                    except Exception as e:
                        _logger.warning(
                            "Collect logs to destination directory failed, "
                            f"error: {repr(e)}."
                        )

                # Start cleaning the temp-dir,
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            # swallow any exception.
            pass
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def check_parent_alive() -> None:
        orig_parent_pid = int(os.environ[RAY_ON_SPARK_START_RAY_PARENT_PID])
        while True:
            time.sleep(0.5)
            if os.getppid() != orig_parent_pid:
                # Note raising SIGTERM signal in a background thread
                # doesn't work
                sigterm_handler()
                break

    threading.Thread(target=check_parent_alive, daemon=True).start()

    try:

        def sighup_handler(*args):
            pass

        # When spark application is terminated, this process will receive
        # SIGHUP (comes from pyspark application termination).
        # Ignore the SIGHUP signal, because in this case,
        # `check_parent_alive` will capture parent process died event
        # and execute killing node and cleanup routine
        # but if we enable default SIGHUP handler, it will kill
        # the process immediately and it causes `check_parent_alive`
        # have no time to exeucte cleanup routine.
        signal.signal(signal.SIGHUP, sighup_handler)

        def sigterm_handler(*args):
            global sigterm_handler_executed
            if not sigterm_handler_executed:
                sigterm_handler_executed = True
                process.terminate()
                on_exit_handler()
            else:
                # wait for exit_handler execution completed in other threads.
                time.sleep(ON_EXIT_HANDLER_WAIT_TIME)
            # Sigterm exit code is 143.
            os._exit(143)

        signal.signal(signal.SIGTERM, sigterm_handler)
        while True:
            try:
                ret_code = process.wait()
                break
            except KeyboardInterrupt:
                # Jupyter notebook interrupt button triggers SIGINT signal and
                # `start_ray_node` (subprocess) will receive SIGINT signal and it
                # causes KeyboardInterrupt exception being raised.
                pass
        on_exit_handler()
        sys.exit(ret_code)
    except Exception:
        on_exit_handler()
        raise
