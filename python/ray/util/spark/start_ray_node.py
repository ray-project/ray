import os.path
import subprocess
import sys
import time
import shutil
import fcntl
import signal
import socket
import logging
import threading
from ray.util.spark.cluster_init import RAY_ON_SPARK_COLLECT_LOG_TO_PATH
from ray._private.ray_process_reaper import SIGTERM_GRACE_PERIOD_SECONDS


# Spark on ray implementation does not directly invoke `ray start ...` script to create
# ray node subprocess, instead, it creates a subprocess to run this
# `ray.util.spark.start_ray_node` module, and in this module it invokes `ray start ...`
# script to start ray node, the purpose of `start_ray_node` module is to set up a
# SIGTERM handler for cleaning ray temp directory when ray node exits.
# When spark driver python process dies, or spark python worker dies, because they
# registered the PR_SET_PDEATHSIG signal, so OS will send a SIGTERM signal to its
# children processes, so `start_ray_node` subprocess will receive a SIGTERM signal and
# the SIGTERM handler will do cleanup work.


_logger = logging.getLogger(__name__)

if __name__ == "__main__":
    arg_list = sys.argv[1:]

    collect_log_to_path = os.environ[RAY_ON_SPARK_COLLECT_LOG_TO_PATH]

    temp_dir_arg_prefix = "--temp-dir="
    temp_dir = None

    for arg in arg_list:
        if arg.startswith(temp_dir_arg_prefix):
            temp_dir = arg[len(temp_dir_arg_prefix) :]

    if temp_dir is None:
        raise ValueError("Please explicitly set --temp-dir option.")

    temp_dir = os.path.normpath(temp_dir)

    ray_exec_path = os.path.join(os.path.dirname(sys.executable), "ray")

    lock_file = temp_dir + ".lock"
    lock_fd = os.open(lock_file, os.O_RDWR | os.O_CREAT | os.O_TRUNC)

    # Mutilple ray nodes might start on the same machine, and they are using the
    # same temp directory, adding a shared lock representing current ray node is
    # using the temp directory.
    fcntl.flock(lock_fd, fcntl.LOCK_SH)
    process = subprocess.Popen([ray_exec_path, "start", *arg_list], text=True)

    def try_clean_temp_dir_at_exit():
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
                        copy_log_dest_path = os.path.join(
                            collect_log_to_path,
                            os.path.basename(temp_dir) + "-logs",
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

    try:

        exit_handler_executed = False
        exit_handler_lock = threading.Lock()

        def exit_handler():
            nonlocal exit_handler_executed
            with exit_handler_lock:
                exit_handler_executed = True
                process.terminate()
                try_clean_temp_dir_at_exit()

        def sigterm_handler(*args):
            exit_handler()
            os._exit(143)


        def sighup_handler(*args):
            exit_handler()
            os._exit(129)

        # When spark application is terminated, all pyspark task worker descendant
        # processes will receive SIGHUP signal, we need to handle it properly.
        signal.signal(signal.SIGHUP, sigterm_handler)

        # When spark job is canceled, all pyspark task workers of this job will
        # receive SIGKILL signal, because we register PR_SET_PDEATHSIG signal for
        # their child processes, so this process will receive SIGTERM signal.
        signal.signal(signal.SIGTERM, sigterm_handler)
        ret_code = process.wait()
        try_clean_temp_dir_at_exit()
        sys.exit(ret_code)
    except Exception:
        try_clean_temp_dir_at_exit()
        raise
