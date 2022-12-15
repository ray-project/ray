import os.path
import subprocess
import sys
import time
import shutil
import fcntl
import signal


# Spark on ray implementation does not directly invoke `ray start ...` script to create ray node
# subprocess, instead, it creates a subprocess to run this `ray.util.spark.start_ray_node`
# module, and in this module it invokes `ray start ...` script to start ray node,
# the purpose of `start_ray_node` module is to set up a SIGTERM handler for cleaning ray temp
# directory when ray node exits.
# When spark driver python process dies, or spark python worker dies, because they registered
# the PR_SET_PDEATHSIG signal, so OS will send a SIGTERM signal to its children processes, so
# `start_ray_node` subprocess will receive a SIGTERM signal and the SIGTERM handler will do
# cleanup work.


_WAIT_TIME_BEFORE_CLEAN_TEMP_DIR = 1


if __name__ == "__main__":
    arg_list = sys.argv[1:]

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

    def try_clean_temp_dir_at_exit():
        try:
            # Wait for a while to ensure the children processes of the ray node all exited.
            time.sleep(_WAIT_TIME_BEFORE_CLEAN_TEMP_DIR)

            # Release the shared lock, representing current ray node does not use the
            # temp dir.
            fcntl.flock(lock_fd, fcntl.LOCK_UN)

            try:
                # Start clean the temp-dir,
                # acquiring exclusive lock to ensure removing dir safely.
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                shutil.rmtree(temp_dir, ignore_errors=True)
            except BlockingIOError:
                # The file has active shared lock or exclusive lock, representing there
                # are other ray nodes running, or other node running cleanup temp-dir routine.
                # skip cleaning temp-dir.
                pass
        except Exception:
            # swallow any exception.
            pass
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    try:
        # Mutilple ray nodes might start on the same machine, and they are using the
        # same temp directory, adding a shared lock representing current ray node is
        # using the temp directory.
        fcntl.flock(lock_fd, fcntl.LOCK_SH)
        process = subprocess.Popen([ray_exec_path, "start", *arg_list], text=True)

        def sigterm_handler(*args):
            process.terminate()
            try_clean_temp_dir_at_exit()
            os._exit(143)

        signal.signal(signal.SIGTERM, sigterm_handler)
        ret_code = process.wait()
        try_clean_temp_dir_at_exit()
        sys.exit(ret_code)
    except Exception:
        try_clean_temp_dir_at_exit()
        raise
