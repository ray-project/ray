import os.path
import subprocess
import sys
import time
import shutil
import fcntl
import signal

from .cluster_init import _RAY_HEAD_NODE_TAG_FILE


_WAIT_TIME_BEFORE_CLEAN_TEMP_DIR = 1


if __name__ == '__main__':
    arg_list = sys.argv[1:]

    temp_dir_arg_prefix = "--temp-dir="
    temp_dir = None

    for arg in arg_list:
        if arg.startswith(temp_dir_arg_prefix):
            temp_dir = arg[len(temp_dir_arg_prefix):]

    if temp_dir is None:
        raise ValueError("Please explicitly set --temp-dir option.")

    is_head_node = True if "--head" in arg_list else False

    temp_dir = os.path.normpath(temp_dir)
    head_node_is_on_local = os.path.exists(os.path.join(temp_dir, _RAY_HEAD_NODE_TAG_FILE))

    ray_exec_path = os.path.join(os.path.dirname(sys.executable), "ray")

    process = subprocess.Popen([ray_exec_path, "start", *arg_list], text=True)

    def sigterm_handler(*args):
        process.terminate()

        try:
            # if a worker node and head node runs on the same machine,
            # when the worker node exits, we cannot delete the temp dir immediately
            # because head node is still using it.
            if is_head_node or not head_node_is_on_local:
                time.sleep(_WAIT_TIME_BEFORE_CLEAN_TEMP_DIR)
                lock_file = temp_dir + ".lock"
                try:
                    mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
                    lock_fd = os.open(lock_file, mode)
                    # because one spark job might start multiple ray worker node on one spark worker
                    # machine, and they use the same temp dir, so acquire an exclusive file lock when
                    # deleting the temp dir.
                    fcntl.flock(lock_fd, fcntl.LOCK_EX)

                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)
                finally:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    os.close(lock_fd)
        finally:
            os._exit(143)

    signal.signal(signal.SIGTERM, sigterm_handler)

    process.wait()
