import os
import os.path
import sys
import time
import shutil
import fcntl
import signal
import socket
import logging
import threading
import psutil
import importlib
import errno

from ray.util.spark.cluster_init import (
    RAY_ON_SPARK_LOGGER_LEVEL,
    RAY_ON_SPARK_COLLECT_LOG_TO_PATH,
    RAY_ON_SPARK_START_RAY_PARENT_PID,
)
from ray._private.ray_process_reaper import SIGTERM_GRACE_PERIOD_SECONDS
from ray._private import ray_constants

# Spark on ray implementation does not directly invoke `ray start ...` script to create
# ray node subprocess, instead, it creates a child process to run this
# `ray.util.spark.start_ray_node` module, and in this module it invokes `ray start ...`
# script to start ray node, the purpose of `start_ray_node` module is to set up a
# exit handler for cleaning ray temp directory when ray node exits.
# When spark driver python process dies, or spark python worker dies, because
# `start_ray_node` starts a daemon thread of `check_parent_alive`, it will detect
# parent process died event and then trigger cleanup work.

_logger = logging.getLogger("ray.util.spark")
_logger.setLevel(ray_constants.LOGGER_LEVEL.upper())

def is_fd_closed(lock_fd):
    # checks for closed fds
    try:
        fd_status = fcntl.fcntl(lock_fd, fcntl.F_GETFD)
        if fd_status == 1:
            return False
        else:
            return True
    except OSError as e:
        # Bad file descriptor error
        if e.errno == 9: 
            return True
        else:
            # Re-raise the exception if it's not a bad file descriptor error
            raise

def list_fds():
    # List process open FDs and their targets
    if not sys.platform.startswith('linux'):
        raise NotImplementedError('Unsupported platform: %s' % sys.platform)
    ret = {}
    base = '/proc/self/fd'
    for num in os.listdir(base):
        path = None
        try:
            path = os.readlink(os.path.join(base, num))
        except OSError as err:
            # Last FD is always the "listdir" one (which may be closed)
            if err.errno != errno.ENOENT:
                raise
        ret[int(num)] = path
    return ret

def try_clean_temp_dir_at_exit(process, lock_fd, collect_log_to_path, temp_dir):
    _logger.debug("try_clean_temp_dir_at_exit -- initialized")
    try:
        # Wait for a while to ensure the children processes of the ray node all
        # exited.
        time.sleep(SIGTERM_GRACE_PERIOD_SECONDS + 0.5)
        _logger.debug("try_clean_temp_dir_at_exit -- "
                      f"finished sleeping for: {SIGTERM_GRACE_PERIOD_SECONDS + 0.5}")
        try: 
            status = process.status()
            _logger.debug("try_clean_temp_dir_at_exit -- "
                          f"ray scripts.scripts.main() status: {status}")
        except Exception:
            status = "killed"
        if (status in ['running', 'sleeping']):
            _logger.debug("try_clean_temp_dir_at_exit -- "
                          "process sleeping or running, killing now")
            # "ray start ..." command process is still alive. Force to kill it.
            try:
                process.kill()
            except psutil.NoSuchProcess:
                _logger.debug("try_clean_temp_dir_at_exit -- "
                              "no process found, already terminated")
                pass
            except Exception as e:
                raise Exception(e)

        # Release the shared lock, representing current ray node does not use the
        # temp dir.
        _logger.debug("try_clean_temp_dir_at_exit -- "
                      "releasing ray node temp dir lock")
        fcntl.flock(lock_fd, fcntl.LOCK_UN)

        try:
            _logger.debug("try_clean_temp_dir_at_exit -- "
                          "acquiring lock for log copy to final destination")
            # acquiring exclusive lock to ensure copy logs and removing dir safely.
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_acquired = True
        except BlockingIOError:
            # The file has active shared lock or exclusive lock, representing there
            # are other ray nodes running, or other node running cleanup temp-dir
            # routine. skip cleaning temp-dir, and skip copy logs to destination
            # directory as well.
            lock_acquired = False
        _logger.debug("try_clean_temp_dir_at_exit -- "
                      f"lock acquired: {lock_acquired}")

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
                        "try_clean_temp_dir_at_exit -- "
                        "Collect logs to destination directory failed, "
                        f"error: {repr(e)}."
                    )

            # Start cleaning the temp-dir,
            _logger.debug("try_clean_temp_dir_at_exit -- cleaning the temp dir")
            shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception as e:
        _logger.debug("try_clean_temp_dir_at_exit -- swallowed non-critical, "
                      f"error: {repr(e)}")
        # swallow any exception.
        pass
    finally:
        if is_fd_closed(lock_fd):
            _logger.debug("try_clean_temp_dir_at_exit -- "
                          "lock is already closed, skipping release")
        else:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            _logger.debug("try_clean_temp_dir_at_exit -- "
                          "lock released, closing file descriptor")
            os.close(lock_fd)
        _logger.debug("try_clean_temp_dir_at_exit -- confirming lock closed")
        fd_status_is_closed = is_fd_closed(lock_fd)
        if fd_status_is_closed is not True:
            raise Exception("try_clean_temp_dir_at_exit -- "
                            "failed ray temp dir lock release "
                            "possible incompatible underlying filesystem, "
                            "try changing temp dir location or underlying fs")
        _logger.debug("try_clean_temp_dir_at_exit -- "
                      "cleaning up any remaining start_ray_node child fd(s)")
        for fd in list(list_fds()):
            if is_fd_closed(fd):
                _logger.debug("try_clean_temp_dir_at_exit -- "
                              f"skipping fd: {fd}, already closed")
            else:
                _logger.debug("try_clean_temp_dir_at_exit -- "
                              f"closing fd: {fd}")
                os.close(fd)
                
def check_parent_alive(process, lock_fd, collect_log_to_path, temp_dir) -> None:
    orig_parent_pid = int(os.environ[RAY_ON_SPARK_START_RAY_PARENT_PID])
    while True:
        time.sleep(0.5)
        # checks the parent process that launched the job alive, if not terminate
        if os.getppid() != orig_parent_pid:
            process.terminate()
            try_clean_temp_dir_at_exit(process, 
                                       lock_fd, 
                                       collect_log_to_path, 
                                       temp_dir)
            process.wait()
            # Keep the same exit code 143 with sigterm signal.
            os._exit(143)

def main():
    arg_list = sys.argv[1:]

    collect_log_to_path = os.environ[RAY_ON_SPARK_COLLECT_LOG_TO_PATH]
    ray_constants.LOGGER_LEVEL = os.environ[RAY_ON_SPARK_LOGGER_LEVEL]
    _logger.setLevel(ray_constants.LOGGER_LEVEL.upper())

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
    lock_file = temp_dir + ".lock"
    
    cpid = os.fork()
    if not cpid:
        os.setsid()
        _logger.debug("initializing setup_ray_node child to run ray.scripts.scripts")
        _logger.debug(f"parent pid: {os.getpid()}, pparent pid: {os.getppid()}")
        _logger.debug(f"proc fds in child at launch {str(list_fds())}")
        while True:
            if os.path.exists(lock_file):
                _logger.debug("lock file detected, continuing ray launch")
                break
            else:
                _logger.debug("lock file does not exist, "
                              "waiting for parent process creation...")
                time.sleep(1)  # Wait for 1 second before checking again
            
        sys.argv = [sys.argv[0], "start", *arg_list]
        scripts_module = importlib.import_module("ray.scripts.scripts")
        scripts_module.main()
        sys.exit(0)
    else:
        process = psutil.Process(cpid)
        ray_start_parent_pid = os.getpid()
        _logger.debug(f"initializing start_ray_node from {ray_start_parent_pid}")
        _logger.debug(f"lock file used in ray_start_node: {lock_file}")
        lock_fd = os.open(lock_file, os.O_RDWR | os.O_CREAT | os.O_TRUNC)
        # Mutilple ray nodes might start on the same machine, and they are using the
        # same temp directory, adding a shared lock representing current ray node is
        # using the temp directory.
        fcntl.flock(lock_fd, fcntl.LOCK_SH)
        _logger.debug("initializing thread for parent alive check")
        _logger.debug("RAY_ON_SPARK_START_RAY_PARENT_PID: "
                      f"{os.getenv('RAY_ON_SPARK_START_RAY_PARENT_PID')}, "
                      f"parent pid: {ray_start_parent_pid}, "
                      f"pparent pid: {os.getppid()}")
        check_parent_thread = threading.Thread(target=check_parent_alive, 
                                               args=(process, 
                                                     lock_fd, 
                                                     collect_log_to_path, 
                                                     temp_dir), 
                                               daemon=True)
        check_parent_thread.name = f"ray-checkp-{check_parent_thread.name}"
        check_parent_thread.start()

        try:
            def sighup_handler(*args):
                _logger.debug("sighup handler triggered for start_ray_node")
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
                _logger.debug("sigterm handler triggered for start_ray_node")
                _logger.debug(f"proc fds in parent at exit {str(list_fds())}")
                try:
                    _logger.debug("terminating process")
                    process.terminate()
                except psutil.NoSuchProcess:
                    _logger.debug("process already dead or killed")
                except Exception as e:
                    raise Exception(e)
                try:
                    try_clean_temp_dir_at_exit(process, 
                                               lock_fd, 
                                               collect_log_to_path, 
                                               temp_dir)
                except Exception:
                    pass
                finally:
                    _logger.debug("finished try clean temp dir at exit, "
                                  "triggering exit now")
                    process.wait()
                    # Sigterm exit code is 143.
                    os._exit(143)

            signal.signal(signal.SIGTERM, sigterm_handler)
            _logger.debug("waiting for start_ray_node to finish")
            ret_code = process.wait()
            try_clean_temp_dir_at_exit(process, 
                                       lock_fd, 
                                       collect_log_to_path, 
                                       temp_dir)
            _logger.debug("exiting")
            sys.exit(ret_code)
        except Exception as e:
            try_clean_temp_dir_at_exit(process, 
                                       lock_fd, 
                                       collect_log_to_path, 
                                       temp_dir)
            raise Exception(e)
            
if __name__ == "__main__":
    main()
