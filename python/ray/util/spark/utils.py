import subprocess
import os
import sys
import random
import math
import time
import fcntl
import threading
import collections


_MEMORY_BUFFER_OFFSET = 0.8
_HEAP_TO_SHARED_RATIO = 0.4


def is_in_databricks_runtime():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


class _NoDbutilsError(Exception):
    pass


def get_dbutils():
    try:
        import IPython

        ip_shell = IPython.get_ipython()
        if ip_shell is None:
            raise _NoDbutilsError
        return ip_shell.ns_table["user_global"]["dbutils"]
    except ImportError:
        raise _NoDbutilsError
    except KeyError:
        raise _NoDbutilsError


def gen_cmd_exec_failure_msg(cmd, return_code, tail_output_deque):
    cmd_str = " ".join(cmd)
    tail_output = "".join(tail_output_deque)
    return (
        f"Command {cmd_str} failed with return code {return_code}, tail output are included "
        f"below.\n{tail_output}\n"
    )


def exec_cmd(
    cmd,
    *,
    extra_env=None,
    synchronous=True,
    **kwargs,
):
    """
    A convenience wrapper of `subprocess.Popen` for running a command from a Python script.
    If `synchronous` is True, wait until the process terminated and if subprocess return code
    is not 0, raise error containing last 100 lines output.
    If `synchronous` is False, return an `Popen` instance and a deque instance holding tail
    outputs.
    The subprocess stdout / stderr output will be streamly redirected to current process stdout.
    """
    illegal_kwargs = set(kwargs.keys()).intersection({"text", "stdout", "stderr"})
    if illegal_kwargs:
        raise ValueError(f"`kwargs` cannot contain {list(illegal_kwargs)}")

    env = kwargs.pop("env", None)
    if extra_env is not None and env is not None:
        raise ValueError("`extra_env` and `env` cannot be used at the same time")

    env = env if extra_env is None else {**os.environ, **extra_env}

    process = subprocess.Popen(
        cmd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        **kwargs
    )

    tail_output_deque = collections.deque(maxlen=100)

    def redirect_log_thread_fn():
        for line in process.stdout:
            # collect tail logs by `tail_output_deque`
            tail_output_deque.append(line)

            # redirect to stdout.
            sys.stdout.write(line)

    threading.Thread(target=redirect_log_thread_fn, args=()).start()

    if not synchronous:
        return process, tail_output_deque

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(gen_cmd_exec_failure_msg(cmd, return_code, tail_output_deque))


def check_port_open(host, port):
    import socket
    from contextlib import closing
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0


def get_random_unused_port(host, min_port=1024, max_port=65535, max_retries=100, exclude_list=None):
    """
    Get random unused port.
    """
    # Use true random generator
    rng = random.SystemRandom()

    exclude_list = exclude_list or []
    for _ in range(max_retries):
        port = rng.randint(min_port, max_port)
        if port in exclude_list:
            continue
        if not check_port_open(host, port):
            return port
    raise RuntimeError(f"Get available port between range {min_port} and {max_port} failed.")


def get_spark_session():
    from pyspark.sql import SparkSession

    spark_session = SparkSession.getActiveSession()
    if spark_session is None:
        raise RuntimeError(
            "Spark session haven't been initiated yet. Please use `SparkSession.builder` to "
            "create a spark session and connect to a spark cluster."
        )
    return spark_session


def get_spark_application_driver_host(spark):
    return spark.conf.get("spark.driver.host")


def get_max_num_concurrent_tasks(spark_context):
    """Gets the current max number of concurrent tasks."""
    # pylint: disable=protected-access
    # spark version 3.1 and above have a different API for fetching max concurrent tasks
    if spark_context._jsc.sc().version() >= "3.1":
        return spark_context._jsc.sc().maxNumConcurrentTasks(
            spark_context._jsc.sc().resourceProfileManager().resourceProfileFromId(0)
        )
    return spark_context._jsc.sc().maxNumConcurrentTasks()


def _get_total_physical_memory():
    import psutil

    return psutil.virtual_memory().total


def _get_total_shared_memory():
    import shutil

    return shutil.disk_usage("/dev/shm").total


def _get_cpu_cores():
    import multiprocessing

    return multiprocessing.cpu_count()


def _calc_mem_per_ray_worker_node(
    num_task_slots, physical_mem_bytes, shared_mem_bytes, heap_to_object_store_ratio
):
    available_physical_mem_per_node = int(
        physical_mem_bytes / num_task_slots * _MEMORY_BUFFER_OFFSET
    )
    available_shared_mem_per_node = int(
        shared_mem_bytes / num_task_slots * _MEMORY_BUFFER_OFFSET
    )

    object_store_bytes = int(
        min(
            available_physical_mem_per_node * heap_to_object_store_ratio,
            available_shared_mem_per_node,
        )
    )
    heap_mem_bytes = (
        available_physical_mem_per_node - object_store_bytes
    )
    return heap_mem_bytes, object_store_bytes


def _resolve_target_spark_tasks(calculated_limits):
    """
    Return the max value of a list of spark task total count calculations based on the
    provided configuration arguments to `init_ray_cluster`.
    Args:
        calculated_limits: A list of calculated values wherein the highest value based on
            spark cluster worker instance sizes and user-specified ray cluster configuration is
            taken to ensure that spark cluster limits are not exceeded.

    Returns: The maximum calculated number of spark tasks for the configured ray cluster.

    """
    return max(*calculated_limits)


def get_target_spark_tasks(
    max_concurrent_tasks,
    num_spark_task_cpus,
    num_spark_task_gpus,
    ray_worker_node_heap_memory_bytes,
    ray_worker_node_object_store_memory_bytes,
    num_spark_tasks,
    total_cpus,
    total_gpus,
    total_heap_memory_bytes,
    total_object_store_memory_bytes,
):

    if num_spark_tasks is not None:
        if num_spark_tasks == -1:
            # num_worker_nodes=-1 represents using all available spark task slots
            num_spark_tasks = max_concurrent_tasks
        elif num_spark_tasks <= 0:
            raise ValueError(
                "The value of 'num_worker_nodes' argument must be either a positive integer or -1."
            )
    else:
        calculated_tasks = [1]
        if total_cpus is not None:
            if total_cpus <= 0:
                raise ValueError(
                    "The value of 'total_cpus' argument must be a positive integer."
                )

            calculated_tasks.append(int(math.ceil(total_cpus / num_spark_task_cpus)))

        if total_gpus is not None:
            if total_gpus <= 0:
                raise ValueError(
                    "The value of 'total_gpus' argument must be a positive integer."
                )

            calculated_tasks.append(int(math.ceil(total_gpus / num_spark_task_gpus)))

        if total_heap_memory_bytes is not None:
            if total_heap_memory_bytes <= 0:
                raise ValueError(
                    "The value of 'total_heap_memory_bytes' argument must be a positive integer."
                )

            calculated_tasks.append(
                int(math.ceil(total_heap_memory_bytes / ray_worker_node_heap_memory_bytes))
            )

        if total_object_store_memory_bytes is not None:
            if total_object_store_memory_bytes <= 0:
                raise ValueError(
                    "The value of 'total_object_store_memory_bytes' argument must be a "
                    "positive integer."
                )

            calculated_tasks.append(
                int(
                    math.ceil(
                        total_object_store_memory_bytes
                        / ray_worker_node_object_store_memory_bytes
                    )
                )
            )

        num_spark_tasks = _resolve_target_spark_tasks(calculated_tasks)
    return num_spark_tasks


def get_avail_mem_per_ray_worker_node(spark, heap_to_object_store_ratio):
    """
    Return the available heap memory and object store memory for each ray worker.
    NB: We have one ray node per spark task.
    """
    num_cpus_per_spark_task = int(
        spark.sparkContext.getConf().get("spark.task.cpus", "1")
    )

    def mapper(_):
        try:
            num_cpus = _get_cpu_cores()
            num_task_slots = num_cpus // num_cpus_per_spark_task

            physical_mem_bytes = _get_total_physical_memory()
            shared_mem_bytes = _get_total_shared_memory()

            (
                ray_worker_node_heap_mem_bytes,
                ray_worker_node_object_store_bytes,
            ) = _calc_mem_per_ray_worker_node(
                num_task_slots,
                physical_mem_bytes,
                shared_mem_bytes,
                heap_to_object_store_ratio,
            )
            return ray_worker_node_heap_mem_bytes, ray_worker_node_object_store_bytes, None
        except Exception as e:
            return -1, -1, repr(e)

    # Running memory inference routine on spark executor side since the spark worker nodes may
    # have a different machine configuration compared to the spark driver node.
    inferred_ray_worker_node_heap_mem_bytes, inferred_ray_worker_node_object_store_bytes, err = (
        spark.sparkContext.parallelize([1], 1).map(mapper).collect()[0]
    )

    if err is not None:
        raise RuntimeError(
            f"Inferring ray worker available memory failed, error: {err}"
        )
    return inferred_ray_worker_node_heap_mem_bytes, inferred_ray_worker_node_object_store_bytes


def get_spark_task_assigned_physical_gpus(gpu_addr_list):
    if "CUDA_VISIBLE_DEVICES" in os.environ:
        visible_cuda_dev_list = [
            int(dev.strip()) for dev in os.environ["CUDA_VISIBLE_DEVICES"].split(",")
        ]
        return [visible_cuda_dev_list[addr] for addr in gpu_addr_list]
    else:
        return gpu_addr_list


def _allocate_port_range_and_start_lock_barrier_thread_for_ray_worker_node_startup():
    """
    If we start multiple ray workers on a machine concurrently, some ray worker processes
    might fail due to ray port conflicts, this is because race condition on getting free
    port and opening the free port.
    To address the issue, this function use an exclusive file lock to delay the worker processes
    to ensure that port acquisition does not create a resource contention issue due to a race
    condition.

    After acquiring lock, it will allocate port range for worker ports
    (for ray node config --min-worker-port and --max-worker-port).
    Because on a spark cluster, multiple ray cluster might be created, so on one spark worker
    machine, there might be multiple ray worker nodes running, these worker nodes might belong
    to different ray cluster, and we must ensure these ray nodes on the same machine using
    non-overlapping worker port range, to achieve this, in this function, it creates a file
    `/tmp/ray_on_spark_worker_port_allocation.txt` file, the file format is composed of multiple
    lines, each line contains 2 number: `pid` and `port_range_slot_index`,
    each port range slot allocates 1000 ports, and corresponding port range is:
    range_begin (inclusive): 20000 + port_range_slot_index * 1000
    range_end (exclusive): range_begin + 1000
    In this function, it first scans `/tmp/ray_on_spark_worker_port_allocation.txt` file,
    removing lines that containing dead process pid, then find the first unused
    port_range_slot_index, then regenerate this file, and return the allocated port range.

    Returns: Allocated port range for current worker ports
    """
    import psutil

    def acquire_lock(file_path):
        mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
        try:
            fd = os.open(file_path, mode)
            # The lock file must be readable / writable to all users.
            os.chmod(file_path, 0o0777)
            # Allow for retrying getting a file lock a maximum number of seconds
            max_lock_iter = 600
            for _ in range(max_lock_iter):
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    # Lock is used by other processes, continue loop to wait for lock available
                    pass
                else:
                    # Acquire lock successfully.
                    return fd
                time.sleep(10)
            raise TimeoutError(f"Acquiring lock on file {file_path} timeout.")
        except Exception:
            os.close(fd)

    lock_file_path = "/tmp/ray_on_spark_worker_startup_barrier_lock.lock"
    try:
        lock_fd = acquire_lock(lock_file_path)
    except TimeoutError:
        # If timeout happens, the file lock might be hold by another process and that process
        # does not release the lock in time by some unexpected reason.
        # In this case, remove the existing lock file and create the file again, and then
        # acquire file lock on the new file.
        try:
            os.remove(lock_file_path)
        except Exception:
            pass
        lock_fd = acquire_lock(lock_file_path)

    def release_lock():
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)

    try:
        port_alloc_file = "/tmp/ray_on_spark_worker_port_allocation.txt"

        # NB: reading / writing `port_alloc_file` is protected by exclusive lock
        # on file `lock_file_path`
        if os.path.exists(port_alloc_file):
            with open(port_alloc_file, mode="r") as fp:
                port_alloc_data = fp.read()
            port_alloc_table = [line.split(" ") for line in port_alloc_data.strip().split("\n")]
            port_alloc_table = [
                (int(pid_str), int(slot_index_str))
                for pid_str, slot_index_str in port_alloc_table
            ]
        else:
            port_alloc_table = []
            with open(port_alloc_file, mode="w"):
                pass
            # The port range allocation file must be readable / writable to all users.
            os.chmod(port_alloc_file, 0o0777)

        port_alloc_map = {
            pid: slot_index
            for pid, slot_index in port_alloc_table
            if psutil.pid_exists(pid)  # remove slot used by dead process
        }

        allocated_slot_set = set(port_alloc_map.values())

        if len(allocated_slot_set) == 0:
            new_slot_index = 0
        else:
            new_slot_index = max(allocated_slot_set) + 1
            for index in range(new_slot_index):
                if index not in allocated_slot_set:
                    new_slot_index = index
                    break

        port_alloc_map[os.getpid()] = new_slot_index

        with open(port_alloc_file, mode="w") as fp:
            for pid, slot_index in port_alloc_map.items():
                fp.write(f"{pid} {slot_index}\n")

        worker_port_range_begin = 20000 + new_slot_index * 1000
        worker_port_range_end = worker_port_range_begin + 1000

        if worker_port_range_end > 65536:
            raise RuntimeError(
                "Too many ray worker nodes are running on this machine, cannot "
                "allocate worker port range for new ray worker node."
            )
    except Exception:
        release_lock()
        raise

    def hold_lock_for_10s_and_release():
        time.sleep(10)
        release_lock()

    threading.Thread(target=hold_lock_for_10s_and_release, args=()).start()

    return worker_port_range_begin, worker_port_range_end


def _display_databricks_driver_proxy_url(spark_context, port, title):
    from dbruntime.display import displayHTML
    driverLocal = spark_context._jvm.com.databricks.backend.daemon.driver.DriverLocal
    commandContextTags = driverLocal.commandContext().get().toStringMap().apply("tags")
    orgId = commandContextTags.apply("orgId")
    clusterId = commandContextTags.apply("clusterId")

    template = "/driver-proxy/o/{orgId}/{clusterId}/{port}/"
    proxy_url = template.format(orgId=orgId, clusterId=clusterId, port=port)

    displayHTML(f"""
      <div style="margin-bottom: 16px">
          <a href="{proxy_url}">
              Open {title} in a new tab
          </a>
      </div>
    """)
