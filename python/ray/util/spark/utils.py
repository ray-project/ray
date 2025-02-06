import subprocess
import os
import sys
import random
import threading
import collections
import logging
import shutil
import time


_logger = logging.getLogger("ray.util.spark.utils")


def is_in_databricks_runtime():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def gen_cmd_exec_failure_msg(cmd, return_code, tail_output_deque):
    cmd_str = " ".join(cmd)
    tail_output = "".join(tail_output_deque)
    return (
        f"Command {cmd_str} failed with return code {return_code}, tail output are "
        f"included below.\n{tail_output}\n"
    )


def get_configured_spark_executor_memory_bytes(spark):
    value_str = spark.conf.get("spark.executor.memory", "1g").lower()
    value_num = int(value_str[:-1])
    value_unit = value_str[-1]
    unit_map = {
        "k": 1024,
        "m": 1024 * 1024,
        "g": 1024 * 1024 * 1024,
        "t": 1024 * 1024 * 1024 * 1024,
    }
    return value_num * unit_map[value_unit]


def exec_cmd(
    cmd,
    *,
    extra_env=None,
    synchronous=True,
    **kwargs,
):
    """
    A convenience wrapper of `subprocess.Popen` for running a command from a Python
    script.
    If `synchronous` is True, wait until the process terminated and if subprocess
    return code is not 0, raise error containing last 100 lines output.
    If `synchronous` is False, return an `Popen` instance and a deque instance holding
    tail outputs.
    The subprocess stdout / stderr output will be streamly redirected to current
    process stdout.
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
        **kwargs,
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
        raise RuntimeError(
            gen_cmd_exec_failure_msg(cmd, return_code, tail_output_deque)
        )


def is_port_in_use(host, port):
    import socket
    from contextlib import closing

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0


def _wait_service_up(host, port, timeout):
    beg_time = time.time()

    while time.time() - beg_time < timeout:
        if is_port_in_use(host, port):
            return True
        time.sleep(1)

    return False


def get_random_unused_port(
    host, min_port=1024, max_port=65535, max_retries=100, exclude_list=None
):
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
        if not is_port_in_use(host, port):
            return port
    raise RuntimeError(
        f"Get available port between range {min_port} and {max_port} failed."
    )


def get_spark_session():
    from pyspark.sql import SparkSession

    spark_session = SparkSession.getActiveSession()
    if spark_session is None:
        raise RuntimeError(
            "Spark session haven't been initiated yet. Please use "
            "`SparkSession.builder` to create a spark session and connect to a spark "
            "cluster."
        )
    return spark_session


def get_spark_application_driver_host(spark):
    return spark.conf.get("spark.driver.host")


def get_max_num_concurrent_tasks(spark_context, resource_profile):
    """Gets the current max number of concurrent tasks."""
    # pylint: disable=protected-access=
    ssc = spark_context._jsc.sc()
    if resource_profile is not None:

        def dummpy_mapper(_):
            pass

        # Runs a dummy spark job to register the `res_profile`
        spark_context.parallelize([1], 1).withResources(resource_profile).map(
            dummpy_mapper
        ).collect()

        return ssc.maxNumConcurrentTasks(resource_profile._java_resource_profile)
    else:
        return ssc.maxNumConcurrentTasks(
            ssc.resourceProfileManager().defaultResourceProfile()
        )


def _get_spark_worker_total_physical_memory():
    import psutil

    if RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES in os.environ:
        return int(os.environ[RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES])
    return psutil.virtual_memory().total


def _get_spark_worker_total_shared_memory():
    import shutil

    if RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES in os.environ:
        return int(os.environ[RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES])

    return shutil.disk_usage("/dev/shm").total


# The maximum proportion for Ray worker node object store memory size
_RAY_ON_SPARK_MAX_OBJECT_STORE_MEMORY_PROPORTION = 0.8

# The buffer offset for calculating Ray node memory.
_RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET = 0.8


def calc_mem_ray_head_node(configured_heap_memory_bytes, configured_object_store_bytes):
    import psutil
    import shutil

    if RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES in os.environ:
        available_physical_mem = int(
            os.environ[RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES]
        )
    else:
        available_physical_mem = psutil.virtual_memory().total

    available_physical_mem = (
        available_physical_mem * _RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET
    )

    if RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES in os.environ:
        available_shared_mem = int(os.environ[RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES])
    else:
        available_shared_mem = shutil.disk_usage("/dev/shm").total

    available_shared_mem = (
        available_shared_mem * _RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET
    )

    heap_mem_bytes, object_store_bytes, warning_msg = _calc_mem_per_ray_node(
        available_physical_mem,
        available_shared_mem,
        configured_heap_memory_bytes,
        configured_object_store_bytes,
    )

    if warning_msg is not None:
        _logger.warning(warning_msg)

    return heap_mem_bytes, object_store_bytes


def _calc_mem_per_ray_worker_node(
    num_task_slots,
    physical_mem_bytes,
    shared_mem_bytes,
    configured_heap_memory_bytes,
    configured_object_store_bytes,
):
    available_physical_mem_per_node = int(
        physical_mem_bytes / num_task_slots * _RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET
    )
    available_shared_mem_per_node = int(
        shared_mem_bytes / num_task_slots * _RAY_ON_SPARK_NODE_MEMORY_BUFFER_OFFSET
    )
    return _calc_mem_per_ray_node(
        available_physical_mem_per_node,
        available_shared_mem_per_node,
        configured_heap_memory_bytes,
        configured_object_store_bytes,
    )


def _calc_mem_per_ray_node(
    available_physical_mem_per_node,
    available_shared_mem_per_node,
    configured_heap_memory_bytes,
    configured_object_store_bytes,
):
    from ray._private.ray_constants import (
        DEFAULT_OBJECT_STORE_MEMORY_PROPORTION,
        OBJECT_STORE_MINIMUM_MEMORY_BYTES,
    )

    warning_msg = None

    object_store_bytes = configured_object_store_bytes or (
        available_physical_mem_per_node * DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
    )

    # If allow Ray using slow storage oas object store,
    # we don't need to cap object store size by /dev/shm capacity
    if not os.environ.get("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE"):
        if object_store_bytes > available_shared_mem_per_node:
            object_store_bytes = available_shared_mem_per_node

    object_store_bytes_upper_bound = (
        available_physical_mem_per_node
        * _RAY_ON_SPARK_MAX_OBJECT_STORE_MEMORY_PROPORTION
    )

    if object_store_bytes > object_store_bytes_upper_bound:
        object_store_bytes = object_store_bytes_upper_bound
        warning_msg = (
            "Your configured `object_store_memory_per_node` value "
            "is too high and it is capped by 80% of per-Ray node "
            "allocated memory."
        )

    if object_store_bytes < OBJECT_STORE_MINIMUM_MEMORY_BYTES:
        if object_store_bytes == available_shared_mem_per_node:
            warning_msg = (
                "Your operating system is configured with too small /dev/shm "
                "size, so `object_store_memory_worker_node` value is configured "
                f"to minimal size ({OBJECT_STORE_MINIMUM_MEMORY_BYTES} bytes),"
                f"Please increase system /dev/shm size."
            )
        else:
            warning_msg = (
                "You configured too small Ray node object store memory size, "
                "so `object_store_memory_worker_node` value is configured "
                f"to minimal size ({OBJECT_STORE_MINIMUM_MEMORY_BYTES} bytes),"
                "Please increase 'object_store_memory_worker_node' argument value."
            )

        object_store_bytes = OBJECT_STORE_MINIMUM_MEMORY_BYTES

    object_store_bytes = int(object_store_bytes)

    if configured_heap_memory_bytes is None:
        heap_mem_bytes = int(available_physical_mem_per_node - object_store_bytes)
    else:
        heap_mem_bytes = int(configured_heap_memory_bytes)

    return heap_mem_bytes, object_store_bytes, warning_msg


# User can manually set these environment variables
# if ray on spark code accessing corresponding information failed.
# Note these environment variables must be set in spark executor side,
# you should set them via setting spark config of
# `spark.executorEnv.[EnvironmentVariableName]`
RAY_ON_SPARK_WORKER_CPU_CORES = "RAY_ON_SPARK_WORKER_CPU_CORES"
RAY_ON_SPARK_WORKER_GPU_NUM = "RAY_ON_SPARK_WORKER_GPU_NUM"
RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES = "RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES"
RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES = "RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES"

# User can manually set these environment variables on spark driver node
# if ray on spark code accessing corresponding information failed.
RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES = "RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES"
RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES = "RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES"


def _get_cpu_cores():
    import multiprocessing

    if RAY_ON_SPARK_WORKER_CPU_CORES in os.environ:
        # In some cases, spark standalone cluster might configure virtual cpu cores
        # for spark worker that different with number of physical cpu cores,
        # but we cannot easily get the virtual cpu cores configured for spark
        # worker, as a workaround, we provide an environmental variable config
        # `RAY_ON_SPARK_WORKER_CPU_CORES` for user.
        return int(os.environ[RAY_ON_SPARK_WORKER_CPU_CORES])

    return multiprocessing.cpu_count()


def _get_num_physical_gpus():
    if RAY_ON_SPARK_WORKER_GPU_NUM in os.environ:
        # In some cases, spark standalone cluster might configure part of physical
        # GPUs for spark worker,
        # but we cannot easily get related configuration,
        # as a workaround, we provide an environmental variable config
        # `RAY_ON_SPARK_WORKER_CPU_CORES` for user.
        return int(os.environ[RAY_ON_SPARK_WORKER_GPU_NUM])

    if shutil.which("nvidia-smi") is None:
        # GPU driver is not installed.
        return 0
    try:
        completed_proc = subprocess.run(
            "nvidia-smi --query-gpu=name --format=csv,noheader",
            shell=True,
            check=True,
            text=True,
            capture_output=True,
        )
        return len(completed_proc.stdout.strip().split("\n"))
    except Exception as e:
        _logger.info(
            "'nvidia-smi --query-gpu=name --format=csv,noheader' command execution "
            f"failed, error: {repr(e)}"
        )
        return 0


def _get_local_ray_node_slots(
    num_cpus,
    num_gpus,
    num_cpus_per_node,
    num_gpus_per_node,
):
    if num_cpus_per_node > num_cpus:
        raise ValueError(
            "cpu number per Ray worker node should be <= spark worker node CPU cores, "
            f"you set cpu number per Ray worker node to {num_cpus_per_node} but "
            f"spark worker node CPU core number is {num_cpus}."
        )
    num_ray_node_slots = num_cpus // num_cpus_per_node

    if num_gpus_per_node > 0:
        if num_gpus_per_node > num_gpus:
            raise ValueError(
                "gpu number per Ray worker node should be <= spark worker node "
                "GPU number, you set GPU devices number per Ray worker node to "
                f"{num_gpus_per_node} but spark worker node GPU devices number "
                f"is {num_gpus}."
            )
        if num_ray_node_slots > num_gpus // num_gpus_per_node:
            num_ray_node_slots = num_gpus // num_gpus_per_node

    return num_ray_node_slots


def _get_avail_mem_per_ray_worker_node(
    num_cpus_per_node,
    num_gpus_per_node,
    heap_memory_per_node,
    object_store_memory_per_node,
):
    """
    Returns tuple of (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_bytes,
        error_message, # always None
        warning_message,
    )
    """
    num_cpus = _get_cpu_cores()
    if num_gpus_per_node > 0:
        num_gpus = _get_num_physical_gpus()
    else:
        num_gpus = 0

    num_ray_node_slots = _get_local_ray_node_slots(
        num_cpus, num_gpus, num_cpus_per_node, num_gpus_per_node
    )

    physical_mem_bytes = _get_spark_worker_total_physical_memory()
    shared_mem_bytes = _get_spark_worker_total_shared_memory()

    (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_bytes,
        warning_msg,
    ) = _calc_mem_per_ray_worker_node(
        num_ray_node_slots,
        physical_mem_bytes,
        shared_mem_bytes,
        heap_memory_per_node,
        object_store_memory_per_node,
    )
    return (
        ray_worker_node_heap_mem_bytes,
        ray_worker_node_object_store_bytes,
        None,
        warning_msg,
    )


def get_avail_mem_per_ray_worker_node(
    spark,
    heap_memory_per_node,
    object_store_memory_per_node,
    num_cpus_per_node,
    num_gpus_per_node,
):
    """
    Return the available heap memory and object store memory for each ray worker,
    and error / warning message if it has.
    Return value is a tuple of
    (ray_worker_node_heap_mem_bytes, ray_worker_node_object_store_bytes,
     error_message, warning_message)
    NB: We have one ray node per spark task.
    """

    def mapper(_):
        try:
            return _get_avail_mem_per_ray_worker_node(
                num_cpus_per_node,
                num_gpus_per_node,
                heap_memory_per_node,
                object_store_memory_per_node,
            )
        except Exception as e:
            import traceback

            trace_msg = "\n".join(traceback.format_tb(e.__traceback__))
            return -1, -1, repr(e) + trace_msg, None

    # Running memory inference routine on spark executor side since the spark worker
    # nodes may have a different machine configuration compared to the spark driver
    # node.
    (
        inferred_ray_worker_node_heap_mem_bytes,
        inferred_ray_worker_node_object_store_bytes,
        err,
        warning_msg,
    ) = (
        spark.sparkContext.parallelize([1], 1).map(mapper).collect()[0]
    )

    if err is not None:
        raise RuntimeError(
            f"Inferring ray worker node available memory failed, error: {err}. "
            "You can bypass this error by setting following spark configs: "
            "spark.executorEnv.RAY_ON_SPARK_WORKER_CPU_CORES, "
            "spark.executorEnv.RAY_ON_SPARK_WORKER_GPU_NUM, "
            "spark.executorEnv.RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES, "
            "spark.executorEnv.RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES."
        )
    if warning_msg is not None:
        _logger.warning(warning_msg)
    return (
        inferred_ray_worker_node_heap_mem_bytes,
        inferred_ray_worker_node_object_store_bytes,
    )


def get_spark_task_assigned_physical_gpus(gpu_addr_list):
    if "CUDA_VISIBLE_DEVICES" in os.environ:
        visible_cuda_dev_list = [
            int(dev.strip()) for dev in os.environ["CUDA_VISIBLE_DEVICES"].split(",")
        ]
        return [visible_cuda_dev_list[addr] for addr in gpu_addr_list]
    else:
        return gpu_addr_list
