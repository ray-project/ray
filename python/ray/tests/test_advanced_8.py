# coding: utf-8
import glob
import logging
import os
import sys
import tempfile
import time
from unittest import mock

import numpy as np
import pytest
import psutil

import ray
from ray.dashboard import k8s_utils
import ray.ray_constants as ray_constants
import ray.util.accelerators
import ray._private.utils
import ray._private.gcs_utils as gcs_utils
import ray.cluster_utils
import ray._private.resource_spec as resource_spec

from ray._private.test_utils import (
    wait_for_condition,
)
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)


def test_export_after_shutdown(ray_start_regular):
    # This test checks that we can use actor and remote function definitions
    # across multiple Ray sessions.

    @ray.remote
    def f():
        pass

    @ray.remote
    class Actor:
        def method(self):
            pass

    ray.get(f.remote())
    a = Actor.remote()
    ray.get(a.method.remote())

    ray.shutdown()

    # Start Ray and use the remote function and actor again.
    ray.init(num_cpus=1)
    ray.get(f.remote())
    a = Actor.remote()
    ray.get(a.method.remote())

    ray.shutdown()

    # Start Ray again and make sure that these definitions can be exported from
    # workers.
    ray.init(num_cpus=2)

    @ray.remote
    def export_definitions_from_worker(remote_function, actor_class):
        ray.get(remote_function.remote())
        actor_handle = actor_class.remote()
        ray.get(actor_handle.method.remote())

    ray.get(export_definitions_from_worker.remote(f, Actor))


def test_invalid_unicode_in_worker_log(shutdown_only):
    info = ray.init(num_cpus=1)

    logs_dir = os.path.join(info["session_dir"], "logs")

    # Wait till first worker log file is created.
    while True:
        log_file_paths = glob.glob(f"{logs_dir}/worker*.out")
        if len(log_file_paths) == 0:
            time.sleep(0.2)
        else:
            break

    with open(log_file_paths[0], "wb") as f:
        f.write(b"\xe5abc\nline2\nline3\n")
        f.write(b"\xe5abc\nline2\nline3\n")
        f.write(b"\xe5abc\nline2\nline3\n")
        f.flush()

    # Wait till the log monitor reads the file.
    time.sleep(1.0)

    # Make sure that nothing has died.
    assert ray._private.services.remaining_processes_alive()


@pytest.mark.skip(reason="This test is too expensive to run.")
def test_move_log_files_to_old(shutdown_only):
    info = ray.init(num_cpus=1)

    logs_dir = os.path.join(info["session_dir"], "logs")

    @ray.remote
    class Actor:
        def f(self):
            print("function f finished")

    # First create a temporary actor.
    actors = [Actor.remote() for i in range(ray_constants.LOG_MONITOR_MAX_OPEN_FILES)]
    ray.get([a.f.remote() for a in actors])

    # Make sure no log files are in the "old" directory before the actors
    # are killed.
    assert len(glob.glob(f"{logs_dir}/old/worker*.out")) == 0

    # Now kill the actors so the files get moved to logs/old/.
    [a.__ray_terminate__.remote() for a in actors]

    while True:
        log_file_paths = glob.glob(f"{logs_dir}/old/worker*.out")
        if len(log_file_paths) > 0:
            with open(log_file_paths[0], "r") as f:
                assert "function f finished\n" in f.readlines()
            break

    # Make sure that nothing has died.
    assert ray._private.services.remaining_processes_alive()


@pytest.mark.parametrize(
    "ray_start_cluster",
    [
        {
            "num_cpus": 0,
            "num_nodes": 1,
            "do_init": False,
        }
    ],
    indirect=True,
)
def test_ray_address_environment_variable(ray_start_cluster):
    address = ray_start_cluster.address
    # In this test we use zero CPUs to distinguish between starting a local
    # ray cluster and connecting to an existing one.

    # Make sure we connect to an existing cluster if
    # RAY_ADDRESS is set to the cluster address.
    os.environ["RAY_ADDRESS"] = address
    ray.init()
    assert "CPU" not in ray.state.cluster_resources()
    ray.shutdown()
    del os.environ["RAY_ADDRESS"]

    # Make sure we connect to an existing cluster if
    # RAY_ADDRESS is set to "auto".
    os.environ["RAY_ADDRESS"] = "auto"
    ray.init()
    assert "CPU" not in ray.state.cluster_resources()
    ray.shutdown()
    del os.environ["RAY_ADDRESS"]

    # Prefer `address` parameter to the `RAY_ADDRESS` environment variable,
    # when `address` is not `auto`.
    os.environ["RAY_ADDRESS"] = "test"
    ray.init(address=address)
    assert "CPU" not in ray.state.cluster_resources()
    ray.shutdown()
    del os.environ["RAY_ADDRESS"]

    # Make sure we start a new cluster if RAY_ADDRESS is not set.
    ray.init()
    assert "CPU" in ray.state.cluster_resources()
    ray.shutdown()


def test_ray_resources_environment_variable(ray_start_cluster):
    address = ray_start_cluster.address

    os.environ["RAY_OVERRIDE_RESOURCES"] = '{"custom1":1, "custom2":2, "CPU":3}'
    ray.init(address=address, resources={"custom1": 3, "custom3": 3})

    cluster_resources = ray.cluster_resources()
    print(cluster_resources)
    assert cluster_resources["custom1"] == 1
    assert cluster_resources["custom2"] == 2
    assert cluster_resources["custom3"] == 3
    assert cluster_resources["CPU"] == 3


def test_gpu_info_parsing():
    info_string = """Model:           Tesla V100-SXM2-16GB
IRQ:             107
GPU UUID:        GPU-8eaaebb8-bb64-8489-fda2-62256e821983
Video BIOS:      88.00.4f.00.09
Bus Type:        PCIe
DMA Size:        47 bits
DMA Mask:        0x7fffffffffff
Bus Location:    0000:00:1e.0
Device Minor:    0
Blacklisted:     No
    """
    constraints_dict = resource_spec._constraints_from_gpu_info(info_string)
    expected_dict = {
        f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}V100": 1,
    }
    assert constraints_dict == expected_dict

    info_string = """Model:           Tesla T4
IRQ:             10
GPU UUID:        GPU-415fe7a8-f784-6e3d-a958-92ecffacafe2
Video BIOS:      90.04.84.00.06
Bus Type:        PCIe
DMA Size:        47 bits
DMA Mask:        0x7fffffffffff
Bus Location:    0000:00:1b.0
Device Minor:    0
Blacklisted:     No
    """
    constraints_dict = resource_spec._constraints_from_gpu_info(info_string)
    expected_dict = {
        f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}T4": 1,
    }
    assert constraints_dict == expected_dict

    assert resource_spec._constraints_from_gpu_info(None) == {}


def test_accelerator_type_api(shutdown_only):
    v100 = ray.util.accelerators.NVIDIA_TESLA_V100
    resource_name = f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}{v100}"
    ray.init(num_cpus=4, resources={resource_name: 1})

    quantity = 1

    @ray.remote(accelerator_type=v100)
    def decorated_func(quantity):
        wait_for_condition(lambda: ray.available_resources()[resource_name] < quantity)
        return True

    assert ray.get(decorated_func.remote(quantity))

    def via_options_func(quantity):
        wait_for_condition(lambda: ray.available_resources()[resource_name] < quantity)
        return True

    assert ray.get(
        ray.remote(via_options_func).options(accelerator_type=v100).remote(quantity)
    )

    @ray.remote(accelerator_type=v100)
    class DecoratedActor:
        def __init__(self):
            pass

        def initialized(self):
            pass

    class ActorWithOptions:
        def __init__(self):
            pass

        def initialized(self):
            pass

    decorated_actor = DecoratedActor.remote()
    # Avoid a race condition where the actor hasn't been initialized and
    # claimed the resources yet.
    ray.get(decorated_actor.initialized.remote())
    wait_for_condition(lambda: ray.available_resources()[resource_name] < quantity)

    quantity = ray.available_resources()[resource_name]
    with_options = ray.remote(ActorWithOptions).options(accelerator_type=v100).remote()
    ray.get(with_options.initialized.remote())
    wait_for_condition(lambda: ray.available_resources()[resource_name] < quantity)


@pytest.mark.skipif(sys.platform == "win32", reason="not relevant for windows")
def test_get_system_memory():
    # cgroups v1, set
    with tempfile.NamedTemporaryFile("w") as memory_limit_file:
        memory_limit_file.write("100")
        memory_limit_file.flush()
        assert (
            ray._private.utils.get_system_memory(
                memory_limit_filename=memory_limit_file.name,
                memory_limit_filename_v2="__does_not_exist__",
            )
            == 100
        )

    # cgroups v1, high
    with tempfile.NamedTemporaryFile("w") as memory_limit_file:
        memory_limit_file.write(str(2 ** 64))
        memory_limit_file.flush()
        psutil_memory_in_bytes = psutil.virtual_memory().total
        assert (
            ray._private.utils.get_system_memory(
                memory_limit_filename=memory_limit_file.name,
                memory_limit_filename_v2="__does_not_exist__",
            )
            == psutil_memory_in_bytes
        )
    # cgroups v2, set
    with tempfile.NamedTemporaryFile("w") as memory_max_file:
        memory_max_file.write("100")
        memory_max_file.flush()
        assert (
            ray._private.utils.get_system_memory(
                memory_limit_filename="__does_not_exist__",
                memory_limit_filename_v2=memory_max_file.name,
            )
            == 100
        )

    # cgroups v2, not set
    with tempfile.NamedTemporaryFile("w") as memory_max_file:
        memory_max_file.write("max")
        memory_max_file.flush()
        psutil_memory_in_bytes = psutil.virtual_memory().total
        assert (
            ray._private.utils.get_system_memory(
                memory_limit_filename="__does_not_exist__",
                memory_limit_filename_v2=memory_max_file.name,
            )
            == psutil_memory_in_bytes
        )


@pytest.mark.skipif(sys.platform == "win32", reason="not relevant for windows")
def test_detect_docker_cpus():
    # No limits set
    with tempfile.NamedTemporaryFile("w") as quota_file, tempfile.NamedTemporaryFile(
        "w"
    ) as period_file, tempfile.NamedTemporaryFile("w") as cpuset_file:
        quota_file.write("-1")
        period_file.write("100000")
        cpuset_file.write("0-63")
        quota_file.flush()
        period_file.flush()
        cpuset_file.flush()
        assert (
            ray._private.utils._get_docker_cpus(
                cpu_quota_file_name=quota_file.name,
                cpu_period_file_name=period_file.name,
                cpuset_file_name=cpuset_file.name,
            )
            == 64
        )

    # No cpuset used
    with tempfile.NamedTemporaryFile("w") as quota_file, tempfile.NamedTemporaryFile(
        "w"
    ) as period_file, tempfile.NamedTemporaryFile("w") as cpuset_file:
        quota_file.write("-1")
        period_file.write("100000")
        cpuset_file.write("0-10,20,50-63")
        quota_file.flush()
        period_file.flush()
        cpuset_file.flush()
        assert (
            ray._private.utils._get_docker_cpus(
                cpu_quota_file_name=quota_file.name,
                cpu_period_file_name=period_file.name,
                cpuset_file_name=cpuset_file.name,
            )
            == 26
        )

    # Quota set
    with tempfile.NamedTemporaryFile("w") as quota_file, tempfile.NamedTemporaryFile(
        "w"
    ) as period_file, tempfile.NamedTemporaryFile("w") as cpuset_file:
        quota_file.write("42")
        period_file.write("100")
        cpuset_file.write("0-63")
        quota_file.flush()
        period_file.flush()
        cpuset_file.flush()
        assert (
            ray._private.utils._get_docker_cpus(
                cpu_quota_file_name=quota_file.name,
                cpu_period_file_name=period_file.name,
                cpuset_file_name=cpuset_file.name,
            )
            == 0.42
        )

    # cgroups v2, cpu_quota set
    with tempfile.NamedTemporaryFile("w") as cpu_max_file:
        cpu_max_file.write("200000 100000")
        cpu_max_file.flush()
        assert (
            ray._private.utils._get_docker_cpus(
                cpu_quota_file_name="nope",
                cpu_period_file_name="give_up",
                cpuset_file_name="lose_hope",
                cpu_max_file_name=cpu_max_file.name,
            )
            == 2.0
        )

    # cgroups v2, cpu_quota unset
    with tempfile.NamedTemporaryFile("w") as cpu_max_file:
        cpu_max_file.write("max 100000")
        cpu_max_file.flush()
        assert (
            ray._private.utils._get_docker_cpus(
                cpu_quota_file_name="nope",
                cpu_period_file_name="give_up",
                cpuset_file_name="lose_hope",
                cpu_max_file_name=cpu_max_file.name,
            )
            is None
        )


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="No need to test on Windows."
)
@pytest.mark.parametrize("use_cgroups_v2", [True, False])
def test_k8s_cpu(use_cgroups_v2: bool):
    """Test all the functions in dashboard/k8s_utils.py.
    Also test ray._private.utils.get_num_cpus when running in a  K8s pod.
    Files were obtained from within a K8s pod with 2 CPU request, CPU limit
    unset, with 1 CPU of stress applied.
    """

    # Some experimentally-obtained K8S CPU usage files for use in test_k8s_cpu.
    PROCSTAT1 = """cpu  2945022 98 3329420 148744854 39522 0 118587 0 0 0
    cpu0 370299 14 413841 18589778 5304 0 15288 0 0 0
    cpu1 378637 10 414414 18589275 5283 0 14731 0 0 0
    cpu2 367328 8 420914 18590974 4844 0 14416 0 0 0
    cpu3 368378 11 423720 18572899 4948 0 14394 0 0 0
    cpu4 369051 13 414615 18607285 4736 0 14383 0 0 0
    cpu5 362958 10 415984 18576655 4590 0 16614 0 0 0
    cpu6 362536 13 414430 18605197 4785 0 14353 0 0 0
    cpu7 365833 15 411499 18612787 5028 0 14405 0 0 0
    intr 1000694027 125 0 0 39 154 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1028 0 2160913 0 2779605 8 0 3981333 3665198 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
    ctxt 1574979439
    btime 1615208601
    processes 857411
    procs_running 6
    procs_blocked 0
    softirq 524311775 0 230142964 27143 63542182 0 0 171 74042767 0 156556548
    """  # noqa

    PROCSTAT2 = """cpu  2945152 98 3329436 148745483 39522 0 118587 0 0 0
    cpu0 370399 14 413841 18589778 5304 0 15288 0 0 0
    cpu1 378647 10 414415 18589362 5283 0 14731 0 0 0
    cpu2 367329 8 420916 18591067 4844 0 14416 0 0 0
    cpu3 368381 11 423724 18572989 4948 0 14395 0 0 0
    cpu4 369052 13 414618 18607374 4736 0 14383 0 0 0
    cpu5 362968 10 415986 18576741 4590 0 16614 0 0 0
    cpu6 362537 13 414432 18605290 4785 0 14353 0 0 0
    cpu7 365836 15 411502 18612878 5028 0 14405 0 0 0
    intr 1000700905 125 0 0 39 154 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1028 0 2160923 0 2779605 8 0 3981353 3665218 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
    ctxt 1574988760
    btime 1615208601
    processes 857411
    procs_running 4
    procs_blocked 0
    softirq 524317451 0 230145523 27143 63542930 0 0 171 74043232 0 156558452
    """  # noqa

    CPUACCTUSAGE1 = "2268980984000"

    CPUACCTUSAGE2 = "2270120061999"

    CPU_STAT_1 = """usage_usec 2268980984
    user_usec 5673216
    system_usec 794353
    nr_periods 168
    nr_throttled 6
    throttled_usec 638117
    """

    CPU_STAT_2 = """usage_usec 2270120061
    user_usec 5673216
    system_usec 794353
    nr_periods 168
    nr_throttled 6
    throttled_usec 638117
    """

    cpu_file, cpu_v2_file, proc_stat_file = [
        tempfile.NamedTemporaryFile("w+") for _ in range(3)
    ]
    cpu_file.write(CPUACCTUSAGE1)
    cpu_v2_file.write(CPU_STAT_1)
    proc_stat_file.write(PROCSTAT1)
    for file in cpu_file, cpu_v2_file, proc_stat_file:
        file.flush()

    if use_cgroups_v2:
        # Should get a file not found for cpuacctusage if on cgroups v2
        cpu_usage_file = "NO_SUCH_FILE"
    else:
        # If using cgroups v1, use the temp file we've just made
        cpu_usage_file = cpu_file.name
    with mock.patch(
        "ray._private.utils.os.environ", {"KUBERNETES_SERVICE_HOST": "host"}
    ), mock.patch("ray.dashboard.k8s_utils.CPU_USAGE_PATH", cpu_usage_file), mock.patch(
        "ray.dashboard.k8s_utils.CPU_USAGE_PATH_V2", cpu_v2_file.name
    ), mock.patch(
        "ray.dashboard.k8s_utils.PROC_STAT_PATH", proc_stat_file.name
    ), mock.patch(
        # get_num_cpus is tested elsewhere
        "ray.dashboard.k8s_utils.get_num_cpus",
        mock.Mock(return_value=2),
    ), mock.patch(
        # Reset this global variable between tests.
        "ray.dashboard.k8s_utils.last_system_usage",
        None,
    ):
        # Validate mocks:
        # Confirm CPU_USAGE_PATH is found with cgroups v2, but not with v2.
        from ray.dashboard.k8s_utils import CPU_USAGE_PATH

        if use_cgroups_v2:
            with pytest.raises(FileNotFoundError):
                print(open(CPU_USAGE_PATH).read())
        else:
            print(open(CPU_USAGE_PATH).read())

        # Test helpers
        assert k8s_utils._cpu_usage() == 2268980984000
        assert k8s_utils._system_usage() == 1551775030000000
        assert k8s_utils._host_num_cpus() == 8

        # No delta for first computation, return 0.
        assert k8s_utils.cpu_percent() == 0.0

        # Write new usage info obtained after 1 sec wait.
        for file in cpu_file, cpu_v2_file, proc_stat_file:
            file.truncate(0)
            file.seek(0)
        cpu_file.write(CPUACCTUSAGE2)
        cpu_v2_file.write(CPU_STAT_2)
        proc_stat_file.write(PROCSTAT2)
        for file in cpu_file, cpu_v2_file, proc_stat_file:
            file.flush()

        # Files were extracted under 1 CPU of load on a 2 CPU pod
        assert 50 < k8s_utils.cpu_percent() < 60


def test_sync_job_config(shutdown_only):
    runtime_env = {"env_vars": {"key": "value"}}

    ray.init(
        job_config=ray.job_config.JobConfig(
            runtime_env=runtime_env,
        )
    )

    # Check that the job config is synchronized at the driver side.
    job_config = ray.worker.global_worker.core_worker.get_job_config()
    job_runtime_env = RuntimeEnv.deserialize(
        job_config.runtime_env_info.serialized_runtime_env
    )
    assert job_runtime_env.env_vars() == runtime_env["env_vars"]

    @ray.remote
    def get_job_config():
        job_config = ray.worker.global_worker.core_worker.get_job_config()
        return job_config.SerializeToString()

    # Check that the job config is synchronized at the worker side.
    job_config = gcs_utils.JobConfig()
    job_config.ParseFromString(ray.get(get_job_config.remote()))
    job_runtime_env = RuntimeEnv.deserialize(
        job_config.runtime_env_info.serialized_runtime_env
    )
    assert job_runtime_env.env_vars() == runtime_env["env_vars"]


def test_duplicated_arg(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def task_with_dup_arg(*args):
        return sum(args)

    # Basic verification.
    arr = np.ones(1 * 1024 * 1024, dtype=np.uint8)  # 1MB
    ref = ray.put(arr)
    assert np.array_equal(
        ray.get(task_with_dup_arg.remote(ref, ref, ref)), sum([arr, arr, arr])
    )

    # Make sure it works when it is mixed with other args.
    ref2 = ray.put(arr)
    assert np.array_equal(
        ray.get(task_with_dup_arg.remote(ref, ref2, ref)), sum([arr, arr, arr])
    )

    # Test complicated scenario with multi nodes.
    cluster.add_node(num_cpus=1, resources={"worker_1": 1})
    cluster.add_node(num_cpus=1, resources={"worker_2": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def create_remote_ref(arr):
        return ray.put(arr)

    @ray.remote
    def task_with_dup_arg_ref(*args):
        args = ray.get(list(args))
        return sum(args)

    ref1 = create_remote_ref.options(resources={"worker_1": 1}).remote(arr)
    ref2 = create_remote_ref.options(resources={"worker_2": 1}).remote(arr)
    ref3 = create_remote_ref.remote(arr)
    np.array_equal(
        ray.get(task_with_dup_arg_ref.remote(ref1, ref2, ref3, ref1, ref2, ref3)),
        sum([arr] * 6),
    )


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
