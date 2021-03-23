"""
This file defines the common pytest fixtures used in current directory.
"""
import os
from contextlib import contextmanager
import pytest
import subprocess

import ray
from ray.cluster_utils import Cluster
from ray.test_utils import init_error_pubsub


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def get_default_fixure_system_config():
    system_config = {
        "object_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
        "object_store_full_delay_ms": 100,
    }
    return system_config


def get_default_fixture_ray_kwargs():
    system_config = get_default_fixure_system_config()
    ray_kwargs = {
        "num_cpus": 1,
        "object_store_memory": 150 * 1024 * 1024,
        "dashboard_port": None,
        "_system_config": system_config,
    }
    return ray_kwargs


@contextmanager
def _ray_start(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    # Start the Ray processes.
    address_info = ray.init(**init_kwargs)

    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_with_dashboard(request):
    param = getattr(request, "param", {})

    with _ray_start(
            num_cpus=1, include_dashboard=True, **param) as address_info:
        yield address_info


# The following fixture will start ray with 0 cpu.
@pytest.fixture
def ray_start_no_cpu(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=0, **param) as res:
        yield res


# The following fixture will start ray with 1 cpu.
@pytest.fixture
def ray_start_regular(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture(scope="module")
def ray_start_regular_shared(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture(
    scope="module", params=[{
        "local_mode": True
    }, {
        "local_mode": False
    }])
def ray_start_shared_local_modes(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture
def ray_start_2_cpus(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=2, **param) as res:
        yield res


@pytest.fixture
def ray_start_10_cpus(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=10, **param) as res:
        yield res


@contextmanager
def _ray_start_cluster(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    num_nodes = 0
    do_init = False
    # num_nodes & do_init are not arguments for ray.init, so delete them.
    if "num_nodes" in kwargs:
        num_nodes = kwargs["num_nodes"]
        del kwargs["num_nodes"]
    if "do_init" in kwargs:
        do_init = kwargs["do_init"]
        del kwargs["do_init"]
    elif num_nodes > 0:
        do_init = True
    init_kwargs.update(kwargs)
    cluster = Cluster()
    remote_nodes = []
    for i in range(num_nodes):
        if i > 0 and "_system_config" in init_kwargs:
            del init_kwargs["_system_config"]
        remote_nodes.append(cluster.add_node(**init_kwargs))
        # We assume driver will connect to the head (first node),
        # so ray init will be invoked if do_init is true
        if len(remote_nodes) == 1 and do_init:
            ray.init(address=cluster.address)
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


# This fixture will start a cluster with empty nodes.
@pytest.fixture
def ray_start_cluster(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(**param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_head(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, num_nodes=1, **param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_2_nodes(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, num_nodes=2, **param) as res:
        yield res


@pytest.fixture
def ray_start_object_store_memory(request):
    # Start the Ray processes.
    store_size = request.param
    system_config = get_default_fixure_system_config()
    init_kwargs = {
        "num_cpus": 1,
        "_system_config": system_config,
        "object_store_memory": store_size,
    }
    ray.init(**init_kwargs)
    yield store_size
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def call_ray_start(request):
    parameter = getattr(
        request, "param", "ray start --head --num-cpus=1 --min-worker-port=0 "
        "--max-worker-port=0 --port 0")
    command_args = parameter.split(" ")
    out = ray._private.utils.decode(
        subprocess.check_output(command_args, stderr=subprocess.STDOUT))
    # Get the redis address from the output.
    redis_substring_prefix = "--address='"
    address_location = (
        out.find(redis_substring_prefix) + len(redis_substring_prefix))
    address = out[address_location:]
    address = address.split("'")[0]

    yield address

    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.check_call(["ray", "stop"])


@pytest.fixture
def call_ray_stop_only():
    yield
    subprocess.check_call(["ray", "stop"])


@pytest.fixture
def enable_pickle_debug():
    os.environ["RAY_PICKLE_VERBOSE_DEBUG"] = "1"
    yield
    del os.environ["RAY_PICKLE_VERBOSE_DEBUG"]


@pytest.fixture()
def two_node_cluster():
    system_config = {
        "object_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
    }
    cluster = ray.cluster_utils.Cluster(
        head_node_args={"_system_config": system_config})
    for _ in range(2):
        remote_node = cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    yield cluster, remote_node

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture()
def error_pubsub():
    p = init_error_pubsub()
    yield p
    p.close()


@pytest.fixture()
def log_pubsub():
    p = ray.worker.global_worker.redis_client.pubsub(
        ignore_subscribe_messages=True)
    log_channel = ray.gcs_utils.LOG_FILE_CHANNEL
    p.psubscribe(log_channel)
    yield p
    p.close()
