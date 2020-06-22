"""
This file defines the common pytest fixtures used in current directory.
"""

from contextlib import contextmanager
import json
import pytest
import subprocess

import ray
from ray.cluster_utils import Cluster


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def get_default_fixure_internal_config():
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
        "object_store_full_max_retries": 3,
        "object_store_full_initial_delay_ms": 100,
    })
    return internal_config


def get_default_fixture_ray_kwargs():
    internal_config = get_default_fixure_internal_config()
    ray_kwargs = {
        "num_cpus": 1,
        "object_store_memory": 150 * 1024 * 1024,
        "_internal_config": internal_config,
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


@pytest.fixture(scope="session")
def ray_start_regular_shared(request):
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
        if i > 0 and "_internal_config" in init_kwargs:
            del init_kwargs["_internal_config"]
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
    internal_config = get_default_fixure_internal_config()
    init_kwargs = {
        "num_cpus": 1,
        "_internal_config": internal_config,
        "object_store_memory": store_size,
    }
    ray.init(**init_kwargs)
    yield store_size
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def call_ray_start(request):
    parameter = getattr(
        request, "param",
        "ray start --head --num-cpus=1 --min-worker-port=0 --max-worker-port=0"
    )
    command_args = parameter.split(" ")
    out = ray.utils.decode(
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


@pytest.fixture()
def two_node_cluster():
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
    })
    cluster = ray.cluster_utils.Cluster(
        head_node_args={"_internal_config": internal_config})
    for _ in range(2):
        remote_node = cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    yield cluster, remote_node

    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()
