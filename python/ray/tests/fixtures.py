import json
import pytest
import subprocess

import ray
from ray.tests.cluster_utils import Cluster
from ray.tests.utils import run_and_get_output


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def get_default_fixure_config():
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
    })
    return internal_config


def ray_start_with_parameter(request):
    internal_config = get_default_fixure_config()
    init_kargs = {
        "num_cpus": 1,
        "_internal_config": internal_config,
    }
    parameter = getattr(request, "param", {})
    init_kargs.update(parameter)
    # Start the Ray processes.
    address_info = ray.init(**init_kargs)
    return address_info


# The following fixture will start ray with 1 cpu.
@pytest.fixture
def ray_start_regular(request):
    address_info = ray_start_with_parameter(request)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_2_cpus(request):
    parameter = getattr(request, "param", {})
    parameter['num_cpus'] = 2
    request.param = parameter
    address_info = ray_start_with_parameter(request)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_10_cpus(request):
    parameter = getattr(request, "param", {})
    parameter['num_cpus'] = 10
    request.param = parameter
    address_info = ray_start_with_parameter(request)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def ray_start_cluster_with_parameter(request):
    internal_config = get_default_fixure_config()
    init_kargs = {
        "num_cpus": 1,
        "_internal_config": internal_config,
    }
    parameter = getattr(request, "param", {})
    if "num_nodes" in parameter:
        num_nodes = parameter["num_nodes"]
        del parameter["num_nodes"]
    else:
        num_nodes = 0
    do_init = False
    if "do_init" in parameter:
        do_init = parameter["do_init"]
        del parameter["do_init"]
    elif num_nodes > 0:
        do_init = True
    init_kargs.update(parameter)
    cluster = Cluster()
    remote_nodes = []
    for _ in range(num_nodes):
        remote_nodes.append(cluster.add_node(**init_kargs))
    if do_init:
        ray.init(redis_address=cluster.redis_address)
    return cluster, remote_nodes


@pytest.fixture
def ray_start_cluster(request):
    cluster, remote_nodes = ray_start_cluster_with_parameter(request)
    yield cluster, remote_nodes
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_start_cluster_head(request):
    parameter = getattr(request, "param", {})
    parameter['do_init'] = True
    parameter['num_nodes'] = 1
    request.param = parameter
    cluster, remote_nodes = ray_start_cluster_with_parameter(request)
    yield cluster, remote_nodes
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_start_cluster_2_nodes(request):
    parameter = getattr(request, "param", {})
    parameter['do_init'] = True
    parameter['num_nodes'] = 2
    request.param = parameter
    cluster, remote_nodes = ray_start_cluster_with_parameter(request)
    yield cluster, remote_nodes
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_start_object_store_memory(request):
    # Start the Ray processes.
    store_size = request.param
    internal_config = get_default_fixure_config()
    init_kargs = {
        "num_cpus": 1,
        "_internal_config": internal_config,
        "object_store_memory": store_size,
    }
    ray.init(**init_kargs)
    yield store_size
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def call_ray_start(request):
    parameter = getattr(request, "param", "ray start --head --num-cpus=1")
    command_args = parameter.split(" ")
    out = run_and_get_output(command_args)
    # Get the redis address from the output.
    redis_substring_prefix = "redis_address=\""
    redis_address_location = (
        out.find(redis_substring_prefix) + len(redis_substring_prefix))
    redis_address = out[redis_address_location:]
    redis_address = redis_address.split("\"")[0]

    yield redis_address

    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.Popen(["ray", "stop"]).wait()