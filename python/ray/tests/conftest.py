"""
This file defines the common pytest fixtures used in current directory.
"""
import os
from contextlib import contextmanager
import pytest
import subprocess
import json
import time

import ray
from ray.cluster_utils import Cluster, AutoscalingCluster
from ray._private.services import REDIS_EXECUTABLE, _start_redis_instance
from ray._private.test_utils import (init_error_pubsub, setup_tls,
                                     teardown_tls, get_and_run_node_killer)
import ray.util.client.server.server as ray_client_server
import ray._private.gcs_utils as gcs_utils


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
        "namespace": "default_test_namespace",
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
    if param.get("num_cpus") is None:
        param["num_cpus"] = 1
    with _ray_start(include_dashboard=True, **param) as address_info:
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
    namespace = init_kwargs.pop("namespace")
    cluster = Cluster()
    remote_nodes = []
    for i in range(num_nodes):
        if i > 0 and "_system_config" in init_kwargs:
            del init_kwargs["_system_config"]
        remote_nodes.append(cluster.add_node(**init_kwargs))
        # We assume driver will connect to the head (first node),
        # so ray init will be invoked if do_init is true
        if len(remote_nodes) == 1 and do_init:
            ray.init(address=cluster.address, namespace=namespace)
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
def ray_start_cluster_init(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, **param) as res:
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
def call_ray_start_with_external_redis(request):
    ports = getattr(request, "param", "6379")
    port_list = ports.split(",")
    for port in port_list:
        _start_redis_instance(REDIS_EXECUTABLE, int(port), password="123")
    address_str = ",".join(map(lambda x: "localhost:" + x, port_list))
    cmd = f"ray start --head --address={address_str} --redis-password=123"
    subprocess.call(cmd.split(" "))

    yield address_str.split(",")[0]

    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.check_call(["ray", "stop"])


@pytest.fixture
def init_and_serve():
    server_handle, _ = ray_client_server.init_and_serve("localhost:50051")
    yield server_handle
    ray_client_server.shutdown_with_server(server_handle.grpc_server)
    time.sleep(2)


@pytest.fixture
def call_ray_stop_only():
    yield
    subprocess.check_call(["ray", "stop"])


# Used to test both Ray Client and non-Ray Client codepaths.
# Usage: In your test, call `ray.init(address)`.
@pytest.fixture(scope="function", params=["ray_client", "no_ray_client"])
def start_cluster(ray_start_cluster, request):
    assert request.param in {"ray_client", "no_ray_client"}
    use_ray_client: bool = request.param == "ray_client"

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=4)
    if use_ray_client:
        cluster.head_node._ray_params.ray_client_server_port = "10004"
        cluster.head_node.start_ray_client_server()
        address = "ray://localhost:10004"
    else:
        address = cluster.address

    yield cluster, address


@pytest.fixture
def enable_pickle_debug():
    os.environ["RAY_PICKLE_VERBOSE_DEBUG"] = "1"
    yield
    del os.environ["RAY_PICKLE_VERBOSE_DEBUG"]


@pytest.fixture
def set_enable_auto_connect(enable_auto_connect: str = "0"):
    try:
        os.environ["RAY_ENABLE_AUTO_CONNECT"] = enable_auto_connect
        yield enable_auto_connect
    finally:
        del os.environ["RAY_ENABLE_AUTO_CONNECT"]


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
    log_channel = gcs_utils.LOG_FILE_CHANNEL
    p.psubscribe(log_channel)
    yield p
    p.close()


@pytest.fixture
def use_tls(request):
    if request.param:
        key_filepath, cert_filepath, temp_dir = setup_tls()
    yield request.param
    if request.param:
        teardown_tls(key_filepath, cert_filepath, temp_dir)


"""
Object spilling test fixture
"""
# -- Smart open param --
bucket_name = "object-spilling-test"

# -- File system param --
spill_local_path = "/tmp/spill"

# -- Spilling configs --
file_system_object_spilling_config = {
    "type": "filesystem",
    "params": {
        "directory_path": spill_local_path
    }
}

# Since we have differet protocol for a local external storage (e.g., fs)
# and distributed external storage (e.g., S3), we need to test both cases.
# This mocks the distributed fs with cluster utils.
mock_distributed_fs_object_spilling_config = {
    "type": "mock_distributed_fs",
    "params": {
        "directory_path": spill_local_path
    }
}
smart_open_object_spilling_config = {
    "type": "smart_open",
    "params": {
        "uri": f"s3://{bucket_name}/"
    }
}

unstable_object_spilling_config = {
    "type": "unstable_fs",
    "params": {
        "directory_path": spill_local_path,
    }
}
slow_object_spilling_config = {
    "type": "slow_fs",
    "params": {
        "directory_path": spill_local_path,
    }
}


def create_object_spilling_config(request, tmp_path):
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    if (request.param["type"] == "filesystem"
            or request.param["type"] == "mock_distributed_fs"):
        request.param["params"]["directory_path"] = str(temp_folder)
    return json.dumps(request.param), temp_folder


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        # TODO(sang): Add a mock dependency to test S3.
        # smart_open_object_spilling_config,
    ])
def object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        mock_distributed_fs_object_spilling_config
    ])
def multi_node_object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function", params=[
        unstable_object_spilling_config,
    ])
def unstable_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function", params=[
        slow_object_spilling_config,
    ])
def slow_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture
def ray_start_chaos_cluster(request):
    """Returns the cluster and chaos thread.
    """
    os.environ["RAY_num_heartbeats_timeout"] = "5"
    os.environ["RAY_raylet_heartbeat_period_milliseconds"] = "100"
    param = getattr(request, "param", {})
    kill_interval = param.get("kill_interval", 2)
    # Config of workers that are re-started.
    head_resources = param["head_resources"]
    worker_node_types = param["worker_node_types"]

    cluster = AutoscalingCluster(head_resources, worker_node_types)
    cluster.start()
    ray.init("auto")
    nodes = ray.nodes()
    assert len(nodes) == 1
    node_killer = get_and_run_node_killer(kill_interval)
    yield node_killer
    assert ray.get(node_killer.get_total_killed_nodes.remote()) > 0
    ray.shutdown()
    cluster.shutdown()
    del os.environ["RAY_num_heartbeats_timeout"]
    del os.environ["RAY_raylet_heartbeat_period_milliseconds"]
