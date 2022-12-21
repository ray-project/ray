"""
This file defines the common pytest fixtures used in current directory.
"""
import json
import logging
import os
import platform
import shutil
import socket
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from tempfile import gettempdir
from typing import List, Tuple
from unittest import mock
import psutil
import pytest

import ray
import ray._private.ray_constants as ray_constants
import ray.util.client.server.server as ray_client_server
from ray._private.conftest_utils import set_override_dashboard_url  # noqa: F401
from ray._private.runtime_env.pip import PipProcessor
from ray._private.runtime_env.plugin_schema_manager import RuntimeEnvPluginSchemaManager

from ray._private.test_utils import (
    get_and_run_node_killer,
    init_error_pubsub,
    init_log_pubsub,
    setup_tls,
    teardown_tls,
    enable_external_redis,
    start_redis_instance,
)
from ray.cluster_utils import AutoscalingCluster, Cluster, cluster_not_supported

logger = logging.getLogger(__name__)

START_REDIS_WAIT_RETRIES = int(os.environ.get("RAY_START_REDIS_WAIT_RETRIES", "60"))


def wait_for_redis_to_start(redis_ip_address: str, redis_port: bool, password=None):
    """Wait for a Redis server to be available.

    This is accomplished by creating a Redis client and sending a random
    command to the server until the command gets through.

    Args:
        redis_ip_address: The IP address of the redis server.
        redis_port: The port of the redis server.
        password: The password of the redis server.

    Raises:
        Exception: An exception is raised if we could not connect with Redis.
    """
    import redis

    redis_client = redis.StrictRedis(
        host=redis_ip_address, port=redis_port, password=password
    )
    # Wait for the Redis server to start.
    num_retries = START_REDIS_WAIT_RETRIES

    delay = 0.001
    for i in range(num_retries):
        try:
            # Run some random command and see if it worked.
            logger.debug(
                "Waiting for redis server at {}:{} to respond...".format(
                    redis_ip_address, redis_port
                )
            )
            redis_client.client_list()
        # If the Redis service is delayed getting set up for any reason, we may
        # get a redis.ConnectionError: Error 111 connecting to host:port.
        # Connection refused.
        # Unfortunately, redis.ConnectionError is also the base class of
        # redis.AuthenticationError. We *don't* want to obscure a
        # redis.AuthenticationError, because that indicates the user provided a
        # bad password. Thus a double except clause to ensure a
        # redis.AuthenticationError isn't trapped here.
        except redis.AuthenticationError as authEx:
            raise RuntimeError(
                f"Unable to connect to Redis at {redis_ip_address}:{redis_port}."
            ) from authEx
        except redis.ConnectionError as connEx:
            if i >= num_retries - 1:
                raise RuntimeError(
                    f"Unable to connect to Redis at {redis_ip_address}:"
                    f"{redis_port} after {num_retries} retries. Check that "
                    f"{redis_ip_address}:{redis_port} is reachable from this "
                    "machine. If it is not, your firewall may be blocking "
                    "this port. If the problem is a flaky connection, try "
                    "setting the environment variable "
                    "`RAY_START_REDIS_WAIT_RETRIES` to increase the number of"
                    " attempts to ping the Redis server."
                ) from connEx
            # Wait a little bit.
            time.sleep(delay)
            # Make sure the retry interval doesn't increase too large, which will
            # affect the delivery time of the Ray cluster.
            delay = min(1, delay * 2)
        else:
            break
    else:
        raise RuntimeError(
            f"Unable to connect to Redis (after {num_retries} retries). "
            "If the Redis instance is on a different machine, check that "
            "your firewall and relevant Ray ports are configured properly. "
            "You can also set the environment variable "
            "`RAY_START_REDIS_WAIT_RETRIES` to increase the number of "
            "attempts to ping the Redis server."
        )


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
def _setup_redis(request):
    # Setup external Redis and env var for initialization.
    redis_ports = []
    for _ in range(3):
        with socket.socket() as s:
            s.bind(("", 0))
            port = s.getsockname()[1]
        redis_ports.append(port)

    processes = []
    enable_tls = "RAY_REDIS_CA_CERT" in os.environ
    leader_port = None
    for port in redis_ports:
        temp_dir = ray._private.utils.get_ray_temp_dir()
        port, proc = start_redis_instance(
            temp_dir,
            port,
            enable_tls=enable_tls,
            replica_of=leader_port,
        )
        if leader_port is None:
            leader_port = redis_ports[0]
        processes.append(proc)
    scheme = "rediss://" if enable_tls else ""
    address_str = f"{scheme}127.0.0.1:{redis_ports[2]}"
    old_addr = os.environ.get("RAY_REDIS_ADDRESS")
    os.environ["RAY_REDIS_ADDRESS"] = address_str
    yield
    if old_addr is not None:
        os.environ["RAY_REDIS_ADDRESS"] = old_addr
    else:
        del os.environ["RAY_REDIS_ADDRESS"]
    for proc in processes:
        proc.process.terminate()


@pytest.fixture
def maybe_external_redis(request):
    if enable_external_redis():
        with _setup_redis(request):
            yield
    else:
        yield


@pytest.fixture
def external_redis(request):
    with _setup_redis(request):
        yield


@pytest.fixture
def shutdown_only(maybe_external_redis):
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Delete the cluster address just in case.
    ray._private.utils.reset_ray_address()


# Provide a shared Ray instance for a test class
@pytest.fixture(scope="class")
def class_ray_instance():
    yield ray.init()
    ray.shutdown()
    # Delete the cluster address just in case.
    ray._private.utils.reset_ray_address()


@contextmanager
def _ray_start(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    # Start the Ray processes.
    address_info = ray.init("local", **init_kwargs)

    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Delete the cluster address just in case.
    ray._private.utils.reset_ray_address()


@pytest.fixture
def ray_start_with_dashboard(request, maybe_external_redis):
    param = getattr(request, "param", {})
    if param.get("num_cpus") is None:
        param["num_cpus"] = 1
    with _ray_start(include_dashboard=True, **param) as address_info:
        yield address_info


@pytest.fixture
def make_sure_dashboard_http_port_unused():
    """Make sure the dashboard agent http port is unused."""
    for process in psutil.process_iter():
        should_kill = False
        try:
            for conn in process.connections():
                if conn.laddr.port == ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT:
                    should_kill = True
                    break
        except Exception:
            continue
        if should_kill:
            try:
                process.kill()
                process.wait()
            except Exception:
                pass
    yield


# The following fixture will start ray with 0 cpu.
@pytest.fixture
def ray_start_no_cpu(request, maybe_external_redis):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=0, **param) as res:
        yield res


# The following fixture will start ray with 1 cpu.
@pytest.fixture
def ray_start_regular(request, maybe_external_redis):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


# We can compose external_redis and ray_start_regular instead of creating this
# separate fixture, if there is a good way to ensure external_redis runs before
# ray_start_regular.
@pytest.fixture
def ray_start_regular_with_external_redis(request, external_redis):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture(scope="module")
def ray_start_regular_shared(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture(scope="module", params=[{"local_mode": True}, {"local_mode": False}])
def ray_start_shared_local_modes(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture
def ray_start_2_cpus(request, maybe_external_redis):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=2, **param) as res:
        yield res


@pytest.fixture
def ray_start_10_cpus(request, maybe_external_redis):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=10, **param) as res:
        yield res


@contextmanager
def _ray_start_cluster(**kwargs):
    cluster_not_supported_ = kwargs.pop("skip_cluster", cluster_not_supported)
    if cluster_not_supported_:
        pytest.skip("Cluster not supported")
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
def ray_start_cluster(request, maybe_external_redis):
    param = getattr(request, "param", {})
    with _ray_start_cluster(**param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_enabled(request, maybe_external_redis):
    param = getattr(request, "param", {})
    param["skip_cluster"] = False
    with _ray_start_cluster(**param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_init(request, maybe_external_redis):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, **param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_head(request, maybe_external_redis):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, num_nodes=1, **param) as res:
        yield res


# We can compose external_redis and ray_start_cluster_head instead of creating
# this separate fixture, if there is a good way to ensure external_redis runs
# before ray_start_cluster_head.
@pytest.fixture
def ray_start_cluster_head_with_external_redis(request, external_redis):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, num_nodes=1, **param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_head_with_env_vars(request, maybe_external_redis, monkeypatch):
    param = getattr(request, "param", {})
    env_vars = param.pop("env_vars", {})
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)
    with _ray_start_cluster(do_init=True, num_nodes=1, **param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_2_nodes(request, maybe_external_redis):
    param = getattr(request, "param", {})
    with _ray_start_cluster(do_init=True, num_nodes=2, **param) as res:
        yield res


@pytest.fixture
def ray_start_object_store_memory(request, maybe_external_redis):
    # Start the Ray processes.
    store_size = request.param
    system_config = get_default_fixure_system_config()
    init_kwargs = {
        "num_cpus": 1,
        "_system_config": system_config,
        "object_store_memory": store_size,
    }
    ray.init("local", **init_kwargs)
    yield store_size
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def call_ray_start(request):
    with call_ray_start_context(request) as address:
        yield address


@contextmanager
def call_ray_start_context(request):
    default_cmd = (
        "ray start --head --num-cpus=1 --min-worker-port=0 "
        "--max-worker-port=0 --port 0"
    )
    parameter = getattr(request, "param", default_cmd)
    env = None

    if isinstance(parameter, dict):
        if "env" in parameter:
            env = {**parameter.get("env"), **os.environ}

        parameter = parameter.get("cmd", default_cmd)

    command_args = parameter.split(" ")

    try:
        out = ray._private.utils.decode(
            subprocess.check_output(command_args, stderr=subprocess.STDOUT, env=env)
        )
    except Exception as e:
        print(type(e), e)
        raise
    # Get the redis address from the output.
    redis_substring_prefix = "--address='"
    address_location = out.find(redis_substring_prefix) + len(redis_substring_prefix)
    address = out[address_location:]
    address = address.split("'")[0]

    yield address

    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.check_call(["ray", "stop"], env=env)
    # Delete the cluster address just in case.
    ray._private.utils.reset_ray_address()


@pytest.fixture
def call_ray_start_with_external_redis(request):
    ports = getattr(request, "param", "6379")
    port_list = ports.split(",")
    for port in port_list:
        temp_dir = ray._private.utils.get_ray_temp_dir()
        start_redis_instance(temp_dir, int(port), password="123")
    address_str = ",".join(map(lambda x: "localhost:" + x, port_list))
    cmd = f"ray start --head --address={address_str} --redis-password=123"
    subprocess.call(cmd.split(" "))

    yield address_str.split(",")[0]

    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.check_call(["ray", "stop"])
    # Delete the cluster address just in case.
    ray._private.utils.reset_ray_address()


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
    # Delete the cluster address just in case.
    ray._private.utils.reset_ray_address()


# Used to test both Ray Client and non-Ray Client codepaths.
# Usage: In your test, call `ray.init(address)`.
@pytest.fixture(scope="function", params=["ray_client", "no_ray_client"])
def start_cluster(ray_start_cluster_enabled, request):
    assert request.param in {"ray_client", "no_ray_client"}
    use_ray_client: bool = request.param == "ray_client"
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=4)
    if use_ray_client:
        cluster.head_node._ray_params.ray_client_server_port = "10004"
        cluster.head_node.start_ray_client_server()
        address = "ray://localhost:10004"
    else:
        address = cluster.address

    yield cluster, address


@pytest.fixture(scope="function")
def tmp_working_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        hello_file = path / "hello"
        with hello_file.open(mode="w") as f:
            f.write("world")

        module_path = path / "test_module"
        module_path.mkdir(parents=True)

        test_file = module_path / "test.py"
        with test_file.open(mode="w") as f:
            f.write("def one():\n")
            f.write("    return 1\n")

        init_file = module_path / "__init__.py"
        with init_file.open(mode="w") as f:
            f.write("from test_module.test import one\n")

        yield tmp_dir


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


@pytest.fixture
def enable_mac_large_object_store():
    os.environ["RAY_ENABLE_MAC_LARGE_OBJECT_STORE"] = "1"
    yield
    del os.environ["RAY_ENABLE_MAC_LARGE_OBJECT_STORE"]


@pytest.fixture()
def two_node_cluster():
    system_config = {
        "object_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
    }
    if cluster_not_supported:
        pytest.skip("Cluster not supported")
    cluster = ray.cluster_utils.Cluster(
        head_node_args={"_system_config": system_config}
    )
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
    p = init_log_pubsub()
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
    "params": {"directory_path": spill_local_path},
}

buffer_object_spilling_config = {
    "type": "filesystem",
    "params": {"directory_path": spill_local_path, "buffer_size": 1_000_000},
}

# Since we have differet protocol for a local external storage (e.g., fs)
# and distributed external storage (e.g., S3), we need to test both cases.
# This mocks the distributed fs with cluster utils.
mock_distributed_fs_object_spilling_config = {
    "type": "mock_distributed_fs",
    "params": {"directory_path": spill_local_path},
}
smart_open_object_spilling_config = {
    "type": "smart_open",
    "params": {"uri": f"s3://{bucket_name}/"},
}
ray_storage_object_spilling_config = {
    "type": "ray_storage",
    # Force the storage config so we don't need to patch each test to separately
    # configure the storage param under this.
    "params": {"_force_storage_for_testing": spill_local_path},
}
buffer_open_object_spilling_config = {
    "type": "smart_open",
    "params": {"uri": f"s3://{bucket_name}/", "buffer_size": 1000},
}
multi_smart_open_object_spilling_config = {
    "type": "smart_open",
    "params": {"uri": [f"s3://{bucket_name}/{i}" for i in range(3)]},
}

unstable_object_spilling_config = {
    "type": "unstable_fs",
    "params": {
        "directory_path": spill_local_path,
    },
}
slow_object_spilling_config = {
    "type": "slow_fs",
    "params": {
        "directory_path": spill_local_path,
    },
}


def create_object_spilling_config(request, tmp_path):
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    if (
        request.param["type"] == "filesystem"
        or request.param["type"] == "mock_distributed_fs"
    ):
        request.param["params"]["directory_path"] = str(temp_folder)
    return json.dumps(request.param), temp_folder


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
    ],
)
def fs_only_object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        ray_storage_object_spilling_config,
        # TODO(sang): Add a mock dependency to test S3.
        # smart_open_object_spilling_config,
    ],
)
def object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        mock_distributed_fs_object_spilling_config,
    ],
)
def multi_node_object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function",
    params=[
        unstable_object_spilling_config,
    ],
)
def unstable_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function",
    params=[
        slow_object_spilling_config,
    ],
)
def slow_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


def _ray_start_chaos_cluster(request):
    param = getattr(request, "param", {})
    kill_interval = param.pop("kill_interval", None)
    config = param.pop("_system_config", {})
    config.update(
        {
            "num_heartbeats_timeout": 10,
            "raylet_heartbeat_period_milliseconds": 100,
            "task_retry_delay_ms": 100,
        }
    )
    # Config of workers that are re-started.
    head_resources = param.pop("head_resources")
    worker_node_types = param.pop("worker_node_types")
    cluster = AutoscalingCluster(
        head_resources,
        worker_node_types,
        idle_timeout_minutes=10,  # Don't take down nodes.
        **param,
    )
    cluster.start(_system_config=config)
    ray.init("auto")
    nodes = ray.nodes()
    assert len(nodes) == 1

    if kill_interval is not None:
        node_killer = get_and_run_node_killer(kill_interval)

    yield cluster

    if kill_interval is not None:
        ray.get(node_killer.stop_run.remote())
        killed = ray.get(node_killer.get_total_killed_nodes.remote())
        assert len(killed) > 0
        died = {node["NodeID"] for node in ray.nodes() if not node["Alive"]}
        assert died.issubset(
            killed
        ), f"Raylets {died - killed} that we did not kill crashed"

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_start_chaos_cluster(request):
    """Returns the cluster and chaos thread."""
    for x in _ray_start_chaos_cluster(request):
        yield x


# Set scope to "class" to force this to run before start_cluster, whose scope
# is "function".  We need these env vars to be set before Ray is started.
@pytest.fixture(scope="class")
def runtime_env_disable_URI_cache():
    with mock.patch.dict(
        os.environ,
        {
            "RAY_RUNTIME_ENV_CONDA_CACHE_SIZE_GB": "0",
            "RAY_RUNTIME_ENV_PIP_CACHE_SIZE_GB": "0",
            "RAY_RUNTIME_ENV_WORKING_DIR_CACHE_SIZE_GB": "0",
            "RAY_RUNTIME_ENV_PY_MODULES_CACHE_SIZE_GB": "0",
        },
    ):
        print(
            "URI caching disabled (conda, pip, working_dir, py_modules cache "
            "size set to 0)."
        )
        yield


# Use to create virtualenv that clone from current python env.
# The difference between this fixture and `pytest_virtual.virtual` is that
# `pytest_virtual.virtual` will not inherit current python env's site-package.
# Note: Can't use in virtualenv, this must be noted when testing locally.
@pytest.fixture(scope="function")
def cloned_virtualenv():
    # Lazy import pytest_virtualenv,
    # aviod import `pytest_virtualenv` in test case `Minimal install`
    from pytest_virtualenv import VirtualEnv

    if PipProcessor._is_in_virtualenv():
        raise RuntimeError("Forbid the use of this fixture in virtualenv")

    venv = VirtualEnv(
        args=[
            "--system-site-packages",
            "--reset-app-data",
            "--no-periodic-update",
            "--no-download",
        ],
    )
    yield venv
    venv.teardown()


@pytest.fixture
def set_runtime_env_retry_times(request):
    runtime_env_retry_times = getattr(request, "param", "0")
    try:
        os.environ["RUNTIME_ENV_RETRY_TIMES"] = runtime_env_retry_times
        yield runtime_env_retry_times
    finally:
        del os.environ["RUNTIME_ENV_RETRY_TIMES"]


@pytest.fixture
def listen_port(request):
    port = getattr(request, "param", 0)
    try:
        sock = socket.socket()
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 0)
        sock.bind(("127.0.0.1", port))
        yield port
    finally:
        sock.close()


@pytest.fixture
def set_bad_runtime_env_cache_ttl_seconds(request):
    ttl = getattr(request, "param", "0")
    os.environ["BAD_RUNTIME_ENV_CACHE_TTL_SECONDS"] = ttl
    yield ttl
    del os.environ["BAD_RUNTIME_ENV_CACHE_TTL_SECONDS"]


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    try:
        append_short_test_summary(rep)
    except Exception as e:
        print(f"+++ Error creating PyTest summary\n{e}")
    try:
        create_ray_logs_for_failed_test(rep)
    except Exception as e:
        print(f"+++ Error saving Ray logs for failing test\n{e}")


def append_short_test_summary(rep):
    """Writes a short summary txt for failed tests to be printed later."""
    if rep.when != "call":
        return

    summary_dir = os.environ.get("RAY_TEST_SUMMARY_DIR")

    if platform.system() != "Linux":
        summary_dir = os.environ.get("RAY_TEST_SUMMARY_DIR_HOST")

    if not summary_dir:
        return

    if not os.path.exists(summary_dir):
        os.makedirs(summary_dir, exist_ok=True)

    test_name = rep.nodeid.replace(os.sep, "::")

    if os.name == "nt":
        # ":" is not legal in filenames in windows
        test_name.replace(":", "$")

    header_file = os.path.join(summary_dir, "000_header.txt")
    summary_file = os.path.join(summary_dir, test_name + ".txt")

    if rep.passed and os.path.exists(summary_file):
        # The test succeeded after failing, thus it is flaky.
        # We do not want to annotate flaky tests just now, so remove report.
        os.remove(summary_file)
        return

    # Only consider failed tests from now on
    if not rep.failed:
        return

    # No failing test information
    if rep.longrepr is None:
        return

    # No failing test information
    if not hasattr(rep.longrepr, "chain"):
        return

    if not os.path.exists(header_file):
        with open(header_file, "wt") as fp:
            test_label = os.environ.get("BUILDKITE_LABEL", "Unknown")
            job_id = os.environ.get("BUILDKITE_JOB_ID")

            fp.write(f"### Pytest failures for: [{test_label}](#{job_id})\n\n")

    # Use `wt` here to overwrite so we only have one result per test (exclude retries)
    with open(summary_file, "wt") as fp:
        fp.write(_get_markdown_annotation(rep))


def _get_markdown_annotation(rep) -> str:
    # Main traceback is the last in the chain (where the last error is raised)
    main_tb, main_loc, _ = rep.longrepr.chain[-1]
    markdown = ""

    # Only keep last line of the message
    short_message = list(filter(None, main_loc.message.split("\n")))[-1]

    # Header: Main error message
    markdown += f"#### {rep.nodeid}\n\n"
    markdown += "<details>\n"
    markdown += f"<summary>{short_message}</summary>\n\n"

    # Add link to test definition
    test_file, test_lineno, _test_node = rep.location
    test_path, test_url = _get_repo_github_path_and_link(
        os.path.abspath(test_file), test_lineno
    )
    markdown += f"Link to test: [{test_path}:{test_lineno}]({test_url})\n\n"

    # Print main traceback
    markdown += "##### Traceback\n\n"
    markdown += "```\n"
    markdown += str(main_tb)
    markdown += "\n```\n\n"

    # Print link to test definition in github
    path, url = _get_repo_github_path_and_link(main_loc.path, main_loc.lineno)
    markdown += f"[{path}:{main_loc.lineno}]({url})\n\n"

    # If this is a longer exception chain, users can expand the full traceback
    if len(rep.longrepr.chain) > 1:
        markdown += "<details><summary>Full traceback</summary>\n\n"

        # Here we just print each traceback and the link to the respective
        # lines in GutHub
        for tb, loc, _ in rep.longrepr.chain:
            if loc:
                path, url = _get_repo_github_path_and_link(loc.path, loc.lineno)
                github_link = f"[{path}:{loc.lineno}]({url})\n\n"
            else:
                github_link = ""

            markdown += "```\n"
            markdown += str(tb)
            markdown += "\n```\n\n"
            markdown += github_link

        markdown += "</details>\n"

    markdown += "<details><summary>PIP packages</summary>\n\n"
    markdown += "```\n"
    markdown += "\n".join(_get_pip_packages())
    markdown += "\n```\n\n"
    markdown += "</details>\n"

    markdown += "</details>\n\n"
    return markdown


def _get_pip_packages() -> List[str]:
    try:
        from pip._internal.operations import freeze

        return list(freeze.freeze())
    except Exception:
        return ["invalid"]


def _get_repo_github_path_and_link(file: str, lineno: int) -> Tuple[str, str]:
    base_url = "https://github.com/ray-project/ray/blob/{commit}/{path}#L{lineno}"

    commit = os.environ.get("BUILDKITE_COMMIT")

    if not commit:
        return file, ""

    path = os.path.relpath(file, "/ray")

    return path, base_url.format(commit=commit, path=path, lineno=lineno)


def create_ray_logs_for_failed_test(rep):
    """Creates artifact zip of /tmp/ray/session_latest/logs for failed tests"""

    # We temporarily restrict to Linux until we have artifact dirs
    # for Windows and Mac
    if platform.system() != "Linux":
        return

    # Only archive failed tests after the "call" phase of the test
    if rep.when != "call" or not rep.failed:
        return

    # Get dir to write zipped logs to
    archive_dir = os.environ.get("RAY_TEST_FAILURE_LOGS_ARCHIVE_DIR")

    if not archive_dir:
        return

    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)

    # Get logs dir from the latest ray session
    tmp_dir = gettempdir()
    logs_dir = os.path.join(tmp_dir, "ray", "session_latest", "logs")

    if not os.path.exists(logs_dir):
        return

    # Write zipped logs to logs archive dir
    test_name = rep.nodeid.replace(os.sep, "::")
    output_file = os.path.join(archive_dir, f"{test_name}_{time.time():.4f}")
    shutil.make_archive(output_file, "zip", logs_dir)


@pytest.fixture(params=[True, False])
def start_http_proxy(request):
    env = {}

    proxy = None
    try:
        if request.param:
            # the `proxy` command is from the proxy.py package.
            proxy = subprocess.Popen(
                ["proxy", "--port", "8899", "--log-level", "ERROR"]
            )
            env["RAY_grpc_enable_http_proxy"] = "1"
            proxy_url = "http://localhost:8899"
        else:
            proxy_url = "http://example.com"
        env["http_proxy"] = proxy_url
        env["https_proxy"] = proxy_url
        yield env
    finally:
        if proxy:
            proxy.terminate()
            proxy.wait()


@pytest.fixture
def set_runtime_env_plugins(request):
    runtime_env_plugins = getattr(request, "param", "0")
    try:
        os.environ["RAY_RUNTIME_ENV_PLUGINS"] = runtime_env_plugins
        yield runtime_env_plugins
    finally:
        del os.environ["RAY_RUNTIME_ENV_PLUGINS"]


@pytest.fixture
def set_runtime_env_plugin_schemas(request):
    runtime_env_plugin_schemas = getattr(request, "param", "0")
    try:
        os.environ["RAY_RUNTIME_ENV_PLUGIN_SCHEMAS"] = runtime_env_plugin_schemas
        # Clear and reload schemas.
        RuntimeEnvPluginSchemaManager.clear()
        yield runtime_env_plugin_schemas
    finally:
        del os.environ["RAY_RUNTIME_ENV_PLUGIN_SCHEMAS"]


@pytest.fixture(params=[True, False])
def enable_syncer_test(request, monkeypatch):
    with_syncer = request.param
    monkeypatch.setenv("RAY_use_ray_syncer", "true" if with_syncer else "false")
    ray._raylet.Config.initialize("")
    yield
    monkeypatch.delenv("RAY_use_ray_syncer")
    ray._raylet.Config.initialize("")
