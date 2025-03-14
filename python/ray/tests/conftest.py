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
from typing import List, Optional
from unittest import mock
import psutil
import pytest
import copy

import ray
import ray._private.ray_constants as ray_constants
from ray._private.conftest_utils import set_override_dashboard_url  # noqa: F401
from ray._private.runtime_env import virtualenv_utils
from ray._private.runtime_env.plugin_schema_manager import RuntimeEnvPluginSchemaManager

from ray._private.test_utils import (
    get_and_run_resource_killer,
    init_error_pubsub,
    init_log_pubsub,
    setup_tls,
    teardown_tls,
    enable_external_redis,
    redis_replicas,
    get_redis_cli,
    start_redis_instance,
    start_redis_sentinel_instance,
    redis_sentinel_replicas,
    wait_for_condition,
    find_free_port,
    reset_autoscaler_v2_enabled_cache,
    RayletKiller,
)
from ray.cluster_utils import AutoscalingCluster, Cluster, cluster_not_supported

# TODO (mengjin) Improve the logging in the conftest files so that the logger can log
# information in stdout as well as stderr and replace the print statements in the test
# files
logger = logging.getLogger(__name__)

START_REDIS_WAIT_RETRIES = int(os.environ.get("RAY_START_REDIS_WAIT_RETRIES", "60"))


@pytest.fixture(autouse=True)
def pre_envs(monkeypatch):
    # To make test run faster
    monkeypatch.setenv("RAY_NUM_REDIS_GET_RETRIES", "2")
    ray_constants.NUM_REDIS_GET_RETRIES = 2
    yield


def wait_for_redis_to_start(
    redis_ip_address: str, redis_port: bool, password=None, username=None
):
    """Wait for a Redis server to be available.

    This is accomplished by creating a Redis client and sending a random
    command to the server until the command gets through.

    Args:
        redis_ip_address: The IP address of the redis server.
        redis_port: The port of the redis server.
        username: The username of the Redis server.
        password: The password of the Redis server.

    Raises:
        Exception: An exception is raised if we could not connect with Redis.
    """
    import redis

    redis_client = redis.StrictRedis(
        host=redis_ip_address, port=redis_port, username=username, password=password
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
        "health_check_initial_delay_ms": 0,
        "health_check_failure_threshold": 10,
        "object_store_full_delay_ms": 100,
        "local_gc_min_interval_s": 1,
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


def is_process_listen_to_port(pid, port):
    retry_num = 10
    interval_time = 0.5
    for _ in range(retry_num):
        try:
            proc = psutil.Process(pid)
            for conn in proc.connections():
                if conn.status == "LISTEN" and conn.laddr.port == port:
                    return True
        except Exception:
            pass
        finally:
            time.sleep(interval_time)
    print(
        f"Process({pid}) has not listened to port {port} "
        + f"for more than {retry_num * interval_time}s."
    )
    return False


def find_user_process_by_port_and_status(
    port: int, statuses_to_check: Optional[list[str]]
):
    """
    Test helper function to find the processes that have a connection to a provided
    port and with statuses in the provided list.

    Args:
        port: The port to check.
        statuses_to_check: The list of statuses to check. If None, the function will not
            check the status of the connection.

    Returns:
        The first process that have a connection
    """
    # Here the function finds all the processes and checks if each of them is with the
    # provided port and status. This is inefficient comparing to leveraging
    # psutil.net_connections to directly filter the processes by port. However, the
    # method is chosen because the net_connections method will need root access to run
    # on macOS: https://psutil.readthedocs.io/en/latest/#psutil.net_connections.
    # Therefore, the current solution is chosen to make the function work for all
    processes = []
    for pid in psutil.pids():
        process = psutil.Process(pid)
        try:
            conns = process.connections()
            for conn in conns:
                if conn.laddr.port == port:
                    if statuses_to_check is None or conn.status in statuses_to_check:
                        processes.append(process)
        except (psutil.AccessDenied, psutil.ZombieProcess, psutil.NoSuchProcess):
            continue

    if not processes:
        print(
            f"Failed to find processes that have connections to the port {port} and "
            f"with connection status in {statuses_to_check}.  It could "
            "be because the process needs higher privilege to access its "
            "information or the port is not listened by any processes."
        )
    return processes


def redis_alive(port, enable_tls):
    try:
        # If there is no redis libs installed, skip the check.
        # This could happen In minimal test, where we don't have
        # redis.
        import redis
    except Exception:
        return True

    params = {}
    if enable_tls:
        from ray._raylet import Config

        params = {"ssl": True, "ssl_cert_reqs": "required"}
        if Config.REDIS_CA_CERT():
            params["ssl_ca_certs"] = Config.REDIS_CA_CERT()
        if Config.REDIS_CLIENT_CERT():
            params["ssl_certfile"] = Config.REDIS_CLIENT_CERT()
        if Config.REDIS_CLIENT_KEY():
            params["ssl_keyfile"] = Config.REDIS_CLIENT_KEY()

    cli = redis.Redis("localhost", port, **params)

    try:
        return cli.ping()
    except Exception:
        pass
    return False


def _find_available_ports(start: int, end: int, *, num: int = 1) -> List[int]:
    ports = []
    for _ in range(num):
        random_port = 0
        with socket.socket() as s:
            s.bind(("", 0))
            random_port = s.getsockname()[1]
        if random_port >= start and random_port <= end and random_port not in ports:
            ports.append(random_port)
            continue

        for port in range(start, end + 1):
            if port in ports:
                continue
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("", port))
                ports.append(port)
                break
            except OSError:
                pass

    if len(ports) != num:
        raise RuntimeError(f"Can't find {num} available port from {start} to {end}.")

    return ports


def start_redis_with_sentinel(db_dir):
    temp_dir = ray._private.utils.get_ray_temp_dir()

    redis_ports = _find_available_ports(49159, 55535, num=redis_sentinel_replicas() + 1)
    sentinel_port = redis_ports[0]
    master_port = redis_ports[1]
    redis_processes = [
        start_redis_instance(temp_dir, p, listen_to_localhost_only=True, db_dir=db_dir)[
            1
        ]
        for p in redis_ports[1:]
    ]

    # ensure all redis servers are up
    for port in redis_ports[1:]:
        wait_for_condition(redis_alive, 3, 100, port=port, enable_tls=False)

    # setup replicas of the master
    for port in redis_ports[2:]:
        redis_cli = get_redis_cli(port, False)
        redis_cli.replicaof("127.0.0.1", master_port)
        sentinel_process = start_redis_sentinel_instance(
            temp_dir, sentinel_port, master_port
        )
        address_str = f"127.0.0.1:{sentinel_port}"
        return address_str, redis_processes + [sentinel_process]


def start_redis(db_dir):
    retry_num = 0
    while True:
        is_need_restart = False
        processes = []
        enable_tls = "RAY_REDIS_CA_CERT" in os.environ
        leader_port = None
        leader_id = None
        redis_ports = []
        while len(redis_ports) != redis_replicas():
            temp_dir = ray._private.utils.get_ray_temp_dir()
            port, free_port = _find_available_ports(49159, 55535, num=2)
            try:
                node_id = None
                proc = None
                node_id, proc = start_redis_instance(
                    temp_dir,
                    port,
                    enable_tls=enable_tls,
                    replica_of=leader_port,
                    leader_id=leader_id,
                    db_dir=db_dir,
                    free_port=free_port,
                )
                wait_for_condition(
                    redis_alive, 3, 100, port=port, enable_tls=enable_tls
                )
            except Exception as e:
                print(f"Fails to start redis on port {port} with exception {e}")
                if (
                    proc is not None
                    and proc.process is not None
                    and proc.process.poll() is None
                ):
                    proc.process.kill()

                # TODO (mengjin) Here we added more debug logs here to help further
                # troubleshoot the potential race condition where the available port
                # we found above is taken by another process and the Redis server
                # cannot be started. Here we won't fail the test but we can check the
                # output log of the test to further investigate the issue if needed.
                if "Redis process exited unexpectedly" in str(e):
                    # Output the process that listens to the port
                    processes = find_user_process_by_port_and_status(
                        port, [psutil.CONN_LISTEN]
                    )

                    for process in processes:
                        print(
                            f"Another process({process.pid}) with command"
                            f"\"{' '.join(process.args)}\" is listening on the port"
                            f"{port}"
                        )

                continue

            redis_ports.append(port)
            if leader_port is None:
                leader_port = port
                leader_id = node_id
            processes.append(proc)
            # Check if th redis has started successfully and is listening on the port.
            if not is_process_listen_to_port(proc.process.pid, port):
                is_need_restart = True
                break

        if is_need_restart:
            retry_num += 1
            for proc in processes:
                proc.process.kill()

            if retry_num > 5:
                raise RuntimeError("Failed to start redis after {retry_num} attempts.")
            print(
                "Retry to start redis because the process failed to "
                + f"listen to the port({port}), retry num:{retry_num}."
            )
            continue

        if redis_replicas() > 1:

            redis_cli = get_redis_cli(str(leader_port), enable_tls)
            while redis_cli.cluster("info")["cluster_state"] != "ok":
                pass

        scheme = "rediss://" if enable_tls else ""
        address_str = f"{scheme}127.0.0.1:{redis_ports[-1]}"
        return address_str, processes


def kill_all_redis_server():
    import psutil

    # Find Redis server processes
    redis_procs = []
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            if proc.name() == "redis-server":
                redis_procs.append(proc)
        except psutil.NoSuchProcess:
            pass

    # Kill Redis server processes
    for proc in redis_procs:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass


@contextmanager
def _setup_redis(request, with_sentinel=False):
    with tempfile.TemporaryDirectory() as tmpdirname:
        kill_all_redis_server()
        address_str, processes = (
            start_redis_with_sentinel(tmpdirname)
            if with_sentinel
            else start_redis(tmpdirname)
        )
        old_addr = os.environ.get("RAY_REDIS_ADDRESS")
        os.environ["RAY_REDIS_ADDRESS"] = address_str
        import uuid

        ns = str(uuid.uuid4())
        old_ns = os.environ.get("RAY_external_storage_namespace")
        os.environ["RAY_external_storage_namespace"] = ns

        yield
        if old_addr is not None:
            os.environ["RAY_REDIS_ADDRESS"] = old_addr
        else:
            del os.environ["RAY_REDIS_ADDRESS"]

        if old_ns is not None:
            os.environ["RAY_external_storage_namespace"] = old_ns
        else:
            del os.environ["RAY_external_storage_namespace"]

        for proc in processes:
            proc.process.kill()
        kill_all_redis_server()


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
def external_redis_with_sentinel(request):
    with _setup_redis(request, True):
        yield


@pytest.fixture
def local_autoscaling_cluster(request, enable_v2):
    reset_autoscaler_v2_enabled_cache()

    # Start a mock multi-node autoscaling cluster.
    head_resources, worker_node_types, system_config = copy.deepcopy(request.param)
    cluster = AutoscalingCluster(
        head_resources=head_resources,
        worker_node_types=worker_node_types,
        autoscaler_v2=enable_v2,
    )
    cluster.start(_system_config=system_config)

    yield None

    # Shutdown the cluster
    cluster.shutdown()


@pytest.fixture
def shutdown_only(maybe_external_redis):
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Delete the cluster address just in case.
    ray._private.utils.reset_ray_address()


@pytest.fixture
def propagate_logs():
    # Ensure that logs are propagated to ancestor handles. This is required if using the
    # caplog or capsys fixtures with Ray's logging.
    # NOTE: This only enables log propagation in the driver process, not the workers!
    logging.getLogger("ray").propagate = True
    logging.getLogger("ray.data").propagate = True
    yield
    logging.getLogger("ray").propagate = False
    logging.getLogger("ray.data").propagate = False


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


@pytest.fixture(scope="module")
def ray_start_regular_shared_2_cpus(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=2, **param) as res:
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
def ray_start_cluster_head_with_external_redis_sentinel(
    request, external_redis_with_sentinel
):
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
            env = {**os.environ, **parameter.get("env")}

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
    idx = out.find(redis_substring_prefix)
    if idx >= 0:
        address_location = idx + len(redis_substring_prefix)
        address = out[address_location:]
        address = address.split("'")[0]
    else:
        address = None

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
    import ray.util.client.server.server as ray_client_server

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
    if os.environ.get("RAY_MINIMAL") == "1" and use_ray_client:
        pytest.skip("Skipping due to we don't have ray client in minimal.")

    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=4, dashboard_agent_listen_port=find_free_port())
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

        test_file_module = path / "file_module.py"
        with test_file_module.open(mode="w") as f:
            f.write("def hello():\n")
            f.write("    return 'hello'\n")

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
def set_enable_auto_connect(enable_auto_connect: bool = False):
    from ray._private import auto_init_hook

    try:
        old_value = auto_init_hook.enable_auto_connect
        auto_init_hook.enable_auto_connect = enable_auto_connect
        yield enable_auto_connect
    finally:
        auto_init_hook.enable_auto_connect = old_value


@pytest.fixture
def enable_mac_large_object_store():
    os.environ["RAY_ENABLE_MAC_LARGE_OBJECT_STORE"] = "1"
    yield
    del os.environ["RAY_ENABLE_MAC_LARGE_OBJECT_STORE"]


@pytest.fixture()
def two_node_cluster():
    system_config = {
        "object_timeout_milliseconds": 200,
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
        node_killer = get_and_run_resource_killer(RayletKiller, kill_interval)

    yield cluster

    if kill_interval is not None:
        ray.get(node_killer.stop_run.remote())
        killed = ray.get(node_killer.get_total_killed.remote())
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

    if virtualenv_utils.is_in_virtualenv():
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

        # Try up to 10 times.
        MAX_RETRY = 10
        for i in range(MAX_RETRY):
            try:
                sock.bind(("127.0.0.1", port))
                break
            except OSError as e:
                if i == MAX_RETRY - 1:
                    raise e
                else:
                    print(f"failed to bind on a port {port}. {e}")
                    time.sleep(1)
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

    if platform.system() == "Darwin":
        summary_dir = os.environ.get("RAY_TEST_SUMMARY_DIR_HOST")

    if not summary_dir:
        return

    if not os.path.exists(summary_dir):
        os.makedirs(summary_dir, exist_ok=True)

    test_name = rep.nodeid.replace(os.sep, "::")

    if platform.system() == "Windows":
        # ":" is not legal in filenames in windows
        test_name = test_name.replace(":", "$")

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

    # Add location to the test definition
    test_file, test_lineno, _test_node = rep.location
    test_path = os.path.abspath(test_file)
    markdown += f"Test location: {test_path}:{test_lineno}\n\n"

    # Print main traceback
    markdown += "##### Traceback\n\n"
    markdown += "```\n"
    markdown += str(main_tb)
    markdown += "\n```\n\n"

    # Print test definition location
    markdown += f"{main_loc.path}:{main_loc.lineno}\n\n"

    # If this is a longer exception chain, users can expand the full traceback
    if len(rep.longrepr.chain) > 1:
        markdown += "<details><summary>Full traceback</summary>\n\n"

        # Here we just print each traceback and the respective lines.
        for tb, loc, _ in rep.longrepr.chain:
            markdown += "```\n"
            markdown += str(tb)
            markdown += "\n```\n\n"
            if loc:
                markdown += f"{loc.path}:{loc.lineno}\n\n"

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


def create_ray_logs_for_failed_test(rep):
    """Creates artifact zip of /tmp/ray/session_latest/logs for failed tests"""

    # We temporarily restrict to Linux until we have artifact dirs
    # for Windows and Mac
    if platform.system() != "Linux" and platform.system() != "Windows":
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
    if platform.system() == "Windows":
        # ":" is not legal in filenames in windows
        test_name = test_name.replace(":", "$")
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


@pytest.fixture(scope="function")
def temp_file(request):
    with tempfile.NamedTemporaryFile("r+b") as fp:
        yield fp


@pytest.fixture(scope="function")
def temp_dir(request):
    with tempfile.TemporaryDirectory("r+b") as d:
        yield d


@pytest.fixture(scope="module")
def random_ascii_file(request):
    import random
    import string

    file_size = getattr(request, "param", 1 << 10)

    with tempfile.NamedTemporaryFile(mode="r+b") as fp:
        fp.write("".join(random.choices(string.ascii_letters, k=file_size)).encode())
        fp.flush()

        yield fp
