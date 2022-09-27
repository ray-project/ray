import json
import os
import random
import sys
import time
from glob import glob
from unittest.mock import patch

import grpc
import pytest

import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.util.client.server.proxier as proxier
from ray._private.ray_constants import REDIS_DEFAULT_PASSWORD
from ray._private.test_utils import run_string_as_driver
from ray.cloudpickle.compat import pickle
from ray.job_config import JobConfig


def start_ray_and_proxy_manager(n_ports=2):
    ray_instance = ray.init(_redis_password=REDIS_DEFAULT_PASSWORD)
    agent_port = ray._private.worker.global_worker.node.metrics_agent_port
    pm = proxier.ProxyManager(
        ray_instance["address"],
        session_dir=ray_instance["session_dir"],
        redis_password=REDIS_DEFAULT_PASSWORD,
        runtime_env_agent_port=agent_port,
    )
    free_ports = random.choices(range(45000, 45100), k=n_ports)
    pm._free_ports = free_ports.copy()

    return pm, free_ports


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
def test_proxy_manager_lifecycle(shutdown_only):
    """
    Creates a ProxyManager and tests basic handling of the lifetime of a
    specific RayClient Server. It checks the following properties:
    1. The SpecificServer is created using the first port.
    2. The SpecificServer comes alive and has a log associated with it.
    3. The SpecificServer destructs itself when no client connects.
    4. The ProxyManager returns the port of the destructed SpecificServer.
    """
    proxier.CHECK_PROCESS_INTERVAL_S = 1
    os.environ["TIMEOUT_FOR_SPECIFIC_SERVER_S"] = "5"
    pm, free_ports = start_ray_and_proxy_manager(n_ports=2)
    client = "client1"

    pm.create_specific_server(client)
    assert pm.start_specific_server(client, JobConfig())
    # Channel should be ready and corresponding to an existing server
    grpc.channel_ready_future(pm.get_channel(client)).result(timeout=5)

    proc = pm._get_server_for_client(client)
    assert proc.port == free_ports[0], f"Free Ports are: {free_ports}"

    log_files_path = os.path.join(
        pm.node.get_session_dir_path(), "logs", "ray_client_server*"
    )
    files = glob(log_files_path)
    assert any(str(free_ports[0]) in f for f in files)

    proc.process_handle_future.result().process.wait(10)
    # Wait for reconcile loop
    time.sleep(2)

    assert len(pm._free_ports) == 2
    assert pm._get_unused_port() == free_ports[1]


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@patch("ray.util.client.server.proxier.CHECK_PROCESS_INTERVAL_S", 1)
@patch("ray.util.client.server.proxier.CHECK_CHANNEL_TIMEOUT_S", 1)
def test_proxy_manager_bad_startup(shutdown_only):
    """
    Test that when a SpecificServer fails to start (because of a bad JobConfig)
    that it is properly GC'd.
    """
    pm, free_ports = start_ray_and_proxy_manager(n_ports=2)
    client = "client1"

    pm.create_specific_server(client)
    assert not pm.start_specific_server(
        client,
        JobConfig(runtime_env={"conda": "conda-env-that-sadly-does-not-exist"}),
    )
    # Wait for reconcile loop
    time.sleep(2)
    assert pm.get_channel(client) is None

    assert len(pm._free_ports) == 2


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True,
)
def test_multiple_clients_use_different_drivers(call_ray_start):
    """
    Test that each client uses a separate JobIDs and namespaces.
    """
    with ray.client("localhost:25001").connect():
        job_id_one = ray.get_runtime_context().job_id
        namespace_one = ray.get_runtime_context().namespace
    with ray.client("localhost:25001").connect():
        job_id_two = ray.get_runtime_context().job_id
        namespace_two = ray.get_runtime_context().namespace

    assert job_id_one != job_id_two
    assert namespace_one != namespace_two


check_we_are_second = """
import ray
info = ray.client('localhost:25005').connect()
assert info._num_clients == {num_clients}
"""


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --ray-client-server-port 25005 "
        "--port 0 --redis-password=password"
    ],
    indirect=True,
)
def test_correct_num_clients(call_ray_start):
    """
    Checks that the returned value of `num_clients` correctly tracks clients
    connecting and disconnecting.
    """
    info = ray.client("localhost:25005").connect()
    assert info._num_clients == 1
    run_string_as_driver(check_we_are_second.format(num_clients=2))
    ray.util.disconnect()
    run_string_as_driver(check_we_are_second.format(num_clients=1))


check_connection = """
import ray
ray.client("localhost:25010").connect()
assert ray.util.client.ray.worker.log_client.log_thread.is_alive()
"""


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="PSUtil does not work the same on windows & MacOS if flaky.",
)
@patch("ray.util.client.server.proxier.LOGSTREAM_RETRIES", 3)
@patch("ray.util.client.server.proxier.LOGSTREAM_RETRY_INTERVAL_SEC", 1)
def test_delay_in_rewriting_environment(shutdown_only):
    """
    Check that a delay in `ray_client_server_env_prep` does not break
    a Client connecting.
    """
    ray_instance = ray.init()
    server = proxier.serve_proxier(
        "localhost:25010",
        ray_instance["address"],
        session_dir=ray_instance["session_dir"],
    )

    def delay_in_rewrite(_input: JobConfig):
        time.sleep(6)
        return _input

    with patch.object(proxier, "ray_client_server_env_prep", delay_in_rewrite):
        run_string_as_driver(check_connection)
    server.stop(0)


get_error = """
import ray
error = None
try:
    ray.client("localhost:25030").connect()
except Exception as e:
    error = e

assert error is not None, "Connect did not fail!"
assert "Initialization failure from server" in str(error), "Bad error msg"
assert "WEIRD_ERROR" in str(error), "Bad error msg"
"""


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
def test_startup_error_yields_clean_result(shutdown_only):
    """
    Check that an error while preparing the environment yields an actionable,
    clear error on the *client side*.
    """
    ray_instance = ray.init()
    server = proxier.serve_proxier(
        "localhost:25030",
        ray_instance["address"],
        session_dir=ray_instance["session_dir"],
    )

    def raise_not_rewrite(input: JobConfig):
        raise RuntimeError("WEIRD_ERROR")

    with patch.object(proxier, "ray_client_server_env_prep", raise_not_rewrite):
        run_string_as_driver(get_error)

    server.stop(0)


@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --ray-client-server-port 25031 "
        "--port 0 --redis-password=password"
    ],
    indirect=True,
)
def test_runtime_install_error_message(call_ray_start):
    """
    Check that an error while preparing the runtime environment for the client
    server yields an actionable, clear error on the *client side*.
    """
    with pytest.raises(ConnectionAbortedError) as excinfo:
        ray.client("localhost:25031").env({"pip": ["ray-this-doesnt-exist"]}).connect()
    assert "No matching distribution found for ray-this-doesnt-exist" in str(
        excinfo.value
    ), str(excinfo.value)

    ray.util.disconnect()


def test_prepare_runtime_init_req_fails():
    """
    Check that a connection that is initiated with a non-Init request
    raises an error.
    """
    put_req = ray_client_pb2.DataRequest(put=ray_client_pb2.PutRequest())
    with pytest.raises(AssertionError):
        proxier.prepare_runtime_init_req(put_req)


def test_prepare_runtime_init_req_no_modification():
    """
    Check that `prepare_runtime_init_req` properly extracts the JobConfig.
    """
    job_config = JobConfig(
        runtime_env={"env_vars": {"KEY": "VALUE"}}, ray_namespace="abc"
    )
    init_req = ray_client_pb2.DataRequest(
        init=ray_client_pb2.InitRequest(
            job_config=pickle.dumps(job_config),
            ray_init_kwargs=json.dumps({"log_to_driver": False}),
        ),
    )
    req, new_config = proxier.prepare_runtime_init_req(init_req)
    assert new_config.serialize() == job_config.serialize()
    assert isinstance(req, ray_client_pb2.DataRequest)
    assert pickle.loads(req.init.job_config).serialize() == new_config.serialize()
    assert json.loads(req.init.ray_init_kwargs) == {"log_to_driver": False}


def test_prepare_runtime_init_req_modified_job():
    """
    Check that `prepare_runtime_init_req` properly extracts the JobConfig and
    modifies it according to `ray_client_server_env_prep`.
    """
    job_config = JobConfig(
        runtime_env={"env_vars": {"KEY": "VALUE"}}, ray_namespace="abc"
    )
    init_req = ray_client_pb2.DataRequest(
        init=ray_client_pb2.InitRequest(
            job_config=pickle.dumps(job_config),
            ray_init_kwargs=json.dumps({"log_to_driver": False}),
        )
    )

    def modify_namespace(job_config: JobConfig):
        job_config.set_ray_namespace("test_value")
        return job_config

    with patch.object(proxier, "ray_client_server_env_prep", modify_namespace):
        req, new_config = proxier.prepare_runtime_init_req(init_req)

    assert new_config.ray_namespace == "test_value"
    assert pickle.loads(req.init.job_config).serialize() == new_config.serialize()
    assert json.loads(req.init.ray_init_kwargs) == {"log_to_driver": False}


@pytest.mark.parametrize(
    "test_case",
    [  # no
        (["ipython", "-m", "ray.util.client.server"], True),
        (["ipython -m ray.util.client.server"], True),
        (["ipython -m", "ray.util.client.server"], True),
        (["bash", "ipython", "-m", "ray.util.client.server"], False),
        (["bash", "ipython -m ray.util.client.server"], False),
        (["python", "-m", "bash", "ipython -m ray.util.client.server"], False),
    ],
)
def test_match_running_client_server(test_case):
    command, result = test_case
    assert proxier._match_running_client_server(command) == result


@pytest.mark.parametrize("with_specific_server", [True, False])
@pytest.mark.skipif(
    sys.platform == "win32", reason="PSUtil does not work the same on windows."
)
@patch("ray.util.client.server.proxier.CHECK_PROCESS_INTERVAL_S", 1)
@patch("ray.util.client.server.proxier.CHECK_CHANNEL_TIMEOUT_S", 5)
def test_proxy_manager_internal_kv(shutdown_only, with_specific_server, monkeypatch):
    """
    Test that proxy manager can use internal kv with and without a
    SpecificServer and that once a SpecificServer is started up, it
    goes through it.
    """
    monkeypatch.setenv("TIMEOUT_FOR_SPECIFIC_SERVER_S", "5")
    pm, free_ports = start_ray_and_proxy_manager(n_ports=2)
    client = "client1"

    task_servicer = proxier.RayletServicerProxy(None, pm)

    def make_internal_kv_calls():
        response = task_servicer.KVPut(
            ray_client_pb2.KVPutRequest(key=b"key", value=b"val")
        )
        assert isinstance(response, ray_client_pb2.KVPutResponse)
        assert not response.already_exists

        response = task_servicer.KVPut(
            ray_client_pb2.KVPutRequest(key=b"key", value=b"val2")
        )
        assert isinstance(response, ray_client_pb2.KVPutResponse)
        assert response.already_exists

        response = task_servicer.KVGet(ray_client_pb2.KVGetRequest(key=b"key"))
        assert isinstance(response, ray_client_pb2.KVGetResponse)
        assert response.value == b"val"

        response = task_servicer.KVPut(
            ray_client_pb2.KVPutRequest(key=b"key", value=b"val2", overwrite=True)
        )
        assert isinstance(response, ray_client_pb2.KVPutResponse)
        assert response.already_exists

        response = task_servicer.KVGet(ray_client_pb2.KVGetRequest(key=b"key"))
        assert isinstance(response, ray_client_pb2.KVGetResponse)
        assert response.value == b"val2"

    with patch(
        "ray.util.client.server.proxier._get_client_id_from_context"
    ) as mock_get_client_id:
        mock_get_client_id.return_value = client

        if with_specific_server:
            pm.create_specific_server(client)
            assert pm.start_specific_server(client, JobConfig())
            channel = pm.get_channel(client)
            assert channel is not None
            task_servicer.Init(
                ray_client_pb2.InitRequest(job_config=pickle.dumps(JobConfig()))
            )

            # Mock out the internal kv calls in this process to raise an
            # exception if they're called. This verifies that we are not
            # making any calls in the proxier if there is a SpecificServer
            # started up.
            with patch(
                "ray.experimental.internal_kv._internal_kv_put"
            ) as mock_put, patch(
                "ray.experimental.internal_kv._internal_kv_get"
            ) as mock_get, patch(
                "ray.experimental.internal_kv._internal_kv_initialized"
            ) as mock_initialized:
                mock_put.side_effect = Exception("This shouldn't be called!")
                mock_get.side_effect = Exception("This shouldn't be called!")
                mock_initialized.side_effect = Exception("This shouldn't be called!")
                make_internal_kv_calls()
        else:
            make_internal_kv_calls()


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
