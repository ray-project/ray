import os
import pickle
import pytest
import sys
import time

import grpc
import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
from ray.job_config import JobConfig
from ray.test_utils import run_string_as_driver
import ray.util.client.server.proxier as proxier


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
def test_proxy_manager_lifecycle(shutdown_only):
    """
    Creates a ProxyManager and tests basic handling of the lifetime of a
    specific RayClient Server. It checks the following properties:
    1. The SpecificServer is created using the first port.
    2. The SpecificServer comes alive.
    3. The SpecificServer destructs itself when no client connects.
    4. The ProxyManager returns the port of the destructed SpecificServer.
    """
    ray_instance = ray.init()
    proxier.CHECK_PROCESS_INTERVAL_S = 1
    os.environ["TIMEOUT_FOR_SPECIFIC_SERVER_S"] = "5"
    pm = proxier.ProxyManager(ray_instance["redis_address"],
                              ray_instance["session_dir"])
    pm._free_ports = [45000, 45001]
    client = "client1"

    pm.create_specific_server(client)
    assert pm.start_specific_server(client, JobConfig())
    # Channel should be ready and corresponding to an existing server
    grpc.channel_ready_future(pm.get_channel(client)).result(timeout=5)

    proc = pm._get_server_for_client(client)
    assert proc.port == 45000

    proc.process_handle_future.result().process.wait(10)
    # Wait for reconcile loop
    time.sleep(2)

    assert len(pm._free_ports) == 2
    assert pm._get_unused_port() == 45001


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
def test_proxy_manager_bad_startup(shutdown_only):
    """
    Test that when a SpecificServer fails to start (because of a bad JobConfig)
    that it is properly GC'd
    """
    ray_instance = ray.init()
    proxier.CHECK_PROCESS_INTERVAL_S = 1
    proxier.CHECK_CHANNEL_TIMEOUT_S = 1
    pm = proxier.ProxyManager(ray_instance["redis_address"],
                              ray_instance["session_dir"])
    pm._free_ports = [46000, 46001]
    client = "client1"

    pm.create_specific_server(client)
    assert not pm.start_specific_server(
        client,
        JobConfig(
            runtime_env={"conda": "conda-env-that-sadly-does-not-exist"}))
    # Wait for reconcile loop
    time.sleep(2)
    assert pm.get_channel(client) is None

    assert len(pm._free_ports) == 2


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
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
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25005 --port 0"],
    indirect=True)
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
    sys.platform == "win32",
    reason="PSUtil does not work the same on windows.")
def test_delay_in_rewriting_environment(shutdown_only):
    """
    Check that a delay in `ray_client_server_env_prep` does not break
    a Client connecting.
    """
    proxier.LOGSTREAM_RETRIES = 3
    proxier.LOGSTREAM_RETRY_INTERVAL_SEC = 1
    ray_instance = ray.init()

    def delay_in_rewrite(input: JobConfig):
        import time
        time.sleep(6)
        return input

    proxier.ray_client_server_env_prep = delay_in_rewrite

    server = proxier.serve_proxier("localhost:25010",
                                   ray_instance["redis_address"],
                                   ray_instance["session_dir"])

    run_string_as_driver(check_connection)
    server.stop(0)


def test_prepare_runtime_init_req_fails():
    """
    Check that a connection that is initiated with a non-Init request
    raises an error.
    """
    put_req = ray_client_pb2.DataRequest(put=ray_client_pb2.PutRequest())
    with pytest.raises(AssertionError):
        proxier.prepare_runtime_init_req(iter([put_req]))


def test_prepare_runtime_init_req_no_modification():
    """
    Check that `prepare_runtime_init_req` properly extracts the JobConfig.
    """
    job_config = JobConfig(worker_env={"KEY": "VALUE"}, ray_namespace="abc")
    init_req = ray_client_pb2.DataRequest(
        init=ray_client_pb2.InitRequest(job_config=pickle.dumps(job_config)))
    req, new_config = proxier.prepare_runtime_init_req(iter([init_req]))
    assert new_config.serialize() == job_config.serialize()
    assert isinstance(req, ray_client_pb2.DataRequest)
    assert pickle.loads(
        req.init.job_config).serialize() == new_config.serialize()


def test_prepare_runtime_init_req_modified_job():
    """
    Check that `prepare_runtime_init_req` properly extracts the JobConfig and
    modifies it according to `ray_client_server_env_prep`.
    """
    job_config = JobConfig(worker_env={"KEY": "VALUE"}, ray_namespace="abc")
    init_req = ray_client_pb2.DataRequest(
        init=ray_client_pb2.InitRequest(job_config=pickle.dumps(job_config)))

    def modify_namespace(job_config: JobConfig):
        job_config.set_ray_namespace("test_value")
        return job_config

    proxier.ray_client_server_env_prep = modify_namespace
    req, new_config = proxier.prepare_runtime_init_req(iter([init_req]))

    assert new_config.ray_namespace == "test_value"
    assert pickle.loads(
        req.init.job_config).serialize() == new_config.serialize()


@pytest.mark.parametrize(
    "test_case",
    [  # no
        (["ipython", "-m", "ray.util.client.server"], True),
        (["ipython -m ray.util.client.server"], True),
        (["ipython -m", "ray.util.client.server"], True),
        (["bash", "ipython", "-m", "ray.util.client.server"], False),
        (["bash", "ipython -m ray.util.client.server"], False),
        (["python", "-m", "bash", "ipython -m ray.util.client.server"], False)
    ])
def test_match_running_client_server(test_case):
    command, result = test_case
    assert proxier._match_running_client_server(command) == result


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
