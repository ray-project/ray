import os
import pickle
import pytest
import time

import grpc
import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
from ray.job_config import JobConfig
import ray.util.client.server.proxier as proxier


def test_proxy_manager(shutdown_only):
    ray_instance = ray.init()
    proxier.CHECK_PROCESS_INTERVAL_S = 1
    os.environ["TIMEOUT_FOR_SPECIFIC_SERVER_S"] = "5"
    pm = proxier.ProxyManager(ray_instance["redis_address"],
                              ray_instance["session_dir"])
    pm._free_ports = [45000, 45001]
    client = "client1"

    pm.start_specific_server(client, JobConfig())
    # Channel should be ready and corresponding to an existing server
    grpc.channel_ready_future(pm.get_channel(client)).result(timeout=5)

    proc = pm._get_server_for_client(client)
    assert proc.port == 45000

    proc.process_handle().process.wait(5)
    # Wait for reconcile loop
    time.sleep(2)

    assert len(pm._free_ports) == 2
    assert pm._get_unused_port() == 45001


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25001 --port 0"],
    indirect=True)
def test_multi_clients(call_ray_start):
    # TODO(ilr) Merge all connect tests to use proxier by default
    ray.client("localhost:25001").connect()
    job_id_one = ray.get_runtime_context().job_id
    ray.util.disconnect()
    ray.client("localhost:25001").connect()
    job_id_two = ray.get_runtime_context().job_id

    assert job_id_one != job_id_two


def test_prepare_runtime_init_req_fails():
    put_req = ray_client_pb2.DataRequest(put=ray_client_pb2.PutRequest())
    with pytest.raises(AssertionError):
        proxier.prepare_runtime_init_req(iter([put_req]))


def test_prepare_runtime_init_req_no_modification():
    job_config = JobConfig(worker_env={"KEY": "VALUE"}, ray_namespace="abc")
    init_req = ray_client_pb2.DataRequest(
        init=ray_client_pb2.InitRequest(job_config=pickle.dumps(job_config)))
    req, new_config = proxier.prepare_runtime_init_req(iter([init_req]))
    assert new_config.serialize() == job_config.serialize()
    assert isinstance(req, ray_client_pb2.DataRequest)
    assert pickle.loads(
        req.init.job_config).serialize() == new_config.serialize()


def test_prepare_runtime_init_req_modified_job():
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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
