import logging
import sys
import tempfile
from contextlib import contextmanager
from functools import partial
from pathlib import Path

import pytest
import pytest_asyncio

import ray
from ray._private.gcs_utils import GcsChannel
from ray._private.ray_constants import DEFAULT_DASHBOARD_AGENT_LISTEN_PORT
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.cluster_utils import Cluster, cluster_not_supported
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_pb2 import AllocationMode
from ray.core.generated.gcs_service_pb2 import CreateOrUpdateVirtualClusterRequest
from ray.dashboard.modules.job.common import JobSubmitRequest, validate_request_type
from ray.dashboard.modules.job.job_head import JobAgentSubmissionClient
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobSubmissionClient
from ray.runtime_env.runtime_env import RuntimeEnv
from ray.tests.conftest import get_default_fixture_ray_kwargs

TEMPLATE_ID_PREFIX = "template_id_"
NTEMPLATE = 5

logger = logging.getLogger(__name__)


@contextmanager
def _ray_start_virtual_cluster(**kwargs):
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
        env_vars = {}
        if i > 0:
            env_vars = {
                "RAY_NODE_TYPE_NAME": TEMPLATE_ID_PREFIX + str((i - 1) % NTEMPLATE)
            }
        remote_nodes.append(
            cluster.add_node(
                **init_kwargs,
                env_vars=env_vars,
            )
        )
        # We assume driver will connect to the head (first node),
        # so ray init will be invoked if do_init is true
        if len(remote_nodes) == 1 and do_init:
            ray.init(address=cluster.address, namespace=namespace)
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest_asyncio.fixture
def is_virtual_cluster_empty(request):
    param = getattr(request, "param", True)
    yield param


@pytest_asyncio.fixture
async def job_sdk_client(request, make_sure_dashboard_http_port_unused, external_redis):
    param = getattr(request, "param", {})
    with _ray_start_virtual_cluster(
        do_init=True, num_cpus=10, num_nodes=16, **param
    ) as res:
        ip, _ = res.webui_url.split(":")
        agent_address = f"{ip}:{DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
        assert wait_until_server_available(agent_address)
        head_address = res.webui_url
        assert wait_until_server_available(head_address)
        yield (
            JobAgentSubmissionClient(format_web_url(agent_address)),
            JobSubmissionClient(format_web_url(head_address)),
            res.gcs_address,
            res,
        )


async def create_virtual_cluster(
    gcs_address, virtual_cluster_id, replica_sets, allocation_mode=AllocationMode.MIXED
):
    channel = GcsChannel(gcs_address, aio=True)
    channel.connect()
    gcs_virtual_cluster_info_stub = (
        gcs_service_pb2_grpc.VirtualClusterInfoGcsServiceStub(channel.channel())
    )
    request = CreateOrUpdateVirtualClusterRequest(
        virtual_cluster_id=virtual_cluster_id,
        mode=allocation_mode,
        replica_sets=replica_sets,
    )
    reply = await (gcs_virtual_cluster_info_stub.CreateOrUpdateVirtualCluster(request))
    assert reply.status.code == 0
    return reply.node_instances


@pytest.mark.parametrize(
    "job_sdk_client",
    [{"_system_config": {"gcs_actor_scheduling_enabled": True}}],
    indirect=True,
)
@pytest.mark.asyncio
async def test_mixed_virtual_cluster(job_sdk_client):
    agent_client, head_client, gcs_address, cluster = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    node_to_virtual_cluster = {}
    for i in range(NTEMPLATE):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        nodes = await create_virtual_cluster(
            gcs_address, virtual_cluster_id, {TEMPLATE_ID_PREFIX + str(i): 3}
        )
        for node_id in nodes:
            assert node_id not in node_to_virtual_cluster
            node_to_virtual_cluster[node_id] = virtual_cluster_id

    for i in range(NTEMPLATE):
        actor_name = f"test_actors_{i}"
        pg_name = f"test_pgs_{i}"
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)
            driver_script = """
import ray
import time
ray.init(address='auto')

@ray.remote(max_restarts=10)
class Actor:
    def __init__(self):
        pass

    def run(self):
        while True:
            time.sleep(1)

pg = ray.util.placement_group(bundles=[{"CPU": 1}],
        name="__pg_name__", lifetime="detached")

a = Actor.options(name="__actor_name__", num_cpus=1, lifetime="detached").remote()
print("actor __actor_name__ created", flush=True)
ray.get(a.run.remote())
            """
            driver_script = driver_script.replace("__actor_name__", actor_name).replace(
                "__pg_name__", pg_name
            )
            test_script_file = path / "test_script.py"
            with open(test_script_file, "w+") as file:
                file.write(driver_script)

            runtime_env = {"working_dir": tmp_dir}
            runtime_env = upload_working_dir_if_needed(
                runtime_env, tmp_dir, logger=logger
            )
            runtime_env = RuntimeEnv(**runtime_env).to_dict()

            request = validate_request_type(
                {
                    "runtime_env": runtime_env,
                    "entrypoint": "python test_script.py",
                    "virtual_cluster_id": virtual_cluster_id,
                },
                JobSubmitRequest,
            )
            submit_result = await agent_client.submit_job_internal(request)
            job_id = submit_result.submission_id

            def _check_job_logs(job_id, actor_name):
                logs = head_client.get_job_logs(job_id)
                assert f"actor {actor_name} created" in logs
                return True

            wait_for_condition(
                partial(_check_job_logs, job_id, actor_name),
                timeout=100,
            )

            def _check_actor_alive(
                actor_name, node_to_virtual_cluster, virtual_cluster_id
            ):
                actors = ray.state.actors()
                for _, actor_info in actors.items():
                    if actor_info["Name"] == actor_name:
                        assert actor_info["State"] == "ALIVE"
                        assert actor_info["NumRestarts"] == 0
                        node_id = actor_info["Address"]["NodeID"]
                        assert node_to_virtual_cluster[node_id] == virtual_cluster_id
                        return True
                return False

            wait_for_condition(
                partial(
                    _check_actor_alive,
                    actor_name,
                    node_to_virtual_cluster,
                    virtual_cluster_id,
                ),
                timeout=100,
            )

            nodes_to_remove = set()

            actors = ray.state.actors()
            nassert = 0
            for _, actor_info in actors.items():
                if actor_info["Name"] == actor_name:
                    node_id = actor_info["Address"]["NodeID"]
                    nodes_to_remove.add(node_id)
                    nassert += 1
                    assert node_to_virtual_cluster[node_id] == virtual_cluster_id
            assert nassert > 0

            nassert = 0
            for _, placement_group_data in ray.util.placement_group_table().items():
                if placement_group_data["name"] == pg_name:
                    node_id = placement_group_data["bundles_to_node_id"][0]
                    nodes_to_remove.add(node_id)
                    nassert += 1
                    assert node_to_virtual_cluster[node_id] == virtual_cluster_id
            assert nassert > 0

            to_remove = []
            for node in cluster.worker_nodes:
                if node.node_id in nodes_to_remove:
                    to_remove.append(node)
            for node in to_remove:
                cluster.remove_node(node)

            def _check_actor_recover(
                nodes_to_remove, actor_name, node_to_virtual_cluster, virtual_cluster_id
            ):
                actors = ray.state.actors()
                for _, actor_info in actors.items():
                    if actor_info["Name"] == actor_name:
                        node_id = actor_info["Address"]["NodeID"]
                        assert actor_info["NumRestarts"] > 0
                        assert node_id not in nodes_to_remove
                        assert node_to_virtual_cluster[node_id] == virtual_cluster_id
                        return True
                return False

            wait_for_condition(
                partial(
                    _check_actor_recover,
                    nodes_to_remove,
                    actor_name,
                    node_to_virtual_cluster,
                    virtual_cluster_id,
                ),
                timeout=100,
            )

            def _check_pg_rescheduled(
                nodes_to_remove, pg_name, node_to_virtual_cluster, virtual_cluster_id
            ):
                for _, placement_group_data in ray.util.placement_group_table().items():
                    if placement_group_data["name"] == pg_name:
                        node_id = placement_group_data["bundles_to_node_id"][0]
                        assert node_id not in nodes_to_remove
                        assert node_to_virtual_cluster[node_id] == virtual_cluster_id
                        return True
                return False

            wait_for_condition(
                partial(
                    _check_pg_rescheduled,
                    nodes_to_remove,
                    pg_name,
                    node_to_virtual_cluster,
                    virtual_cluster_id,
                ),
                timeout=100,
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
