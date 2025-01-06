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
from ray.core.generated.gcs_service_pb2 import CreateOrUpdateVirtualClusterRequest
from ray.dashboard.modules.job.common import (
    JOB_ACTOR_NAME_TEMPLATE,
    SUPERVISOR_ACTOR_RAY_NAMESPACE,
)
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobSubmissionClient
from ray.runtime_env.runtime_env import RuntimeEnv
from ray.tests.conftest import get_default_fixture_ray_kwargs

TEMPLATE_ID_PREFIX = "template_id_"
kPrimaryClusterID = "kPrimaryClusterID"

logger = logging.getLogger(__name__)


@contextmanager
def _ray_start_virtual_cluster(**kwargs):
    cluster_not_supported_ = kwargs.pop("skip_cluster", cluster_not_supported)
    if cluster_not_supported_:
        pytest.skip("Cluster not supported")
    init_kwargs = get_default_fixture_ray_kwargs()
    num_nodes = 0
    do_init = False
    ntemplates = kwargs["ntemplates"]
    # num_nodes & do_init are not arguments for ray.init, so delete them.
    if "num_nodes" in kwargs:
        num_nodes = kwargs["num_nodes"]
        del kwargs["num_nodes"]
    if "do_init" in kwargs:
        do_init = kwargs["do_init"]
        del kwargs["do_init"]
    elif num_nodes > 0:
        do_init = True

    kwargs.pop("ntemplates")
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
                "RAY_NODE_TYPE_NAME": TEMPLATE_ID_PREFIX + str((i - 1) % ntemplates)
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
    ntemplates = param["ntemplates"]
    with _ray_start_virtual_cluster(
        do_init=True, num_cpus=20, num_nodes=4 * ntemplates + 1, **param
    ) as res:
        ip, _ = res.webui_url.split(":")
        agent_address = f"{ip}:{DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
        assert wait_until_server_available(agent_address)
        head_address = res.webui_url
        assert wait_until_server_available(head_address)
        yield (
            JobSubmissionClient(format_web_url(head_address)),
            res.gcs_address,
            res,
        )


async def create_virtual_cluster(
    gcs_address, virtual_cluster_id, replica_sets, divisible=False
):
    channel = GcsChannel(gcs_address, aio=True)
    channel.connect()
    gcs_virtual_cluster_info_stub = (
        gcs_service_pb2_grpc.VirtualClusterInfoGcsServiceStub(channel.channel())
    )
    request = CreateOrUpdateVirtualClusterRequest(
        virtual_cluster_id=virtual_cluster_id,
        divisible=divisible,
        replica_sets=replica_sets,
    )
    reply = await (gcs_virtual_cluster_info_stub.CreateOrUpdateVirtualCluster(request))
    assert reply.status.code == 0
    return reply.node_instances


@pytest.mark.parametrize(
    "job_sdk_client",
    [
        {
            "_system_config": {"gcs_actor_scheduling_enabled": False},
            "ntemplates": 3,
        },
        {
            "_system_config": {"gcs_actor_scheduling_enabled": True},
            "ntemplates": 3,
        },
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_mixed_virtual_cluster(job_sdk_client):
    head_client, gcs_address, cluster = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    node_to_virtual_cluster = {}
    ntemplates = 3
    for i in range(ntemplates):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        nodes = await create_virtual_cluster(
            gcs_address, virtual_cluster_id, {TEMPLATE_ID_PREFIX + str(i): 3}
        )
        for node_id in nodes:
            assert node_id not in node_to_virtual_cluster
            node_to_virtual_cluster[node_id] = virtual_cluster_id

    @ray.remote
    class ControlActor:
        def __init__(self):
            self._nodes = set()
            self._ready = False

        def ready(self):
            self._ready = True

        def is_ready(self):
            return self._ready

        def add_node(self, node_id):
            self._nodes.add(node_id)

        def nodes(self):
            return self._nodes

    for i in range(ntemplates):
        actor_name = f"test_actors_{i}"
        pg_name = f"test_pgs_{i}"
        control_actor_name = f"control_{i}"
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        control_actor = ControlActor.options(
            name=control_actor_name, namespace="control"
        ).remote()
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)
            driver_script = """
import ray
import time
import asyncio

ray.init(address="auto")

control = ray.get_actor(name="{control_actor_name}", namespace="control")


@ray.remote(max_restarts=10)
class Actor:
    def __init__(self, control, pg):
        node_id = ray.get_runtime_context().get_node_id()
        ray.get(control.add_node.remote(node_id))
        self._pg = pg

    async def run(self, control):
        node_id = ray.get_runtime_context().get_node_id()
        await control.add_node.remote(node_id)

        while True:
            node_id = ray.util.placement_group_table(self._pg)["bundles_to_node_id"][0]
            if node_id == "":
                await asyncio.sleep(1)
                continue
            break

        await control.add_node.remote(node_id)

        await control.ready.remote()
        while True:
            await asyncio.sleep(1)

    async def get_node_id(self):
        while True:
            node_id = ray.util.placement_group_table(pg)["bundles_to_node_id"][0]
            if node_id == "":
                await asyncio.sleep(1)
                continue
            break
        return (ray.get_runtime_context().get_node_id(), node_id)


pg = ray.util.placement_group(
    bundles=[{{"CPU": 1}}], name="{pg_name}", lifetime="detached"
)


@ray.remote
def hello(control):
    node_id = ray.get_runtime_context().get_node_id()
    ray.get(control.add_node.remote(node_id))


ray.get(hello.remote(control))
a = Actor.options(name="{actor_name}",
                  namespace="control",
                  num_cpus=1,
                  lifetime="detached").remote(
    control, pg
)
ray.get(a.run.remote(control))
            """
            driver_script = driver_script.format(
                actor_name=actor_name,
                pg_name=pg_name,
                control_actor_name=control_actor_name,
            )
            test_script_file = path / "test_script.py"
            with open(test_script_file, "w+") as file:
                file.write(driver_script)

            runtime_env = {"working_dir": tmp_dir}
            runtime_env = upload_working_dir_if_needed(
                runtime_env, tmp_dir, logger=logger
            )
            runtime_env = RuntimeEnv(**runtime_env).to_dict()

            job_id = head_client.submit_job(
                entrypoint="python test_script.py",
                entrypoint_memory=1,
                runtime_env=runtime_env,
                virtual_cluster_id=virtual_cluster_id,
            )

            def _check_ready(control_actor):
                return ray.get(control_actor.is_ready.remote())

            wait_for_condition(partial(_check_ready, control_actor), timeout=20)

            def _check_virtual_cluster(
                control_actor, node_to_virtual_cluster, virtual_cluster_id
            ):
                nodes = ray.get(control_actor.nodes.remote())
                assert len(nodes) > 0
                for node in nodes:
                    assert node_to_virtual_cluster[node] == virtual_cluster_id
                return True

            wait_for_condition(
                partial(
                    _check_virtual_cluster,
                    control_actor,
                    node_to_virtual_cluster,
                    virtual_cluster_id,
                ),
                timeout=20,
            )

            supervisor_actor = ray.get_actor(
                name=JOB_ACTOR_NAME_TEMPLATE.format(job_id=job_id),
                namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
            )
            actor_info = ray.state.actors(supervisor_actor._actor_id.hex())
            driver_node_id = actor_info["Address"]["NodeID"]
            assert node_to_virtual_cluster[driver_node_id] == virtual_cluster_id

            job_info = head_client.get_job_info(job_id)
            assert (
                node_to_virtual_cluster[job_info.driver_node_id] == virtual_cluster_id
            )

            nodes_to_remove = ray.get(control_actor.nodes.remote())
            if driver_node_id in nodes_to_remove:
                nodes_to_remove.remove(driver_node_id)

            to_remove = []
            for node in cluster.worker_nodes:
                if node.node_id in nodes_to_remove:
                    to_remove.append(node)
            for node in to_remove:
                cluster.remove_node(node)

            def _check_recover(
                nodes_to_remove, actor_name, node_to_virtual_cluster, virtual_cluster_id
            ):
                actor = ray.get_actor(actor_name, namespace="control")
                nodes = ray.get(actor.get_node_id.remote())
                for node_id in nodes:
                    assert node_id not in nodes_to_remove
                    assert node_to_virtual_cluster[node_id] == virtual_cluster_id
                return True

            wait_for_condition(
                partial(
                    _check_recover,
                    nodes_to_remove,
                    actor_name,
                    node_to_virtual_cluster,
                    virtual_cluster_id,
                ),
                timeout=120,
            )
            head_client.stop_job(job_id)


@pytest.mark.parametrize(
    "job_sdk_client",
    [
        {
            "_system_config": {"gcs_actor_scheduling_enabled": False},
            "ntemplates": 4,
        },
        {
            "_system_config": {"gcs_actor_scheduling_enabled": True},
            "ntemplates": 4,
        },
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_divisible_virtual_cluster(job_sdk_client):
    head_client, gcs_address, cluster = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    node_to_virtual_cluster = {}
    ntemplates = 3
    for i in range(ntemplates):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        nodes = await create_virtual_cluster(
            gcs_address,
            virtual_cluster_id,
            {TEMPLATE_ID_PREFIX + str(i): 2},
            True,
        )
        for node_id in nodes:
            assert node_id not in node_to_virtual_cluster
            node_to_virtual_cluster[node_id] = virtual_cluster_id

    for node in cluster.worker_nodes:
        if node.node_id not in node_to_virtual_cluster:
            node_to_virtual_cluster[node.node_id] = kPrimaryClusterID

    @ray.remote
    class ControlActor:
        def __init__(self):
            self._nodes = set()
            self._ready = False

        def ready(self):
            self._ready = True

        def is_ready(self):
            return self._ready

        def add_node(self, node_id):
            self._nodes.add(node_id)

        def nodes(self):
            return self._nodes

    for i in range(ntemplates + 1):
        actor_name = f"test_actors_{i}"
        pg_name = f"test_pgs_{i}"
        control_actor_name = f"control_{i}"
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        if i == ntemplates:
            virtual_cluster_id = kPrimaryClusterID
        control_actor = ControlActor.options(
            name=control_actor_name, namespace="control"
        ).remote()
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)
            driver_script = """
import ray
import time
import asyncio

ray.init(address="auto")

control = ray.get_actor(name="{control_actor_name}", namespace="control")


@ray.remote(max_restarts=10)
class Actor:
    def __init__(self, control, pg):
        node_id = ray.get_runtime_context().get_node_id()
        ray.get(control.add_node.remote(node_id))
        self._pg = pg

    async def run(self, control):
        node_id = ray.get_runtime_context().get_node_id()
        await control.add_node.remote(node_id)

        while True:
            node_id = ray.util.placement_group_table(self._pg)["bundles_to_node_id"][0]
            if node_id == "":
                await asyncio.sleep(1)
                continue
            break

        await control.add_node.remote(node_id)

        await control.ready.remote()
        while True:
            await asyncio.sleep(1)

    async def get_node_id(self):
        while True:
            node_id = ray.util.placement_group_table(pg)["bundles_to_node_id"][0]
            if node_id == "":
                await asyncio.sleep(1)
                continue
            break
        return (ray.get_runtime_context().get_node_id(), node_id)


pg = ray.util.placement_group(
    bundles=[{{"CPU": 1}}], name="{pg_name}", lifetime="detached"
)


@ray.remote
def hello(control):
    node_id = ray.get_runtime_context().get_node_id()
    ray.get(control.add_node.remote(node_id))


ray.get(hello.remote(control))
a = Actor.options(name="{actor_name}",
                  namespace="control",
                  num_cpus=1,
                  lifetime="detached").remote(
    control, pg
)
ray.get(a.run.remote(control))
            """
            driver_script = driver_script.format(
                actor_name=actor_name,
                pg_name=pg_name,
                control_actor_name=control_actor_name,
            )
            test_script_file = path / "test_script.py"
            with open(test_script_file, "w+") as file:
                file.write(driver_script)

            runtime_env = {"working_dir": tmp_dir}
            runtime_env = upload_working_dir_if_needed(
                runtime_env, tmp_dir, logger=logger
            )
            runtime_env = RuntimeEnv(**runtime_env).to_dict()

            job_id = head_client.submit_job(
                entrypoint="python test_script.py",
                entrypoint_memory=1,
                runtime_env=runtime_env,
                virtual_cluster_id=virtual_cluster_id,
                replica_sets={TEMPLATE_ID_PREFIX + str(i): 2},
            )

            def _check_ready(control_actor):
                return ray.get(control_actor.is_ready.remote())

            wait_for_condition(partial(_check_ready, control_actor), timeout=20)

            def _check_virtual_cluster(
                control_actor, node_to_virtual_cluster, virtual_cluster_id
            ):
                nodes = ray.get(control_actor.nodes.remote())
                assert len(nodes) > 0
                for node in nodes:
                    assert node_to_virtual_cluster[node] == virtual_cluster_id
                return True

            wait_for_condition(
                partial(
                    _check_virtual_cluster,
                    control_actor,
                    node_to_virtual_cluster,
                    virtual_cluster_id,
                ),
                timeout=20,
            )

            supervisor_actor = ray.get_actor(
                name=JOB_ACTOR_NAME_TEMPLATE.format(job_id=job_id),
                namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
            )
            actor_info = ray.state.actors(supervisor_actor._actor_id.hex())
            driver_node_id = actor_info["Address"]["NodeID"]
            assert node_to_virtual_cluster[driver_node_id] == virtual_cluster_id

            job_info = head_client.get_job_info(job_id)
            assert (
                node_to_virtual_cluster[job_info.driver_node_id] == virtual_cluster_id
            )

            nodes_to_remove = ray.get(control_actor.nodes.remote())
            if driver_node_id in nodes_to_remove:
                nodes_to_remove.remove(driver_node_id)

            to_remove = []
            for node in cluster.worker_nodes:
                if node.node_id in nodes_to_remove:
                    to_remove.append(node)
            for node in to_remove:
                cluster.remove_node(node)

            def _check_recover(
                nodes_to_remove, actor_name, node_to_virtual_cluster, virtual_cluster_id
            ):
                actor = ray.get_actor(actor_name, namespace="control")
                nodes = ray.get(actor.get_node_id.remote())
                for node_id in nodes:
                    assert node_id not in nodes_to_remove
                    assert node_to_virtual_cluster[node_id] == virtual_cluster_id
                return True

            wait_for_condition(
                partial(
                    _check_recover,
                    nodes_to_remove,
                    actor_name,
                    node_to_virtual_cluster,
                    virtual_cluster_id,
                ),
                timeout=120,
            )
            head_client.stop_job(job_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
