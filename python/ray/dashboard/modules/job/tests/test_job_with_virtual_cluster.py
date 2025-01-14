import logging
import sys
import tempfile
from functools import partial
from pathlib import Path

import pytest
import pytest_asyncio

import ray
from ray._private.gcs_utils import GcsChannel
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray._private.utils import hex_to_binary
from ray._raylet import PlacementGroupID
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_service_pb2 import CreateOrUpdateVirtualClusterRequest
from ray.dashboard.modules.job.common import (
    JOB_ACTOR_NAME_TEMPLATE,
    SUPERVISOR_ACTOR_RAY_NAMESPACE,
    JobStatus,
)
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobSubmissionClient
from ray.runtime_env.runtime_env import RuntimeEnv
from ray.tests.conftest import _ray_start_virtual_cluster
from ray.util.placement_group import PlacementGroup

TEMPLATE_ID_PREFIX = "template_id_"
kPrimaryClusterID = "kPrimaryClusterID"

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture
def job_sdk_client(request):
    param = getattr(request, "param", {})
    ntemplates = param["ntemplates"]
    with _ray_start_virtual_cluster(
        do_init=True,
        num_cpus=20,
        num_nodes=4 * ntemplates + 1,
        template_id_prefix=TEMPLATE_ID_PREFIX,
        **param,
    ) as res:
        ip, _ = res.webui_url.split(":")
        for node in res.worker_nodes:
            agent_address = f"{ip}:{node._dashboard_agent_listen_port}"
            assert wait_until_server_available(agent_address)
        head_address = res.webui_url
        assert wait_until_server_available(head_address)
        res.wait_for_nodes()
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
            "ntemplates": 2,
        },
        {
            "_system_config": {"gcs_actor_scheduling_enabled": True},
            "ntemplates": 2,
        },
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_indivisible_virtual_cluster(job_sdk_client):
    head_client, gcs_address, cluster = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    node_to_virtual_cluster = {}
    ntemplates = 2
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
        node_id = ray.util.placement_group_table(self._pg)["bundles_to_node_id"][0]
        await control.add_node.remote(node_id)
        await control.ready.remote()
        while True:
            await asyncio.sleep(1)

    async def get_node_id(self):
        node_id = ray.util.placement_group_table(pg)["bundles_to_node_id"][0]
        return (ray.get_runtime_context().get_node_id(), node_id)


pg = ray.util.placement_group(
    bundles=[{{"CPU": 1}}], name="{pg_name}", lifetime="detached"
)
ray.get(pg.ready())


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
            "ntemplates": 2,
        },
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_divisible_virtual_cluster(job_sdk_client):
    head_client, gcs_address, cluster = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    node_to_virtual_cluster = {}
    ntemplates = 1
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
        node_id = ray.util.placement_group_table(self._pg)["bundles_to_node_id"][0]
        await control.add_node.remote(node_id)
        await control.ready.remote()
        while True:
            await asyncio.sleep(1)

    async def get_node_id(self):
        node_id = ray.util.placement_group_table(pg)["bundles_to_node_id"][0]
        return (ray.get_runtime_context().get_node_id(), node_id)


pg = ray.util.placement_group(
    bundles=[{{"CPU": 1}}], name="{pg_name}", lifetime="detached"
)
ray.get(pg.ready())


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


@pytest.mark.parametrize(
    "job_sdk_client",
    [
        {
            "ntemplates": 2,
        },
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_job_access_cluster_data(job_sdk_client):
    head_client, gcs_address, cluster = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    node_to_virtual_cluster = {}

    @ray.remote
    class StorageActor:
        def __init__(self):
            self._nodes = set()
            self._ready = False
            self._driver_info = {}
            self._actor_info = {}
            self._normal_task_info = {}

        def ready(self):
            self._ready = True

        def is_ready(self):
            return self._ready

        def get_info(self):
            return {
                "driver": self._driver_info,
                "actor": self._actor_info,
                "normal_task": self._normal_task_info,
            }

        def set_driver_info(self, key, value):
            self._driver_info[key] = value

        def set_actor_info(self, key, value):
            self._actor_info[key] = value

        def set_normal_task_info(self, key, value):
            self._normal_task_info[key] = value

    ntemplates = 2
    for i in range(ntemplates):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        nodes = await create_virtual_cluster(
            gcs_address, virtual_cluster_id, {TEMPLATE_ID_PREFIX + str(i): 3}
        )
        for node_id in nodes:
            assert node_id not in node_to_virtual_cluster
            node_to_virtual_cluster[node_id] = virtual_cluster_id

    for i in range(ntemplates):
        storage_actor_name = f"storage_{i}"
        if i == ntemplates:
            virtual_cluster_id = kPrimaryClusterID
        storage_actor = StorageActor.options(
            name=storage_actor_name, namespace="storage", num_cpus=0
        ).remote()

        assert not ray.get(storage_actor.is_ready.remote())
        resource_accessor_name = f"accessor_{i}"
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)
            driver_script = """
import ray
import os


ray.init(address="auto")
storage = ray.get_actor(name="{storage_actor_name}", namespace="storage")


@ray.remote
def access_nodes():
    return ray.nodes()


@ray.remote
def access_cluster_resources():
    return ray.cluster_resources()


@ray.remote
def access_available_resources():
    return ray.available_resources()


@ray.remote
class ResourceAccessor:
    def is_ready(self):
        return True

    def nodes(self):
        self._nodes = ray.nodes()
        return self._nodes

    def total_cluster_resources(self):
        self._total_cluster_resources = ray.cluster_resources()
        return self._total_cluster_resources

    def available_resources(self):
        self._available_resources = ray.available_resources()
        return self._available_resources


accessor = ResourceAccessor.options(
    name="{resource_accessor_name}", namespace="storage", num_cpus=0
).remote()
ray.get(accessor.is_ready.remote())

ray.get(storage.ready.remote())

driver_nodes = ray.nodes()
driver_cluster_resources = ray.cluster_resources()
driver_available_resources = ray.available_resources()
ray.get(storage.set_driver_info.remote("nodes", driver_nodes))
ray.get(storage.set_driver_info.remote("cluster_resources", driver_cluster_resources))
ray.get(
    storage.set_driver_info.remote("available_resources", driver_available_resources)
)

actor_nodes = ray.get(accessor.nodes.remote())
actor_cluster_resources = ray.get(accessor.total_cluster_resources.remote())
actor_available_resources = ray.get(accessor.available_resources.remote())
ray.get(storage.set_actor_info.remote("nodes", actor_nodes))
ray.get(storage.set_actor_info.remote("cluster_resources", actor_cluster_resources))
ray.get(storage.set_actor_info.remote("available_resources", actor_available_resources))

normal_task_nodes = ray.get(access_nodes.options(num_cpus=0).remote())
normal_task_cluster_resources = ray.get(
    access_cluster_resources.options(num_cpus=0).remote()
)
normal_task_available_resources = ray.get(
    access_available_resources.options(num_cpus=0).remote()
)
ray.get(storage.set_normal_task_info.remote("nodes", normal_task_nodes))
ray.get(
    storage.set_normal_task_info.remote(
        "cluster_resources", normal_task_cluster_resources
    )
)
ray.get(
    storage.set_normal_task_info.remote(
        "available_resources", normal_task_available_resources
    )
)
            """
            driver_script = driver_script.format(
                resource_accessor_name=resource_accessor_name,
                storage_actor_name=storage_actor_name,
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

            wait_for_condition(
                partial(
                    lambda storage_actor: ray.get(storage_actor.is_ready.remote()),
                    storage_actor,
                ),
                timeout=20,
            )

            def _check_only_access_virtual_cluster_nodes(
                storage_actor, node_to_virtual_cluster, virtual_cluster_id
            ):
                cluster_info = ray.get(storage_actor.get_info.remote())
                expect_nodes = ray.nodes(virtual_cluster_id)
                expect_total_cluster_resources = ray.cluster_resources(
                    virtual_cluster_id
                )
                expect_available_resources = ray.available_resources(virtual_cluster_id)

                assert len(cluster_info) > 0
                assert cluster_info["driver"]["nodes"] == expect_nodes
                assert (
                    cluster_info["driver"]["cluster_resources"]["CPU"]
                    == expect_total_cluster_resources["CPU"]
                )
                assert (
                    cluster_info["driver"]["available_resources"]["CPU"]
                    == expect_available_resources["CPU"]
                )
                assert cluster_info["actor"]["nodes"] == expect_nodes
                assert (
                    cluster_info["actor"]["cluster_resources"]["CPU"]
                    == expect_total_cluster_resources["CPU"]
                )
                assert (
                    cluster_info["actor"]["available_resources"]["CPU"]
                    == expect_available_resources["CPU"]
                )
                assert cluster_info["normal_task"]["nodes"] == expect_nodes
                assert (
                    cluster_info["normal_task"]["cluster_resources"]["CPU"]
                    == expect_total_cluster_resources["CPU"]
                )
                assert (
                    cluster_info["normal_task"]["available_resources"]["CPU"]
                    == expect_available_resources["CPU"]
                )

                for node in cluster_info["driver"]["nodes"]:
                    node_id = node["NodeID"]
                    assert node_to_virtual_cluster[node_id] == virtual_cluster_id
                for node in cluster_info["actor"]["nodes"]:
                    node_id = node["NodeID"]
                    assert node_to_virtual_cluster[node_id] == virtual_cluster_id
                for node in cluster_info["normal_task"]["nodes"]:
                    node_id = node["NodeID"]
                    assert node_to_virtual_cluster[node_id] == virtual_cluster_id
                return True

            wait_for_condition(
                partial(
                    _check_only_access_virtual_cluster_nodes,
                    storage_actor,
                    node_to_virtual_cluster,
                    virtual_cluster_id,
                ),
                timeout=20,
            )
            head_client.stop_job(job_id)


@pytest.mark.parametrize(
    "job_sdk_client",
    [
        {
            "ntemplates": 2,
        },
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_list_nodes(job_sdk_client):
    head_client, gcs_address, cluster = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    node_to_virtual_cluster = {}
    ntemplates = 2
    for i in range(ntemplates):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        nodes = await create_virtual_cluster(
            gcs_address, virtual_cluster_id, {TEMPLATE_ID_PREFIX + str(i): 3}
        )
        for node_id in nodes:
            assert node_id not in node_to_virtual_cluster
            node_to_virtual_cluster[node_id] = virtual_cluster_id

    for i in range(ntemplates):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        cluster_nodes = ray.nodes(virtual_cluster_id=virtual_cluster_id_prefix + str(i))
        for node in cluster_nodes:
            assert node["NodeID"] in node_to_virtual_cluster
            assert node_to_virtual_cluster[node["NodeID"]] == virtual_cluster_id

    assert len(ray.nodes()) == 9
    assert len(ray.nodes("")) == 9
    assert len(ray.nodes(None)) == 9

    for i in range(ntemplates):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        assert len(ray.nodes(virtual_cluster_id)) == 3

    assert len(ray.nodes("FAKE")) == 0
    with pytest.raises(TypeError):
        ray.nodes(1)


@pytest.mark.parametrize(
    "job_sdk_client",
    [
        {
            "ntemplates": 2,
        },
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_list_cluster_resources(job_sdk_client):
    head_client, gcs_address, cluster = job_sdk_client
    virtual_cluster_id_prefix = "VIRTUAL_CLUSTER_"
    node_to_virtual_cluster = {}
    ntemplates = 2
    for i in range(ntemplates):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        nodes = await create_virtual_cluster(
            gcs_address, virtual_cluster_id, {TEMPLATE_ID_PREFIX + str(i): 3}
        )
        for node_id in nodes:
            assert node_id not in node_to_virtual_cluster
            node_to_virtual_cluster[node_id] = virtual_cluster_id

    total_resources = ray.cluster_resources()
    assert len(total_resources) > 0, f"total_resources {total_resources} is empty"
    assert total_resources["CPU"] > 0
    for i in range(ntemplates):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        virtual_cluster_resources = ray.cluster_resources(
            virtual_cluster_id=virtual_cluster_id_prefix + str(i)
        )
        assert int(virtual_cluster_resources["CPU"]) == 60
    assert len(ray.cluster_resources("NON_EXIST_VIRTUAL_CLUSTER")) == 0
    with pytest.raises(TypeError):
        ray.cluster_resources(1)

    available_resources = ray.available_resources()
    assert (
        len(available_resources) > 0
    ), f"available_resources {available_resources} is empty"
    assert available_resources["CPU"] > 0
    assert available_resources["CPU"] <= total_resources["CPU"]
    assert ray.available_resources(None) == available_resources
    for i in range(ntemplates):
        virtual_cluster_id = virtual_cluster_id_prefix + str(i)
        virtual_cluster_resources = ray.available_resources(
            virtual_cluster_id=virtual_cluster_id_prefix + str(i)
        )
        assert int(virtual_cluster_resources["CPU"]) > 0
        assert int(virtual_cluster_resources["CPU"]) < total_resources["CPU"]
    assert len(ray.available_resources("NON_EXIST_VIRTUAL_CLUSTER")) == 0
    with pytest.raises(TypeError):
        ray.available_resources(1)


@pytest.mark.parametrize(
    "job_sdk_client",
    [
        {
            "_system_config": {"expired_job_clusters_gc_interval_ms": 1000},
            "ntemplates": 2,
        },
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_detached_job_cluster(job_sdk_client):
    head_client, gcs_address, cluster = job_sdk_client
    virtual_cluster_id = "VIRTUAL_CLUSTER_0"
    await create_virtual_cluster(
        gcs_address,
        virtual_cluster_id,
        {TEMPLATE_ID_PREFIX + str(0): 2},
        True,
    )
    await create_virtual_cluster(
        gcs_address,
        virtual_cluster_id + "_indivisible",
        {TEMPLATE_ID_PREFIX + str(1): 2},
        True,
    )

    actor_name = "test_actor"
    pg_name = "test_pg"
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        driver_script = """
import ray
import time
import asyncio

ray.init(address="auto")

@ray.remote(max_restarts=10)
class Actor:
    def __init__(self):
        pass

    async def run(self):
        pass

pg = ray.util.placement_group(
    bundles=[{{"CPU": 1}}], name="{pg_name}", lifetime="detached"
)

a = Actor.options(name="{actor_name}",
                  namespace="test",
                  num_cpus=1,
                  lifetime="detached").remote()
ray.get(a.run.remote())
            """
        driver_script = driver_script.format(
            actor_name=actor_name,
            pg_name=pg_name,
        )
        test_script_file = path / "test_script.py"
        with open(test_script_file, "w+") as file:
            file.write(driver_script)

        runtime_env = {"working_dir": tmp_dir}
        runtime_env = upload_working_dir_if_needed(runtime_env, tmp_dir, logger=logger)
        runtime_env = RuntimeEnv(**runtime_env).to_dict()

        def _successful_submit(head_client, runtime_env, virtual_cluster_id):
            job_id = head_client.submit_job(
                entrypoint="python test_script.py",
                entrypoint_memory=1,
                runtime_env=runtime_env,
                virtual_cluster_id=virtual_cluster_id,
                replica_sets={TEMPLATE_ID_PREFIX + str(0): 2},
            )

            def _check_job_succeeded(head_client, job_id):
                assert head_client.get_job_status(job_id) == JobStatus.SUCCEEDED
                return True

            assert head_client.get_job_status(job_id) != JobStatus.FAILED

            wait_for_condition(
                partial(
                    _check_job_succeeded,
                    head_client,
                    job_id,
                ),
                timeout=20,
            )
            head_client.stop_job(job_id)
            return True

        _successful_submit(head_client, runtime_env, virtual_cluster_id)

        def _failed_submit():
            job_id = head_client.submit_job(
                entrypoint="python test_script.py",
                entrypoint_memory=1,
                runtime_env=runtime_env,
                virtual_cluster_id=virtual_cluster_id,
                replica_sets={TEMPLATE_ID_PREFIX + str(0): 2},
            )

            def _check_job_failed(head_client, job_id):
                assert head_client.get_job_status(job_id) == JobStatus.FAILED
                assert (
                    "No enough node instances to create the job cluster"
                    in head_client.get_job_info(job_id).message
                )
                return True

            wait_for_condition(
                partial(
                    _check_job_failed,
                    head_client,
                    job_id,
                ),
                timeout=20,
            )

        _failed_submit()

        def _remove_pg():
            pgs = ray.state.state.placement_group_table()
            pg_id = None
            for id, pg in pgs.items():
                if pg["state"] == "CREATED":
                    pg_id = id
                    break
            assert pg_id is not None

            pg = PlacementGroup(PlacementGroupID(hex_to_binary(pg_id)))
            ray.util.remove_placement_group(pg)

        _remove_pg()

        _failed_submit()

        actor = ray.get_actor(name=actor_name, namespace="test")
        ray.kill(actor, no_restart=False)

        _failed_submit()

        actor = ray.get_actor(name=actor_name, namespace="test")
        ray.kill(actor)

        wait_for_condition(
            partial(_successful_submit, head_client, runtime_env, virtual_cluster_id),
            timeout=50,
            retry_interval_ms=5000,
        )

        _remove_pg()

        actor = ray.get_actor(name=actor_name, namespace="test")
        ray.kill(actor)

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)
            driver_script += """
ray.kill(a)
ray.util.remove_placement_group(pg)
            """
            test_script_file = path / "test_script.py"
            with open(test_script_file, "w") as file:
                file.write(driver_script)

            runtime_env = {"working_dir": tmp_dir}
            runtime_env = upload_working_dir_if_needed(
                runtime_env, tmp_dir, logger=logger
            )
            runtime_env = RuntimeEnv(**runtime_env).to_dict()

            wait_for_condition(
                partial(
                    _successful_submit, head_client, runtime_env, virtual_cluster_id
                ),
                timeout=50,
                retry_interval_ms=5000,
            )
            # Test that the job submission is successful if detached is
            # early killed in job.
            wait_for_condition(
                partial(
                    _successful_submit, head_client, runtime_env, virtual_cluster_id
                ),
                timeout=50,
                retry_interval_ms=5000,
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
