import os
import signal
import sys
import uuid
from collections import Counter
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

import ray
import ray._private.ray_constants as ray_constants
import ray.dashboard.consts as dashboard_consts
from ray._common.network_utils import find_free_port
from ray._common.test_utils import (
    async_wait_for_condition,
    run_string_as_driver,
    wait_for_condition,
)
from ray._private.grpc_utils import init_grpc_channel
from ray._private.test_utils import wait_for_aggregator_agent_if_enabled
from ray._raylet import ActorID, GcsClient, JobID, TaskID
from ray.core.generated.common_pb2 import WorkerType
from ray.core.generated.gcs_service_pb2 import (
    FilterPredicate,
    GetAllActorInfoReply,
    GetAllPlacementGroupReply,
    GetAllWorkerInfoReply,
    GetTaskEventsReply,
)
from ray.core.generated.gcs_service_pb2_grpc import TaskInfoGcsServiceStub
from ray.core.generated.node_manager_pb2 import GetObjectsInfoReply
from ray.core.generated.reporter_pb2 import ListLogsReply, StreamLogReply
from ray.core.generated.runtime_env_agent_pb2 import GetRuntimeEnvsInfoReply
from ray.dashboard.modules.job.pydantic_models import JobDetails
from ray.job_submission import JobSubmissionClient
from ray.util.state import (
    get_actor,
    get_job,
    get_node,
    get_objects,
    get_placement_group,
    get_task,
    get_worker,
    list_actors,
    list_cluster_events,
    list_jobs,
    list_nodes,
    list_objects,
    list_placement_groups,
    list_runtime_envs,
    list_tasks,
    list_workers,
)
from ray.util.state.common import GetApiOptions, ListApiOptions
from ray.util.state.exception import DataSourceUnavailable
from ray.util.state.state_cli import ray_get
from ray.util.state.state_manager import StateDataSourceClient


def state_source_client(gcs_address):
    GRPC_CHANNEL_OPTIONS = (
        *ray_constants.GLOBAL_GRPC_OPTIONS,
        ("grpc.max_send_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", ray_constants.GRPC_CPP_MAX_MESSAGE_SIZE),
    )
    gcs_channel = init_grpc_channel(
        gcs_address, GRPC_CHANNEL_OPTIONS, asynchronous=True
    )
    gcs_client = GcsClient(address=gcs_address)
    client = StateDataSourceClient(gcs_channel=gcs_channel, gcs_client=gcs_client)
    return client


def is_hex(val):
    try:
        int_val = int(val, 16)
    except ValueError:
        return False
    val = val.lstrip("0")
    return f"0x{val}" == hex(int_val)


def test_list_api_options_has_conflicting_filters():
    # single filter
    options = ListApiOptions(filters=[("name", "=", "task_name")])
    assert not options.has_conflicting_filters()
    # multiple filters, different keys
    options = ListApiOptions(filters=[("name", "=", "task_name"), ("job_id", "=", "1")])
    assert not options.has_conflicting_filters()
    # multiple filters, same key, different value, not equal predicate
    options = ListApiOptions(
        filters=[("name", "!=", "task_name_1"), ("name", "!=", "task_name_2")]
    )
    assert not options.has_conflicting_filters()
    # multiple filters, same key, same value, equal predicate
    options = ListApiOptions(
        filters=[("name", "=", "task_name_1"), ("name", "=", "task_name_1")]
    )
    assert not options.has_conflicting_filters()
    # multiple filters, same key, different value, equal predicate
    options = ListApiOptions(
        filters=[("name", "=", "task_name_1"), ("name", "=", "task_name_2")]
    )
    assert options.has_conflicting_filters()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
class TestListActors:
    def test_list_get_actors(self, class_ray_instance):
        @ray.remote
        class A:
            pass

        @ray.remote(num_gpus=1)
        class UnschedulableActor:
            pass

        job_id = ray.get_runtime_context().get_job_id()
        node_id = ray.get_runtime_context().get_node_id()
        a = A.remote()
        b = UnschedulableActor.remote()

        def verify():
            # Test list
            actors = list_actors(filters=[("actor_id", "=", a._actor_id.hex())])
            assert len(actors) == 1
            assert actors[0]["state"] == "ALIVE"
            assert is_hex(actors[0]["actor_id"])
            assert a._actor_id.hex() == actors[0]["actor_id"]
            assert actors[0]["job_id"] == job_id
            assert actors[0]["node_id"] == node_id

            # Test the second actor's node id is None because
            # it is not scheduled.
            actors = list_actors(filters=[("actor_id", "=", b._actor_id.hex())])
            assert actors[0]["node_id"] is None

            # Test get
            actors = list_actors(detail=True)
            for actor in actors:
                get_actor_data = get_actor(actor["actor_id"])
                assert get_actor_data is not None
                assert get_actor_data == actor

            return True

        wait_for_condition(verify)
        print(list_actors())

    def test_list_actors_namespace(self, class_ray_instance):
        """Check that list_actors returns namespaces."""

        @ray.remote
        class A:
            pass

        # generating unique names for actor namespaces as we are using a shared ray cluster for these tests
        # and the tests run twice in the same cluster
        namespace1 = "namespace_" + str(uuid.uuid4())
        namespace2 = "namespace_" + str(uuid.uuid4())

        A.options(namespace=namespace1, name="x").remote()
        A.options(namespace=namespace2, name="y").remote()

        actors = list_actors()
        namespaces = Counter([actor["ray_namespace"] for actor in actors])
        assert namespaces[namespace1] == 1
        assert namespaces[namespace2] == 1

        # Check that we can filter by namespace
        namespace1_actors = list_actors(filters=[("ray_namespace", "=", namespace1)])
        assert len(namespace1_actors) == 1
        assert namespace1_actors[0]["ray_namespace"] == namespace1


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_get_pgs(shutdown_only):
    ray.init()
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])  # noqa

    def verify():
        # Test list
        pgs = list_placement_groups()
        assert len(pgs) == 1
        assert pgs[0]["state"] == "CREATED"
        assert is_hex(pgs[0]["placement_group_id"])
        assert pg.id.hex() == pgs[0]["placement_group_id"]

        # Test get
        pgs = list_placement_groups(detail=True)
        for pg_data in pgs:
            get_pg_data = get_placement_group(pg_data["placement_group_id"])
            assert get_pg_data is not None
            assert pg_data == get_pg_data

        return True

    wait_for_condition(verify)
    print(list_placement_groups())


@pytest.mark.asyncio
async def test_get_all_node_info_with_filters(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, node_name="head_node")
    ray.init(address=cluster.address)
    cluster.add_node(
        num_cpus=0,  # no cpus to avoid scheduling failure. We are not running
        # any workloads so this should be safe.
        node_name="worker_node_1",
        dashboard_agent_listen_port=find_free_port(),
    )
    cluster.add_node(
        num_cpus=0,
        node_name="worker_node_2",
        dashboard_agent_listen_port=find_free_port(),
    )

    client = state_source_client(cluster.address)

    # Get all nodes' info
    result = {}

    async def verify_all_nodes():
        node_infos, num_filtered = await client.get_all_node_info()
        assert len(node_infos) == 3
        assert len(node_infos) + num_filtered == 3
        assert num_filtered == 0
        node_names = {node.node_name for node in node_infos.values()}
        assert node_names == {"head_node", "worker_node_1", "worker_node_2"}
        result["all_node_infos"] = node_infos
        return True

    await async_wait_for_condition(
        verify_all_nodes, timeout=ray_constants.GCS_SERVER_REQUEST_TIMEOUT_SECONDS
    )

    node_name_to_id = {
        node.node_name: node.node_id.hex() for node in result["all_node_infos"].values()
    }

    # Get a specific node using node_id filter
    head_node_id = node_name_to_id["head_node"]

    async def verify_single_node():
        node_infos, _ = await client.get_all_node_info(
            filters=[("node_id", "=", head_node_id)]
        )
        assert len(node_infos) == 1
        node_info = list(node_infos.values())[0]
        assert node_info.node_name == "head_node"
        assert node_info.node_id.hex() == head_node_id
        return True

    await async_wait_for_condition(verify_single_node)

    # Get multiple nodes using node_name filters
    async def verify_multi_node():
        node_infos, _ = await client.get_all_node_info(
            filters=[
                ("node_name", "=", "worker_node_1"),
                ("node_name", "=", "worker_node_2"),
            ]
        )
        assert len(node_infos) == 2
        returned_names = {node.node_name for node in node_infos.values()}
        assert returned_names == {"worker_node_1", "worker_node_2"}
        return True

    await async_wait_for_condition(verify_multi_node)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_get_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, node_name="head_node")
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(
        num_cpus=1,
        node_name="worker_node",
        dashboard_agent_listen_port=find_free_port(),
    )

    cluster.remove_node(worker_node)

    def verify():
        nodes = list_nodes(detail=True)
        for node in nodes:
            assert is_hex(node["node_id"])
            assert node["labels"] == {"ray.io/node-id": node["node_id"]}
            if node["node_name"] == "head_node":
                assert node["is_head_node"]
                assert node["state"] == "ALIVE"
                assert node["state_message"] is None
            else:
                assert not node["is_head_node"]
                assert node["state"] == "DEAD"
                assert node["state_message"] == "Expected termination: received SIGTERM"

        # Check with legacy API
        check_nodes = ray.nodes()
        assert len(check_nodes) == len(nodes)

        check_nodes = sorted(check_nodes, key=lambda n: n["NodeID"])
        nodes = sorted(nodes, key=lambda n: n["node_id"])

        for check_node, node in zip(check_nodes, nodes, strict=False):
            assert check_node["NodeID"] == node["node_id"]
            assert check_node["NodeName"] == node["node_name"]

        # Check the Get api
        nodes = list_nodes(detail=True)
        for node in nodes:
            get_node_data = get_node(node["node_id"])
            assert get_node_data == node
        return True

    wait_for_condition(verify)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_get_jobs(shutdown_only):
    ray.init()
    # Test submission job
    client = JobSubmissionClient(
        f"http://{ray._private.worker.global_worker.node.address_info['webui_url']}"
    )
    job_id = client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )

    def verify():
        job_data = list_jobs(detail=True)[0]
        print(job_data)
        job_id_from_api = job_data["submission_id"]
        assert job_data["status"] == "SUCCEEDED"
        assert job_id == job_id_from_api
        assert job_data["start_time"] > 0
        assert job_data["end_time"] > 0
        return True

    wait_for_condition(verify)

    # Test driver jobs
    script = """

import ray

ray.init("auto")

@ray.remote
def f():
    pass

ray.get(f.remote())
"""
    run_string_as_driver(script)

    def verify():
        jobs = list_jobs(filters=[("type", "=", "DRIVER")], detail=True)
        assert len(jobs) == 2, "1 test driver + 1 script run above"
        for driver_job in jobs:
            assert driver_job["driver_info"] is not None
            assert driver_job["start_time"] > 0

        sub_jobs = list_jobs(filters=[("type", "=", "SUBMISSION")])
        assert len(sub_jobs) == 1
        assert sub_jobs[0]["submission_id"] is not None
        return True

    wait_for_condition(verify)

    # Test GET api
    def verify():
        job = get_job(id=job_id)
        assert job["submission_id"] == job_id
        assert job["entrypoint"] == "ls"
        assert job["status"] == "SUCCEEDED"
        return True

    wait_for_condition(verify)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_get_workers(shutdown_only):
    ray.init()

    def verify():
        workers = list_workers(detail=True)
        assert is_hex(workers[0]["worker_id"])
        # +1 to take into account of drivers.
        assert len(workers) == ray.cluster_resources()["CPU"] + 1
        # End time should be 0 as it is not configured yet.
        assert workers[0]["end_time_ms"] == 0

        # Test get worker returns the same result
        workers = list_workers(detail=True)
        for worker in workers:
            got_worker = get_worker(worker["worker_id"])
            assert got_worker == worker

        return True

    wait_for_condition(verify)

    # Kill the worker
    workers = list_workers()
    os.kill(workers[-1]["pid"], signal.SIGKILL)

    def verify():
        workers = list_workers(
            detail=True,
            filters=[("is_alive", "=", "False")],
            raise_on_missing_output=False,
        )
        assert len(workers) == 1
        assert workers[0]["end_time_ms"] != 0
        return True

    wait_for_condition(verify)
    print(list_workers(detail=True))


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failed on Windows",
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_cluster_events(shutdown_only):
    ray.init()

    @ray.remote(num_gpus=1)
    def f():
        pass

    f.remote()

    def verify():
        events = list_cluster_events()
        print(events)
        assert len(events) == 1
        assert (
            "Error: No available node types can fulfill resource request"
        ) in events[0]["message"]
        return True

    wait_for_condition(verify)
    print(list_cluster_events())

    # TODO(sang): Support get_cluster_events


def test_list_get_tasks(shutdown_only):
    ray.init(num_cpus=2)
    job_id = ray.get_runtime_context().get_job_id()
    node_id = ray.get_runtime_context().get_node_id()

    @ray.remote
    def f():
        import time

        time.sleep(30)

    @ray.remote
    def g(dep):
        import time

        time.sleep(30)

    @ray.remote(num_gpus=1)
    def impossible():
        pass

    f_refs = [f.options(name=f"f_{i}").remote() for i in range(2)]  # noqa
    g_ref = g.remote(f.remote())  # noqa
    im_ref = impossible.remote()  # noqa

    def verify_task_from_objectref(task, job_id, tasks):
        assert task["job_id"] == job_id
        assert task["actor_id"] is None
        assert any(task["task_id"] == t["task_id"] for t in tasks)

    def verify():
        tasks = list_tasks()
        assert len(tasks) == 5
        for task in tasks:
            assert task["job_id"] == job_id
        for task in tasks:
            assert task["actor_id"] is None

        # Test get_task by objectRef
        for ref in f_refs:
            verify_task_from_objectref(get_task(ref), job_id, tasks)
        verify_task_from_objectref(get_task(g_ref), job_id, tasks)
        verify_task_from_objectref(get_task(im_ref), job_id, tasks)

        waiting_for_execution = len(
            list(
                filter(
                    lambda task: task["state"] == "SUBMITTED_TO_WORKER",
                    tasks,
                )
            )
        )
        assert waiting_for_execution == 0
        scheduled = len(
            list(
                filter(
                    lambda task: task["state"] == "PENDING_NODE_ASSIGNMENT",
                    tasks,
                )
            )
        )
        assert scheduled == 2
        waiting_for_dep = len(
            list(
                filter(
                    lambda task: task["state"] == "PENDING_ARGS_AVAIL",
                    tasks,
                )
            )
        )
        assert waiting_for_dep == 1
        running = len(
            list(
                filter(
                    lambda task: task["state"] == "RUNNING",
                    tasks,
                )
            )
        )
        assert running == 2

        # Test get tasks
        tasks = list_tasks(detail=True)
        for task in tasks:
            get_task_data = get_task(task["task_id"])
            assert get_task_data == task

        # Test node id.
        tasks = list_tasks(filters=[("state", "=", "PENDING_NODE_ASSIGNMENT")])
        for task in tasks:
            assert task["node_id"] is None

        tasks = list_tasks(filters=[("state", "=", "RUNNING")])
        for task in tasks:
            assert task["node_id"] == node_id

        tasks = list_tasks(filters=[("job_id", "=", job_id)])
        for task in tasks:
            assert task["job_id"] == job_id

        tasks = list_tasks(filters=[("name", "=", "f_0")], limit=1)
        assert len(tasks) == 1

        # using limit to make sure state filtering is done on the gcs side
        tasks = list_tasks(filters=[("STATE", "=", "PENDING_ARGS_AVAIL")], limit=1)
        assert len(tasks) == 1

        return True

    wait_for_condition(verify)
    print(list_tasks())


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_get_tasks_call_site(shutdown_only):
    """
    Call chain: Driver -> caller -> callee.
    Verify that the call site is captured in callee, and it contains string
    "caller".
    """
    ray.init(
        num_cpus=2,
        runtime_env={"env_vars": {"RAY_record_task_actor_creation_sites": "true"}},
    )

    @ray.remote
    def callee():
        import time

        time.sleep(30)

    @ray.remote
    def caller():
        return callee.remote()

    caller_ref = caller.remote()
    callee_ref = ray.get(caller_ref)

    def verify():
        callee_task = get_task(callee_ref)
        assert callee_task["call_site"] is not None
        assert "caller" in callee_task["call_site"]
        return True

    wait_for_condition(verify)
    print(list_tasks())


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_actor_tasks_call_site(shutdown_only):
    """
    Call chain: Driver -> create_actor -> (Actor, Actor.method).

    Verify that the call sites are captured in both Actor and Actor.method,
    and they contain string "create_actor".
    """
    ray.init(
        num_cpus=2,
        runtime_env={"env_vars": {"RAY_record_task_actor_creation_sites": "true"}},
    )

    @ray.remote
    class Actor:
        def method(self):
            import time

            time.sleep(30)

    @ray.remote
    def create_actor():
        a = Actor.remote()
        m_ref = a.method.remote()
        return a, m_ref

    actor_ref, method_ref = ray.get(create_actor.remote())

    def verify():
        method_task = get_task(method_ref)
        assert method_task["call_site"] is not None
        assert "create_actor" in method_task["call_site"]

        actors = list_actors(detail=True)
        assert len(actors) == 1
        actor = actors[0]
        assert actor["call_site"] is not None
        assert "create_actor" in actor["call_site"]
        return True

    wait_for_condition(verify)
    print(list_tasks())


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_get_tasks_label_selector(ray_start_cluster):
    """
    Call chain: Driver -> caller -> callee.
    Verify that the call site is captured in callee, and it contains string
    "caller".
    """
    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=2, labels={"ray.io/accelerator-type": "A100", "region": "us-west4"}
    )
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(
        label_selector={"region": "us-west4"},
        fallback_strategy=[{"label_selector": {"region": "us-west5"}}],
    )
    def foo():
        import time

        time.sleep(5)

    call_ref = foo.remote()

    ray.get(call_ref)

    def verify():
        task = get_task(call_ref)
        assert task["label_selector"] == {"region": "us-west4"}
        expected_fallback = {
            "options": [
                {
                    "label_selector": {
                        "label_constraints": [
                            {
                                "label_key": "region",
                                "operator": "LABEL_OPERATOR_IN",
                                "label_values": ["us-west5"],
                            }
                        ]
                    }
                }
            ]
        }
        assert task["fallback_strategy"] == expected_fallback
        return True

    wait_for_condition(verify)
    print(list_tasks())


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_actor_tasks_label_selector(ray_start_cluster):
    """
    Call chain: Driver -> create_actor -> (Actor, Actor.method).

    Verify that the call sites are captured in both Actor and Actor.method,
    and they contain string "create_actor".
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2, labels={"region": "us-west4"})
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(
        label_selector={"region": "us-west4"},
        fallback_strategy=[{"label_selector": {"region": "us-west5"}}],
    )
    class Actor:
        def method(self):
            import time

            time.sleep(5)

    actor = Actor.remote()
    ray.get(actor.method.remote())

    def verify():
        actors = list_actors(detail=True)
        assert len(actors) == 1
        actor = actors[0]
        assert actor["label_selector"] == {"region": "us-west4"}
        expected_fallback = {
            "options": [
                {
                    "label_selector": {
                        "label_constraints": [
                            {
                                "label_key": "region",
                                "operator": "LABEL_OPERATOR_IN",
                                "label_values": ["us-west5"],
                            }
                        ]
                    }
                }
            ]
        }
        assert actor["fallback_strategy"] == expected_fallback
        return True

    wait_for_condition(verify)
    print(list_actors(detail=True))


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_get_task_multiple_attempt_all_failed(shutdown_only):
    ray.init(num_cpus=2)
    job_id = ray.get_runtime_context().get_job_id()
    node_id = ray.get_runtime_context().get_node_id()

    @ray.remote(retry_exceptions=True, max_retries=2)
    def f():
        raise ValueError("f is expected to failed")

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(f.remote())

    def verify(task_attempts):
        assert len(task_attempts) == 3  # 2 retries + 1 initial run
        for task_attempt in task_attempts:
            assert task_attempt["job_id"] == job_id
            assert task_attempt["state"] == "FAILED"
            assert task_attempt["node_id"] == node_id

        assert {task_attempt["attempt_number"] for task_attempt in task_attempts} == {
            0,
            1,
            2,
        }, "Attempt number should be 0,1,2"

        assert (
            len({task_attempt["task_id"] for task_attempt in task_attempts}) == 1
        ), "Same task id"
        return True

    wait_for_condition(lambda: verify(list_tasks()))

    # Test get with task id returns multiple task attempts
    task_id = list_tasks()[0]["task_id"]
    wait_for_condition(lambda: verify(get_task(task_id)))


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_get_task_multiple_attempt_finished_after_retry(shutdown_only):
    ray.init(num_cpus=2)

    # Test success after retries.
    @ray.remote
    class Phaser:
        def __init__(self):
            self.i = 0

        def inc(self):
            self.i += 1
            if self.i < 3:
                raise ValueError(
                    f"First two tries are expected to fail (try={self.i})."
                )

    phaser = Phaser.remote()

    @ray.remote(retry_exceptions=True, max_retries=3)
    def f():
        ray.get(phaser.inc.remote())

    ray.get(f.remote())

    def verify(task_attempts):
        assert len(task_attempts) == 3
        for task_attempt in task_attempts[1:]:
            assert task_attempt["state"] == "FAILED"

        task_attempts[0]["state"] == "FINISHED"

        assert {task_attempt["attempt_number"] for task_attempt in task_attempts} == {
            0,
            1,
            2,
        }, "Attempt number should be 0,1,2"

        return True

    wait_for_condition(lambda: verify(list_tasks(filters=[("name", "=", "f")])))


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_actor_tasks(shutdown_only):
    ray_context = ray.init(num_cpus=2)
    wait_for_aggregator_agent_if_enabled(
        ray_context.address_info["address"],
        ray_context.address_info["node_id"],
    )
    job_id = ray.get_runtime_context().get_job_id()

    @ray.remote(max_concurrency=2)
    class Actor:
        def call(self):
            import time

            time.sleep(30)

    a = Actor.remote()
    actor_id = a._actor_id.hex()
    calls = [a.call.remote() for _ in range(10)]  # noqa

    def verify():
        tasks = list_tasks()
        for task in tasks:
            assert task["job_id"] == job_id
        for task in tasks:
            assert task["actor_id"] == actor_id
        # Actor.__init__: 1 finished
        # Actor.call: 2 running, 8 waiting for execution (queued).
        assert len(tasks) == 11
        assert (
            len(
                list(
                    filter(
                        lambda task: task["state"]
                        == "PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY",
                        tasks,
                    )
                )
            )
            == 8
        )
        assert (
            len(
                list(
                    filter(
                        lambda task: task["state"] == "PENDING_NODE_ASSIGNMENT",
                        tasks,
                    )
                )
            )
            == 0
        )
        assert (
            len(
                list(
                    filter(
                        lambda task: task["state"] == "PENDING_ARGS_AVAIL",
                        tasks,
                    )
                )
            )
            == 0
        )
        assert (
            len(
                list(
                    filter(
                        lambda task: task["state"] == "RUNNING",
                        tasks,
                    )
                )
            )
            == 2
        )

        # Filters with actor id.
        assert len(list_tasks(filters=[("actor_id", "=", actor_id)])) == 11
        assert len(list_tasks(filters=[("actor_id", "!=", actor_id)])) == 0

        return True

    wait_for_condition(verify)
    print(list_tasks())


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_get_objects(shutdown_only):
    ray.init()
    import numpy as np

    data = np.ones(50 * 1024 * 1024, dtype=np.uint8)
    plasma_obj = ray.put(data)

    @ray.remote
    def f(obj):
        print(obj)

    ray.get(f.remote(plasma_obj))

    def verify():
        obj = list_objects()[0]
        # For detailed output, the test is covered from `test_memstat.py`
        assert obj["object_id"] == plasma_obj.hex()

        obj = list_objects(detail=True)[0]
        got_objs = get_objects(plasma_obj.hex())
        assert len(got_objs) == 1
        assert obj == got_objs[0]

        return True

    wait_for_condition(verify)
    print(list_objects())


@pytest.mark.skipif(
    sys.platform == "win32", reason="Runtime env not working in Windows."
)
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_list_runtime_envs(shutdown_only):
    ray.init(runtime_env={"pip": ["requests"]})

    @ray.remote
    class Actor:
        def ready(self):
            pass

    a = Actor.remote()  # noqa
    b = Actor.options(runtime_env={"pip": ["nonexistent_dep"]}).remote()  # noqa
    ray.get(a.ready.remote())
    with pytest.raises(ray.exceptions.RuntimeEnvSetupError):
        ray.get(b.ready.remote())

    def verify():
        result = list_runtime_envs(detail=True)
        assert len(result) == 2

        failed_runtime_env = result[0]
        assert (
            not failed_runtime_env["success"]
            and failed_runtime_env["error"]
            and failed_runtime_env["ref_cnt"] == 0
        )

        successful_runtime_env = result[1]
        assert (
            successful_runtime_env["success"] and successful_runtime_env["ref_cnt"] == 2
        )
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_limit(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        def ready(self):
            pass

    actors = [A.remote() for _ in range(4)]
    ray.get([actor.ready.remote() for actor in actors])

    output = list_actors(limit=2)
    assert len(output) == 2

    # Make sure the output is deterministic.
    assert output == list_actors(limit=2)


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_get_id_not_found(shutdown_only):
    """Test get API CLI fails correctly when there's no corresponding id

    Related: https://github.com/ray-project/ray/issues/26808
    """
    ray.init()
    runner = CliRunner()
    id = ActorID.from_random().hex()
    result = runner.invoke(ray_get, ["actors", id])
    assert result.exit_code == 0, str(result.exception) + result.output
    assert f"Resource with id={id} not found in the cluster." in result.output


@pytest.mark.asyncio
@patch.object(
    StateDataSourceClient, "__init__", lambda self, gcs_channel, gcs_client: None
)
async def test_state_data_source_client_get_all_task_info_no_early_return():
    #  Setup
    mock_gcs_task_info_stub = AsyncMock(TaskInfoGcsServiceStub)

    client = StateDataSourceClient(None, None)
    client._gcs_task_info_stub = mock_gcs_task_info_stub

    mock_reply = MagicMock(GetTaskEventsReply)
    mock_gcs_task_info_stub.GetTaskEvents = AsyncMock()
    mock_gcs_task_info_stub.GetTaskEvents.side_effect = [mock_reply]

    test_actor_id = ActorID.from_random()
    test_job_id = JobID.from_int(1)
    test_task_id_1 = TaskID.for_fake_task(test_job_id)
    test_task_id_2 = TaskID.for_fake_task(test_job_id)
    test_task_name = "task_name"
    test_state = "running"
    input_filters = []
    input_filters.append(("actor_id", "=", test_actor_id.hex()))
    input_filters.append(("job_id", "!=", test_job_id.hex()))
    input_filters.append(("task_id", "=", test_task_id_1.hex()))
    input_filters.append(("name", "=", test_task_name))
    input_filters.append(("task_id", "!=", test_task_id_2.hex()))
    input_filters.append(("state", "=", test_state))
    input_timeout = 100
    input_limit = 200
    input_exclude_driver = True

    # Execute the function
    result = await client.get_all_task_info(
        input_timeout, input_limit, input_filters, input_exclude_driver
    )

    # Verify
    assert result is mock_reply
    mock_gcs_task_info_stub.GetTaskEvents.assert_awaited_once()

    input_args = mock_gcs_task_info_stub.GetTaskEvents.await_args
    assert len(input_args.kwargs) == 1
    assert input_args.kwargs["timeout"] == input_timeout

    assert len(input_args.args) == 1
    request_arg = input_args.args[0]
    assert request_arg.limit == input_limit

    filters_arg = request_arg.filters
    task_filters_arg = request_arg.filters.task_filters
    assert len(task_filters_arg) == 2
    if task_filters_arg[0].predicate == FilterPredicate.EQUAL:
        assert TaskID(task_filters_arg[0].task_id) == test_task_id_1
        assert task_filters_arg[1].predicate == FilterPredicate.NOT_EQUAL
        assert TaskID(task_filters_arg[1].task_id) == test_task_id_2
    else:
        assert task_filters_arg[0].task_id == test_task_id_2
        assert task_filters_arg[1].predicate == FilterPredicate.EQUAL
        assert TaskID(task_filters_arg[1].task_id) == test_task_id_1

    actor_filters_arg = request_arg.filters.actor_filters
    assert len(actor_filters_arg) == 1
    assert ActorID(actor_filters_arg[0].actor_id) == test_actor_id
    assert actor_filters_arg[0].predicate == FilterPredicate.EQUAL

    job_filters_arg = request_arg.filters.job_filters
    assert len(job_filters_arg) == 1
    assert JobID(job_filters_arg[0].job_id) == test_job_id
    assert job_filters_arg[0].predicate == FilterPredicate.NOT_EQUAL

    task_name_filters_arg = request_arg.filters.task_name_filters
    assert len(task_name_filters_arg) == 1
    assert task_name_filters_arg[0].task_name == test_task_name
    assert task_name_filters_arg[0].predicate == FilterPredicate.EQUAL

    state_filters_arg = request_arg.filters.state_filters
    assert len(state_filters_arg) == 1
    assert state_filters_arg[0].state == test_state
    assert state_filters_arg[0].predicate == FilterPredicate.EQUAL

    assert filters_arg.exclude_driver == input_exclude_driver


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
async def test_state_data_source_client(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=2, dashboard_agent_listen_port=find_free_port())
    ray.init(address=cluster.address)
    # worker
    worker = cluster.add_node(num_cpus=2, dashboard_agent_listen_port=find_free_port())

    client = state_source_client(cluster.address)

    """
    Test actor
    """
    result = await client.get_all_actor_info()
    assert isinstance(result, GetAllActorInfoReply)

    """
    Test placement group
    """
    result = await client.get_all_placement_group_info()
    assert isinstance(result, GetAllPlacementGroupReply)

    """
    Test node
    """
    result = await client.get_all_node_info()
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], dict)  # node_infos
    assert isinstance(result[1], int)  # num_filtered

    """
    Test worker info
    """
    result = await client.get_all_worker_info()
    assert isinstance(result, GetAllWorkerInfoReply)

    """
    Test job
    """
    job_client = JobSubmissionClient(
        f"http://{ray._private.worker.global_worker.node.address_info['webui_url']}"
    )
    job_id = job_client.submit_job(  # noqa
        # Entrypoint shell command to execute
        entrypoint="ls",
    )
    result = await client.get_job_info()
    assert isinstance(result[0], JobDetails)
    found_job = False
    for job in result:
        if job.type != "DRIVER":
            assert job.submission_id == job_id
            found_job = True
    assert found_job, result
    assert isinstance(result, list)

    """
    Test tasks
    """

    wait_for_condition(lambda: len(ray.nodes()) == 2)

    """
    Test objects
    """
    wait_for_condition(lambda: len(ray.nodes()) == 2)
    for node in ray.nodes():
        ip = node["NodeManagerAddress"]
        port = node["NodeManagerPort"]
        result = await client.get_object_info(ip, port)
        assert isinstance(result, GetObjectsInfoReply)

    """
    Test runtime env
    """
    wait_for_condition(lambda: len(ray.nodes()) == 2)
    for node in ray.nodes():
        node_id = node["NodeID"]
        key = f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id}"

        def get_addr():
            return ray.experimental.internal_kv._internal_kv_get(
                key, namespace=ray_constants.KV_NAMESPACE_DASHBOARD
            )

        wait_for_condition(lambda: get_addr() is not None)
        result = await client.get_runtime_envs_info(
            node["NodeManagerAddress"], node["RuntimeEnvAgentPort"]
        )
        assert isinstance(result, GetRuntimeEnvsInfoReply)

    """
    Test logs
    """
    with pytest.raises(ValueError):
        result = await client.list_logs("1234", "*")
    with pytest.raises(ValueError):
        result = await client.stream_log("1234", "raylet.out", True, 100, 1, 5)

    wait_for_condition(lambda: len(ray.nodes()) == 2)
    # The node information should've been registered in the previous section.
    for node in ray.nodes():
        node_id = node["NodeID"]
        result = await client.list_logs(node_id, timeout=30, glob_filter="*")
        assert isinstance(result, ListLogsReply)

        stream = await client.stream_log(node_id, "raylet.out", False, 10, 1, 5)
        async for logs in stream:
            log_lines = len(logs.data.decode().split("\n"))
            assert isinstance(logs, StreamLogReply)
            assert log_lines >= 10
            assert log_lines <= 11

    """
    Test the exception is raised when the RPC error occurs.
    """
    cluster.remove_node(worker)
    # Wait until the dead node information is propagated.
    wait_for_condition(
        lambda: len(list(filter(lambda node: node["Alive"], ray.nodes()))) == 1
    )
    for node in ray.nodes():
        node_id = node["NodeID"]
        if node["Alive"]:
            continue

        # Querying to the dead node raises gRPC error, which should raise an exception.
        with pytest.raises(DataSourceUnavailable):
            await client.get_object_info(
                node["NodeManagerAddress"], node["NodeManagerPort"]
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
async def test_state_data_source_client_limit_gcs_source(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    client = state_source_client(gcs_address=cluster.address)
    """
    Test actor
    """

    @ray.remote
    class Actor:
        def ready(self):
            pass

    actors = [Actor.remote() for _ in range(3)]
    for actor in actors:
        ray.get(actor.ready.remote())

    result = await client.get_all_actor_info(limit=2)
    assert len(result.actor_table_data) == 2
    assert result.total == 3

    """
    Test placement group
    """
    pgs = [ray.util.placement_group(bundles=[{"CPU": 0.001}]) for _ in range(3)]  # noqa
    result = await client.get_all_placement_group_info(limit=2)
    assert len(result.placement_group_table_data) == 2
    assert result.total == 3

    """
    Test worker info
    """
    result = await client.get_all_worker_info(limit=2)
    assert len(result.worker_table_data) == 2
    assert result.total == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
async def test_state_data_source_client_limit_distributed_sources(ray_start_cluster):
    cluster = ray_start_cluster
    # head
    cluster.add_node(num_cpus=8, dashboard_agent_listen_port=find_free_port())
    ray.init(address=cluster.address)
    client = state_source_client(cluster.address)

    [node] = ray.nodes()
    ip, port = node["NodeManagerAddress"], int(node["NodeManagerPort"])

    @ray.remote
    def long_running_task(obj):  # noqa
        objs = [ray.put(1) for _ in range(10)]  # noqa
        import time

        time.sleep(300)

    objs = [ray.put(1) for _ in range(4)]
    refs = [long_running_task.remote(obj) for obj in objs]

    async def verify():
        result = await client.get_object_info(ip, port, limit=2)
        # 4 objs (driver)
        # 4 refs (driver)
        # 4 pinned in memory for each task
        # 40 for 4 tasks * 10 objects each
        assert result.total == 52
        # Only 1 core worker stat is returned because data is truncated.
        assert len(result.core_workers_stats) == 1

        for c in result.core_workers_stats:
            # The query will be always done in the consistent ordering
            # and driver should always come first.
            assert (
                WorkerType.DESCRIPTOR.values_by_number[c.worker_type].name == "DRIVER"
            )
            assert c.objects_total == 8
            assert len(c.object_refs) == 2
        return True

    await async_wait_for_condition(verify)
    [ray.cancel(ref, force=True) for ref in refs]
    del refs

    """
    Test runtime env
    """
    for node in ray.nodes():
        node_id = node["NodeID"]
        ip = node["NodeManagerAddress"]
        runtime_env_agent_port = int(node["RuntimeEnvAgentPort"])
        key = f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id}"

        def get_addr():
            return ray.experimental.internal_kv._internal_kv_get(
                key, namespace=ray_constants.KV_NAMESPACE_DASHBOARD
            )

        wait_for_condition(lambda: get_addr() is not None)

    @ray.remote
    class Actor:
        def ready(self):
            pass

    actors = [
        Actor.options(runtime_env={"env_vars": {"index": f"{i}"}}).remote()
        for i in range(3)
    ]
    ray.get([actor.ready.remote() for actor in actors])

    result = await client.get_runtime_envs_info(ip, runtime_env_agent_port, limit=2)
    assert result.total == 3
    assert len(result.runtime_env_states) == 2


@pytest.mark.parametrize(
    "event_routing_config", ["default", "aggregator"], indirect=True
)
@pytest.mark.usefixtures("event_routing_config")
def test_get_actor_timeout_multiplier(shutdown_only):
    """Test that GetApiOptions applies the same timeout multiplier as ListApiOptions.

    This test reproduces the issue where get_actor with timeout=1 fails even though
    the actual operation takes less than 1 second, because GetApiOptions doesn't
    apply the 0.8 server timeout multiplier that ListApiOptions uses.

    Related issue: https://github.com/ray-project/ray/issues/54153
    """

    @ray.remote
    class TestActor:
        def ready(self):
            pass

    actor = TestActor.remote()
    ray.get(actor.ready.remote())

    # Test that both options classes apply the same timeout multiplier
    test_timeout = 1
    get_options = GetApiOptions(timeout=test_timeout)
    list_options = ListApiOptions(timeout=test_timeout)

    # After __post_init__, both should have the same effective timeout
    assert get_options.timeout == list_options.timeout

    # Test that get_actor works with a 1-second timeout
    actors = list_actors()
    actor_id = actors[0]["actor_id"]

    # This should work without timeout issues
    result = get_actor(actor_id, timeout=1)
    assert result["actor_id"] == actor_id


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
