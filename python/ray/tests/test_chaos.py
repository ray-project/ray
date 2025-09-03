import random
import sys
import time

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    RayletKiller,
    WorkerKillerActor,
    get_and_run_resource_killer,
    get_log_message,
)
from ray.cluster_utils import AutoscalingCluster
from ray.exceptions import ObjectLostError, RayTaskError
from ray.experimental import shuffle
from ray.tests.conftest import _ray_start_chaos_cluster
from ray.util.placement_group import placement_group
from ray.util.state.api import StateApiClient, list_nodes
from ray.util.state.common import ListApiOptions, StateResource


def assert_no_system_failure(p, timeout):
    # Get all logs for 20 seconds.
    logs = get_log_message(p, timeout=timeout)
    for log in logs:
        assert "SIG" not in log, "There's the segfault or SIGBART reported."
        assert "Check failed" not in log, "There's the check failure reported."


@pytest.fixture
def set_kill_interval(request):
    lineage_reconstruction_enabled, kill_interval = request.param

    request.param = {
        "_system_config": {
            "lineage_pinning_enabled": lineage_reconstruction_enabled,
            "max_direct_call_object_size": 1000,
        },
        "kill_interval": kill_interval,
        "head_resources": {
            "CPU": 0,
        },
        "worker_node_types": {
            "cpu_node": {
                "resources": {
                    "CPU": 2,
                },
                "node_config": {
                    "object_store_memory": int(200e6),
                },
                "min_workers": 0,
                "max_workers": 3,
            },
        },
    }
    cluster_fixture = _ray_start_chaos_cluster(request)
    for x in cluster_fixture:
        yield (lineage_reconstruction_enabled, kill_interval, cluster_fixture)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "set_kill_interval",
    [(True, None), (True, 20), (False, None), (False, 20)],
    indirect=True,
)
def test_chaos_actor_retry(set_kill_interval):
    # Chaos testing.
    @ray.remote(num_cpus=0.25, max_restarts=-1, max_task_retries=-1)
    class Actor:
        def __init__(self):
            self.letter_dict = set()

        def add(self, letter):
            self.letter_dict.add(letter)

    NUM_CPUS = 16
    TOTAL_TASKS = 300

    actors = [Actor.remote() for _ in range(NUM_CPUS)]
    results = []
    for a in actors:
        results.extend([a.add.remote(str(i)) for i in range(TOTAL_TASKS)])

    start = time.time()
    ray.get(results)
    runtime_with_failure = time.time() - start
    print(f"Runtime when there are many failures: {runtime_with_failure}")

    # TODO(sang): Currently, there are lots of SIGBART with
    # plasma client failures. Fix it.
    # assert_no_system_failure(p, 10)


def test_chaos_defer(monkeypatch, ray_start_cluster):
    with monkeypatch.context() as m:
        # defer for 3s
        m.setenv(
            "RAY_testing_asio_delay_us",
            "NodeManagerService.grpc_client.PrepareBundleResources=2000000:2000000",
        )
        m.setenv("RAY_event_stats", "true")
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=1, object_store_memory=1e9)
        cluster.wait_for_nodes()
        ray.init(address="auto")  # this will connect to gpu nodes
        cluster.add_node(num_cpus=0, num_gpus=1)
        bundle = [{"GPU": 1}, {"CPU": 1}]
        pg = placement_group(bundle)
        # PG will not be ready within 3s
        with pytest.raises(ray.exceptions.GetTimeoutError):
            ray.get(pg.ready(), timeout=1)
        # it'll be ready eventually
        ray.get(pg.ready())


@ray.remote(num_cpus=0)
class ShuffleStatusTracker:
    def __init__(self):
        self.num_map = 0
        self.num_reduce = 0
        self.map_refs = []
        self.reduce_refs = []

    def register_objectrefs(self, map_refs, reduce_refs):
        self.map_refs = map_refs
        self.reduce_refs = reduce_refs

    def get_progress(self):
        if self.map_refs:
            ready, self.map_refs = ray.wait(
                self.map_refs,
                timeout=1,
                num_returns=len(self.map_refs),
                fetch_local=False,
            )
            if ready:
                print("Still waiting on map refs", self.map_refs)
            self.num_map += len(ready)
        elif self.reduce_refs:
            ready, self.reduce_refs = ray.wait(
                self.reduce_refs,
                timeout=1,
                num_returns=len(self.reduce_refs),
                fetch_local=False,
            )
            if ready:
                print("Still waiting on reduce refs", self.reduce_refs)
            self.num_reduce += len(ready)
        return self.num_map, self.num_reduce


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "set_kill_interval", [(False, None), (False, 60)], indirect=True
)
def test_nonstreaming_shuffle(set_kill_interval):
    lineage_reconstruction_enabled, kill_interval, _ = set_kill_interval
    try:
        # Create our own tracker so that it gets scheduled onto the head node.
        tracker = ShuffleStatusTracker.remote()
        ray.get(tracker.get_progress.remote())
        assert len(ray.nodes()) == 1, (
            "Tracker actor may have been scheduled to remote node "
            "and may get killed during the test"
        )

        shuffle.run(
            ray_address="auto",
            no_streaming=True,
            num_partitions=200,
            partition_size=1e6,
            tracker=tracker,
        )
    except (RayTaskError, ObjectLostError):
        assert kill_interval is not None
        assert not lineage_reconstruction_enabled


@pytest.mark.skip(reason="https://github.com/ray-project/ray/issues/20713")
@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize(
    "set_kill_interval",
    [(True, None), (True, 60), (False, None), (False, 60)],
    indirect=True,
)
def test_streaming_shuffle(set_kill_interval):
    lineage_reconstruction_enabled, kill_interval, _ = set_kill_interval
    try:
        # Create our own tracker so that it gets scheduled onto the head node.
        tracker = ShuffleStatusTracker.remote()
        ray.get(tracker.get_progress.remote())
        assert len(ray.nodes()) == 1, (
            "Tracker actor may have been scheduled to remote node "
            "and may get killed during the test"
        )

        shuffle.run(
            ray_address="auto",
            no_streaming=False,
            num_partitions=200,
            partition_size=1e6,
            tracker=tracker,
        )
    except (RayTaskError, ObjectLostError):
        assert kill_interval is not None

        # TODO(swang): Enable this once we implement support ray.put.
        # assert not lineage_reconstruction_enabled


def test_worker_killer():
    ray.init()

    task_name = "worker_to_kill"

    @ray.remote
    def worker_to_kill():
        time.sleep(3)

    # Run WorkerKillerActor to kill 3 tasks, and run remote "worker_to_kill"
    # task with max_retries=3. 4 tasks in total (1 initial + 3 retries).
    # First 3 tasks will be killed, the last retry will succeed.
    worker_killer = get_and_run_resource_killer(
        WorkerKillerActor,
        1,
        max_to_kill=3,
        kill_filter_fn=lambda: lambda task: task.name == task_name,
    )
    worker_to_kill.options(name=task_name, max_retries=3).remote()

    def check():
        tasks = StateApiClient().list(
            StateResource.TASKS,
            options=ListApiOptions(filters=[("name", "=", task_name)]),
            raise_on_missing_output=False,
        )
        failed = 0
        finished = 0
        for task in tasks:
            if task.state == "FAILED":
                failed += 1
            elif task.state == "FINISHED":
                finished += 1
        return failed == 3 and finished == 1

    wait_for_condition(check, timeout=20)

    killed_tasks = ray.get(worker_killer.get_total_killed.remote())
    assert len(killed_tasks) == 3

    tasks = StateApiClient().list(
        StateResource.TASKS,
        options=ListApiOptions(filters=[("name", "=", task_name)]),
        raise_on_missing_output=False,
    )
    for task in tasks:
        if task.state == "FAILED":
            assert (task.task_id, task.worker_pid) in killed_tasks

    ray.shutdown()


@pytest.mark.parametrize(
    "autoscaler_v2",
    [False, True],
    ids=["v1", "v2"],
)
def test_node_killer_filter(autoscaler_v2):
    # Initialize cluster with 1 head node and 2 worker nodes.
    try:
        cluster = AutoscalingCluster(
            head_resources={"CPU": 0},
            worker_node_types={
                "cpu_node": {
                    "resources": {
                        "CPU": 1,
                    },
                    "node_config": {},
                    "min_workers": 2,
                    "max_workers": 2,
                },
            },
            autoscaler_v2=autoscaler_v2,
            idle_timeout_minutes=999,  # it could idle killed before the killer.
        )
        cluster.start()
        ray.init()

        wait_for_condition(lambda: len(list_nodes()) > 2)

        # Choose random worker node to kill.
        worker_nodes = [node for node in list_nodes() if not node["is_head_node"]]
        node_to_kill = random.choice(worker_nodes)
        node_killer = get_and_run_resource_killer(
            RayletKiller,
            1,
            max_to_kill=1,
            kill_filter_fn=lambda: lambda node: node["NodeID"] == node_to_kill.node_id,
        )

        def check_killed():
            # Check that killed node is consistent across list_nodes()
            killed = list(ray.get(node_killer.get_total_killed.remote()))
            dead = [node.node_id for node in list_nodes() if node.state == "DEAD"]
            if len(killed) != 1 or len(dead) != 1:
                return False
            return killed[0] == dead[0] == node_to_kill.node_id

        wait_for_condition(check_killed, timeout=100)
    finally:
        cluster.shutdown()
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
