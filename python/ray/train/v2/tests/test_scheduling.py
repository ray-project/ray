import asyncio
import sys
import time
from collections import defaultdict

import pytest

import ray
import ray.train
from ray._raylet import GcsClient
from ray.cluster_utils import AutoscalingCluster, Cluster
from ray.core.generated import autoscaler_pb2
from ray.data._internal.cluster_autoscaler import (
    ResourceRequestPriority,
    get_or_create_autoscaling_coordinator,
)
from ray.train import ScalingConfig
from ray.train.v2._internal.constants import WORKER_GROUP_START_TIMEOUT_S_ENV_VAR
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# Workers can't import this test module, so ship its functions by value.
ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])

MB = 1024**2


@pytest.fixture
def multi_cpu_node_cluster():
    """Yields a CPU cluster with 4 CPU nodes (4x4 CPU)."""
    cluster = Cluster()
    for _ in range(4):
        cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    cluster.connect()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture(scope="class")
def multi_cpu_node_labeled_cluster():
    """Yields a CPU cluster with 4 CPU nodes (4x4 CPU), two in each subcluster.

    Class-scoped, so the tests in `TestOnSharedCluster` start it once. A module
    scope would keep it running while the other tests start clusters of their own.
    """
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=4, labels={"subcluster": "my_subcluster"})
    for _ in range(2):
        cluster.add_node(num_cpus=4, labels={"subcluster": "other_subcluster"})
    cluster.wait_for_nodes()
    cluster.connect()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


class TestOnSharedCluster:
    """Tests that don't change the cluster, so they share one."""

    @pytest.mark.parametrize("placement_strategy", ["PACK", "SPREAD"])
    def test_placement_strategy(
        self, multi_cpu_node_labeled_cluster, tmp_path, placement_strategy
    ):
        """Tests the placement strategy of the worker group.
        Nodes in this test are virtual but still have unique `node_id`s.
        """
        scaling_config = ScalingConfig(
            num_workers=4,
            resources_per_worker={"CPU": 1},
            placement_strategy=placement_strategy,
        )

        def train_fn(config):
            node_id = ray.get_runtime_context().get_node_id()
            (tmp_path / node_id).touch()

        trainer = DataParallelTrainer(train_fn, scaling_config=scaling_config)
        trainer.fit()

        if placement_strategy == "PACK":
            assert len(list(tmp_path.iterdir())) == 1
        elif placement_strategy == "SPREAD":
            assert len(list(tmp_path.iterdir())) == 4

    @pytest.mark.parametrize(
        "label_selector, expected_subcluster_counts",
        [
            ({"subcluster": "my_subcluster"}, {"my_subcluster": 2}),
            (
                [{"subcluster": "my_subcluster"}, {"subcluster": "my_subcluster"}],
                {"my_subcluster": 2},
            ),
            (
                [{"subcluster": "my_subcluster"}, {"subcluster": "other_subcluster"}],
                {"my_subcluster": 1, "other_subcluster": 1},
            ),
        ],
    )
    def test_label_selector(
        self, multi_cpu_node_labeled_cluster, label_selector, expected_subcluster_counts
    ):
        @ray.remote
        class Counter:
            def __init__(self):
                self.subcluster_counts = defaultdict(int)

            def increment(self, subcluster):
                self.subcluster_counts[subcluster] += 1

            def get(self):
                return self.subcluster_counts

        counter = Counter.remote()
        scaling_config = ScalingConfig(
            num_workers=2,
            label_selector=label_selector,
        )

        def train_fn():
            ray.get(
                counter.increment.remote(
                    ray.get_runtime_context().get_node_labels()["subcluster"]
                )
            )

        trainer = DataParallelTrainer(train_fn, scaling_config=scaling_config)
        trainer.fit()
        assert ray.get(counter.get.remote()) == expected_subcluster_counts


@pytest.mark.parametrize("placement_strategy", ["STRICT_PACK", "STRICT_SPREAD"])
def test_infeasible_placement_strategy(
    multi_cpu_node_cluster, tmp_path, monkeypatch, placement_strategy
):
    """Infeasible requests will continue retrying until the resources are available."""

    @ray.remote(num_cpus=0)
    def run():
        monkeypatch.setenv(WORKER_GROUP_START_TIMEOUT_S_ENV_VAR, "2")
        scaling_config = ScalingConfig(
            num_workers=8,
            resources_per_worker={"CPU": 1},
            placement_strategy=placement_strategy,
        )
        trainer = DataParallelTrainer(lambda: None, scaling_config=scaling_config)
        trainer.fit()

    future = run.remote()

    ready, _ = ray.wait([future], timeout=0.5)
    assert not ready

    # Add 4 more 8-CPU nodes to the cluster to make the request feasible.
    for _ in range(4):
        multi_cpu_node_cluster.add_node(num_cpus=8)

    ray.get(future)


# The tests below check that a run starts and finishes when the
# AutoscalingCoordinator's view of the cluster differs from what the scheduler
# can use. The coordinator reserves from each node's *total* capacity, counts
# draining nodes as alive, and recomputes reservations when nodes join or leave,
# so placement has to come from the scheduler rather than from that view.


# A run that can't place its worker group retries every few seconds, so a run
# that is going to start does so well within this.
RUN_DEADLINE_S = 60


def _set_short_worker_group_start_timeout(monkeypatch):
    # Set before the cluster starts, so the driver tasks and the controller
    # inherit it and a failed placement retries quickly.
    monkeypatch.setenv(WORKER_GROUP_START_TIMEOUT_S_ENV_VAR, "5")


@pytest.fixture
def head_node_cluster(monkeypatch):
    _set_short_worker_group_start_timeout(monkeypatch)
    cluster = Cluster(initialize_head=True, head_node_args=dict(num_cpus=4))
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)
    yield cluster
    ray.shutdown()
    cluster.shutdown()


def _add_slot_node(cluster: Cluster, sort_first: bool = False):
    """Add a node with one ``slot``.

    A node with ``sort_first`` has more CPUs and object store than the others,
    so the coordinator lists it, and reserves on it, first.
    """
    node = cluster.add_node(
        num_cpus=3 if sort_first else 2,
        resources={"slot": 1},
        object_store_memory=(200 if sort_first else 150) * MB,
    )
    cluster.wait_for_nodes()
    return node


@ray.remote(num_cpus=0)
class _Gate:
    """Records where each run's workers start, and holds them until released."""

    def __init__(self, released: bool = True):
        self._released = asyncio.Event()
        if released:
            self._released.set()
        self._started_on = {}

    def started(self, run_name: str, node_id: str):
        self._started_on.setdefault(run_name, []).append(node_id)

    def started_on(self):
        return self._started_on

    def release(self):
        self._released.set()

    async def wait(self):
        await self._released.wait()


def _train_fn(config: dict):
    gate = config["gate"]
    node_id = ray.get_runtime_context().get_node_id()
    ray.get(gate.started.remote(config["run_name"], node_id))
    ray.get(gate.wait.remote())
    if config.get("fail"):
        raise RuntimeError("Injected failure.")


@ray.remote(num_cpus=0)
def _fit(train_loop_config: dict, scaling_config, storage_path: str, max_failures=0):
    trainer = DataParallelTrainer(
        _train_fn,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        run_config=ray.train.RunConfig(
            storage_path=storage_path,
            failure_config=ray.train.FailureConfig(max_failures=max_failures),
        ),
    )
    trainer.fit()


def _on_head(head_node_id: str) -> NodeAffinitySchedulingStrategy:
    return NodeAffinitySchedulingStrategy(head_node_id, soft=False)


def _submit_fit(head_node_id: str, *args, **kwargs) -> ray.ObjectRef:
    # Run the driver, and so the controller, on the head node, so the
    # scenarios can drain and remove worker nodes without killing them.
    return _fit.options(scheduling_strategy=_on_head(head_node_id)).remote(
        *args, **kwargs
    )


def _fit_one_slot_run(head_node_id: str, storage_path: str) -> str:
    """Run a one-worker, one-slot run to completion and return its node."""
    gate = _Gate.options(scheduling_strategy=_on_head(head_node_id)).remote()
    _get_with_deadline(
        _submit_fit(
            head_node_id,
            {"gate": gate, "run_name": "run"},
            _slot_scaling_config(),
            storage_path,
        )
    )
    (node_id,) = ray.get(gate.started_on.remote())["run"]
    return node_id


def _placement_diagnostics() -> str:
    lines = ["Placement groups:"]
    for pg_id, pg in ray.util.placement_group_table().items():
        lines.append(f"  {pg_id[:8]} state={pg['state']} bundles={pg['bundles']}")
    lines.append("Available resources per node:")
    for node_id, res in ray._private.state.available_resources_per_node().items():
        lines.append(f"  {node_id[:8]} {res}")
    return "\n".join(lines)


def _get_with_deadline(refs, timeout_s: float = RUN_DEADLINE_S):
    try:
        return ray.get(refs, timeout=timeout_s)
    except ray.exceptions.GetTimeoutError:
        pytest.fail(
            f"Training did not finish within {timeout_s}s.\n" + _placement_diagnostics()
        )


def _wait_for_starts(gate, expected: dict) -> dict:
    """Wait until each run in ``expected`` has started at least that many times."""
    deadline = time.monotonic() + RUN_DEADLINE_S
    while time.monotonic() < deadline:
        started_on = ray.get(gate.started_on.remote())
        if all(len(started_on.get(r, [])) >= n for r, n in expected.items()):
            return started_on
        time.sleep(0.5)
    pytest.fail(
        f"Runs didn't start as expected ({expected}): "
        f"{ray.get(gate.started_on.remote())}\n" + _placement_diagnostics()
    )


def _slot_scaling_config(**kwargs):
    return ScalingConfig(num_workers=1, resources_per_worker={"slot": 1}, **kwargs)


@ray.remote(num_cpus=0)
class _Holder:
    def ready(self):
        pass


def _start_holder(node_id: str, **resources) -> "ray.actor.ActorHandle":
    holder = _Holder.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        **resources,
    ).remote()
    ray.get(holder.ready.remote())
    return holder


def _hold_slot_outside_the_coordinator(node_id: str):
    # The actor never registers with the coordinator, so the coordinator still
    # counts the slot as free.
    return [_start_holder(node_id, resources={"slot": 1})]


def _drain(node_id: str):
    # Keep the node busy, as a real preempted node would be, so draining doesn't
    # take it down straight away. The actor leaves the node's slot free.
    holder = _start_holder(node_id, num_cpus=1)
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    is_accepted, rejection_reason = gcs_client.drain_node(
        node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption notice",
        int((time.time() + 600) * 1000),
    )
    assert is_accepted, rejection_reason
    # `ray.nodes()` still reports a draining node alive.
    return [holder]


@pytest.mark.parametrize(
    "make_unusable",
    [_hold_slot_outside_the_coordinator, _drain],
    ids=["slot_held_outside_the_coordinator", "draining"],
)
def test_starts_when_a_node_is_unusable(head_node_cluster, tmp_path, make_unusable):
    """Node X sorts first, so the coordinator reserves it, but X can't take the
    run. The run must start on node Y."""
    head_node_id = ray.get_runtime_context().get_node_id()
    x = _add_slot_node(head_node_cluster, sort_first=True)
    y = _add_slot_node(head_node_cluster)
    _holders = make_unusable(x.node_id)  # Keep the actors alive.

    assert _fit_one_slot_run(head_node_id, str(tmp_path)) == y.node_id


@pytest.mark.parametrize(
    "scaling_kwargs",
    [
        dict(num_workers=2),
        dict(num_workers=2, placement_strategy="STRICT_SPREAD"),
        dict(num_workers=(2, 3)),
    ],
    ids=["fixed", "strict_spread", "elastic"],
)
def test_waits_while_only_a_draining_node_would_make_room(
    head_node_cluster, tmp_path, scaling_kwargs
):
    """A run needs two slots. Node A drains and node B joins, so counting A
    there are two slots, but only B's is usable. The run must keep waiting, then
    start on B and C once C joins."""
    head_node_id = ray.get_runtime_context().get_node_id()
    a = _add_slot_node(head_node_cluster)
    _holders = _drain(a.node_id)  # Keep the actors alive.
    b = _add_slot_node(head_node_cluster)
    gate = _Gate.options(scheduling_strategy=_on_head(head_node_id)).remote()
    ref = _submit_fit(
        head_node_id,
        {"gate": gate, "run_name": "run"},
        ScalingConfig(resources_per_worker={"slot": 1}, **scaling_kwargs),
        str(tmp_path),
    )

    # Longer than one worker group start timeout, so the run has failed at least
    # one attempt and must have retried.
    time.sleep(8)
    assert "run" not in ray.get(gate.started_on.remote())
    ready, _ = ray.wait([ref], timeout=0)
    assert not ready, "The run gave up while waiting for a usable slot."

    c = _add_slot_node(head_node_cluster)
    _get_with_deadline(ref)
    assert sorted(ray.get(gate.started_on.remote())["run"]) == sorted(
        [b.node_id, c.node_id]
    )


def test_runs_queue_for_a_single_slot(head_node_cluster, tmp_path):
    """Run A holds the only slot when run B starts. B must keep retrying while A
    runs, then start on the same node once A finishes."""
    head_node_id = ray.get_runtime_context().get_node_id()
    slot_node = _add_slot_node(head_node_cluster)
    gate = _Gate.options(scheduling_strategy=_on_head(head_node_id)).remote(
        released=False
    )

    def submit(run_name: str) -> ray.ObjectRef:
        return _submit_fit(
            head_node_id,
            {"gate": gate, "run_name": run_name},
            _slot_scaling_config(),
            str(tmp_path / run_name),
        )

    a_ref = submit("a")
    _wait_for_starts(gate, {"a": 1})
    b_ref = submit("b")

    # Longer than one worker group start timeout, so B has failed at least one
    # attempt and must have retried.
    time.sleep(8)
    assert "b" not in ray.get(gate.started_on.remote())
    ready, _ = ray.wait([a_ref, b_ref], num_returns=2, timeout=0)
    assert not ready, "A run gave up while the slot was busy."

    ray.get(gate.release.remote())
    _get_with_deadline([a_ref, b_ref])
    assert ray.get(gate.started_on.remote()) == {
        "a": [slot_node.node_id],
        "b": [slot_node.node_id],
    }


def test_concurrent_runs_survive_node_churn(head_node_cluster, tmp_path):
    """Runs A and B each hold one of two slots. A new node joins and sorts first,
    then B's node is removed. B must restart on the new node, without
    disturbing A, and both runs must finish.

    A starts first, so the coordinator serves it first. Recomputing from scratch
    after the churn then reserves the new node for A and A's node for B."""
    head_node_id = ray.get_runtime_context().get_node_id()
    nodes = {}
    for _ in range(2):
        node = _add_slot_node(head_node_cluster)
        nodes[node.node_id] = node

    gate = _Gate.options(scheduling_strategy=_on_head(head_node_id)).remote(
        released=False
    )
    refs = {}

    def submit(run_name: str):
        refs[run_name] = _submit_fit(
            head_node_id,
            {"gate": gate, "run_name": run_name},
            _slot_scaling_config(),
            str(tmp_path / run_name),
            max_failures=1,
        )

    submit("a")
    _wait_for_starts(gate, {"a": 1})
    submit("b")
    started_on = _wait_for_starts(gate, {"a": 1, "b": 1})
    a_node_id, b_node_id = started_on["a"][0], started_on["b"][0]
    assert a_node_id != b_node_id

    z = _add_slot_node(head_node_cluster, sort_first=True)
    head_node_cluster.remove_node(nodes[b_node_id])

    started_on = _wait_for_starts(gate, {"a": 1, "b": 2})
    assert started_on["b"][1] == z.node_id
    assert started_on["a"] == [a_node_id]

    ray.get(gate.release.remote())
    _get_with_deadline([refs["a"], refs["b"]])


@pytest.mark.parametrize("fail", [False, True], ids=["succeeded", "failed"])
def test_a_finished_run_releases_its_resources(head_node_cluster, tmp_path, fail):
    """A run that finishes, whether it succeeds or fails, gives back its
    placement group and its coordinator reservation, so the next requester can
    use the same node."""
    head_node_id = ray.get_runtime_context().get_node_id()
    slot_node = _add_slot_node(head_node_cluster)

    gate = _Gate.options(scheduling_strategy=_on_head(head_node_id)).remote()
    ref = _submit_fit(
        head_node_id,
        {"gate": gate, "run_name": "a", "fail": fail},
        _slot_scaling_config(),
        str(tmp_path / "a"),
    )
    if fail:
        with pytest.raises(ray.exceptions.RayTaskError, match="Injected failure"):
            _get_with_deadline(ref)
    else:
        _get_with_deadline(ref)
    assert ray.get(gate.started_on.remote())["a"] == [slot_node.node_id]

    pg_states = {pg["state"] for pg in ray.util.placement_group_table().values()}
    assert pg_states <= {"REMOVED"}, _placement_diagnostics()

    # A request at the run's priority, made after it, only gets the slot if the
    # finished run's request is gone.
    coordinator = get_or_create_autoscaling_coordinator()
    ray.get(
        coordinator.request_resources.remote(
            requester_id="probe",
            resources=[{"slot": 1}],
            expire_after_s=60,
            priority=ResourceRequestPriority.HIGH,
        )
    )
    reserved = ray.get(
        coordinator.get_reserved_resources.remote("probe", recompute=True)
    )
    assert reserved.get(slot_node.node_id, {}).get("slot") == 1, reserved
    ray.get(coordinator.cancel_request.remote("probe"))

    assert _fit_one_slot_run(head_node_id, str(tmp_path / "b")) == slot_node.node_id


@pytest.fixture
def autoscaling_cluster(monkeypatch):
    _set_short_worker_group_start_timeout(monkeypatch)
    cluster = AutoscalingCluster(
        # No GPUs on the head, even if the host has one, so every worker needs
        # a node the autoscaler launches.
        head_resources={"CPU": 2, "GPU": 0},
        worker_node_types={
            "gpu_node": {
                "resources": {
                    "CPU": 2,
                    "GPU": 4,
                    "object_store_memory": 100 * MB,
                },
                "node_config": {},
                "min_workers": 0,
                "max_workers": 3,
            },
        },
        autoscaler_v2=True,
    )
    cluster.start()
    ray.init("auto")
    yield cluster
    ray.shutdown()
    cluster.shutdown()


def test_strict_spread_starts_on_an_autoscaling_cluster(autoscaling_cluster, tmp_path):
    """A STRICT_SPREAD run needs one node per worker, and the cluster starts with
    none. The autoscaler must launch enough nodes for the run to start."""
    head_node_id = ray.get_runtime_context().get_node_id()
    gate = _Gate.options(scheduling_strategy=_on_head(head_node_id)).remote()

    _get_with_deadline(
        _submit_fit(
            head_node_id,
            {"gate": gate, "run_name": "run"},
            ScalingConfig(
                num_workers=2, use_gpu=True, placement_strategy="STRICT_SPREAD"
            ),
            str(tmp_path),
        ),
        # Includes the time the autoscaler takes to launch two nodes.
        timeout_s=120,
    )
    started_on = ray.get(gate.started_on.remote())["run"]
    assert len(set(started_on)) == 2


def test_strict_pack_starts_on_an_autoscaling_cluster(autoscaling_cluster, tmp_path):
    """A STRICT_PACK run needs one node with room for every worker, and the
    cluster starts with none. The autoscaler must launch such a node."""
    head_node_id = ray.get_runtime_context().get_node_id()
    gate = _Gate.options(scheduling_strategy=_on_head(head_node_id)).remote()

    _get_with_deadline(
        _submit_fit(
            head_node_id,
            {"gate": gate, "run_name": "run"},
            # A node has 4 GPUs but only 2 CPUs, so ask for GPUs only.
            ScalingConfig(
                num_workers=4,
                use_gpu=True,
                resources_per_worker={"GPU": 1},
                placement_strategy="STRICT_PACK",
            ),
            str(tmp_path),
        ),
        # Includes the time the autoscaler takes to launch a node.
        timeout_s=120,
    )
    started_on = ray.get(gate.started_on.remote())["run"]
    assert len(started_on) == 4
    assert len(set(started_on)) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
