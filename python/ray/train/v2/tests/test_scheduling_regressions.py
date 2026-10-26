"""Train V2 runs must start when the autoscaling coordinator's view is wrong.

Train V2 sizes its worker group from the AutoscalingCoordinator's reservation.
That reservation is built from each node's *total* capacity, counts draining
nodes as alive, and is recomputed from scratch when nodes join or leave. These
tests check that a run still starts and finishes when that view is wrong:

* a node's capacity is held by work that never went through the coordinator,
* a node is draining,
* nodes join and leave while two runs share the cluster,
* the autoscaler has to launch nodes for a STRICT_SPREAD run.

Pinning workers to the reservation's nodes (#66002, reverted in #67337) turned
each of these into a run that retried the same pins forever.
"""

import asyncio
import sys
import time

import pytest

import ray
import ray.train
from ray._raylet import GcsClient
from ray.cluster_utils import AutoscalingCluster, Cluster
from ray.core.generated import autoscaler_pb2
from ray.train.v2._internal.constants import WORKER_GROUP_START_TIMEOUT_S_ENV_VAR
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# Workers can't import this test module, so ship its functions by value.
ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])

MB = 1024**2

# A run that can't place its worker group retries every few seconds, so a run
# that is going to start does so well within this.
RUN_DEADLINE_S = 60


@pytest.fixture(autouse=True)
def short_worker_group_start_timeout(monkeypatch):
    # Set before any cluster starts, so the driver tasks and the controller
    # inherit it and a failed placement retries quickly.
    monkeypatch.setenv(WORKER_GROUP_START_TIMEOUT_S_ENV_VAR, "5")


@pytest.fixture
def cluster():
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


def _slot_scaling_config(**kwargs):
    return ray.train.ScalingConfig(
        num_workers=1, resources_per_worker={"slot": 1}, **kwargs
    )


def test_starts_when_capacity_is_held_outside_the_coordinator(cluster, tmp_path):
    """Node X's slot is held by an actor that never registered with the
    coordinator, so the coordinator still counts it. The run must start on Y."""
    head_node_id = ray.get_runtime_context().get_node_id()
    x = _add_slot_node(cluster, sort_first=True)
    y = _add_slot_node(cluster)

    @ray.remote(num_cpus=0, resources={"slot": 1})
    class SlotHolder:
        def ready(self):
            pass

    holder = SlotHolder.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(x.node_id, soft=False)
    ).remote()
    ray.get(holder.ready.remote())

    assert _fit_one_slot_run(head_node_id, str(tmp_path)) == y.node_id


def test_starts_when_a_node_is_draining(cluster, tmp_path):
    """Node X is draining after a preemption notice. `ray.nodes()` still reports
    it alive, but it accepts no new work. The run must start on Y."""
    head_node_id = ray.get_runtime_context().get_node_id()
    x = _add_slot_node(cluster, sort_first=True)
    y = _add_slot_node(cluster)

    # Keep X busy, as a real preempted node would be, so draining doesn't
    # take it down straight away. The actor leaves X's slot free.
    @ray.remote(num_cpus=1)
    class Busy:
        def ready(self):
            pass

    busy = Busy.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(x.node_id, soft=False)
    ).remote()
    ray.get(busy.ready.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    is_accepted, rejection_reason = gcs_client.drain_node(
        x.node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption notice",
        int((time.time() + 600) * 1000),
    )
    assert is_accepted, rejection_reason

    assert _fit_one_slot_run(head_node_id, str(tmp_path)) == y.node_id


def test_concurrent_runs_survive_node_churn(cluster, tmp_path):
    """Runs A and B each hold one of two slots. A new node joins and sorts first,
    then B's node is removed. B must restart on the new node, without
    disturbing A, and both runs must finish.

    A starts first, so the coordinator serves it first. Recomputing from scratch
    after the churn then reserves the new node for A and A's node for B."""
    head_node_id = ray.get_runtime_context().get_node_id()
    nodes = {}
    for _ in range(2):
        node = _add_slot_node(cluster)
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

    def wait_for_starts(expected: dict):
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

    submit("a")
    wait_for_starts({"a": 1})
    submit("b")
    started_on = wait_for_starts({"a": 1, "b": 1})
    a_node_id, b_node_id = started_on["a"][0], started_on["b"][0]
    assert a_node_id != b_node_id

    z = _add_slot_node(cluster, sort_first=True)
    cluster.remove_node(nodes[b_node_id])

    started_on = wait_for_starts({"a": 1, "b": 2})
    assert started_on["b"][1] == z.node_id
    assert started_on["a"] == [a_node_id]

    ray.get(gate.release.remote())
    _get_with_deadline([refs["a"], refs["b"]])


@pytest.fixture
def autoscaling_cluster():
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
    none. The autoscaler must launch enough nodes for the run to start (#66766)."""
    head_node_id = ray.get_runtime_context().get_node_id()
    gate = _Gate.options(scheduling_strategy=_on_head(head_node_id)).remote()

    _get_with_deadline(
        _submit_fit(
            head_node_id,
            {"gate": gate, "run_name": "run"},
            ray.train.ScalingConfig(
                num_workers=2, use_gpu=True, placement_strategy="STRICT_SPREAD"
            ),
            str(tmp_path),
        ),
        # Includes the time the autoscaler takes to launch two nodes.
        timeout_s=120,
    )
    started_on = ray.get(gate.started_on.remote())["run"]
    assert len(set(started_on)) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
