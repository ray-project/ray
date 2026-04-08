import threading
import time

import numpy as np
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._raylet import STREAMING_GENERATOR_RETURN, ObjectRefGenerator
from ray.experimental.actor_pool import ActorPool, RetryPolicy


@pytest.fixture
def actor_pool_ft_cluster(ray_start_cluster):
    reconstruction_config = {
        "health_check_failure_threshold": 10,
        "health_check_period_ms": 100,
        "health_check_timeout_ms": 100,
        "health_check_initial_delay_ms": 0,
        "max_direct_call_object_size": 100,
        "task_retry_delay_ms": 100,
        "object_timeout_milliseconds": 200,
        "fetch_warn_timeout_milliseconds": 1000,
    }

    cluster = ray_start_cluster
    cluster.add_node(
        num_cpus=0,
        resources={"head": 2},
        enable_object_reconstruction=True,
        _system_config=reconstruction_config,
    )
    worker_nodes = [
        cluster.add_node(num_cpus=1, object_store_memory=200 * 1024 * 1024)
        for _ in range(3)
    ]
    worker_nodes_by_id = {node.node_id: node for node in worker_nodes}
    ray.init(address=cluster.address)

    yield cluster, worker_nodes_by_id

    ray.shutdown()


@pytest.mark.parametrize(
    "kill_after_yields",
    [2, 5],
    ids=["retry_incomplete", "reconstruct_completed"],
)
def test_actor_pool_streaming_generator_fault_tolerance(
    actor_pool_ft_cluster, kill_after_yields
):
    # Verify that actor-pool streaming-generator outputs survive both
    # mid-stream retry and post-completion reconstruction on another pool actor.

    _STREAM_ITEMS = 5
    _PAYLOAD_BYTES = 1_000_000

    @ray.remote(num_cpus=0, resources={"head": 1}, max_concurrency=8)
    class StreamingCoordinator:
        def __init__(self, kill_after_yields: int):
            self._kill_after_yields = kill_after_yields
            self._attempt_actor_ids = []
            self._actor_id_to_attempt_index = {}
            self._actor_id_to_node_id = {}
            self._yield_counts_by_actor_id = {}
            self._kill_point_reached = False
            self._finished_actor_ids = set()
            self._pause_released = threading.Event()

        def register_attempt(self, actor_id: str, node_id: str) -> int:
            if actor_id not in self._actor_id_to_attempt_index:
                attempt_index = len(self._attempt_actor_ids)
                self._attempt_actor_ids.append(actor_id)
                self._actor_id_to_attempt_index[actor_id] = attempt_index
                self._actor_id_to_node_id[actor_id] = node_id
                self._yield_counts_by_actor_id[actor_id] = 0
            return self._actor_id_to_attempt_index[actor_id]

        def after_yield(self, actor_id: str) -> None:
            self._yield_counts_by_actor_id[actor_id] += 1
            yielded = self._yield_counts_by_actor_id[actor_id]
            attempt_index = self._actor_id_to_attempt_index[actor_id]

            if (
                attempt_index == 0
                and self._kill_after_yields < _STREAM_ITEMS
                and yielded == self._kill_after_yields
            ):
                self._kill_point_reached = True
                self._pause_released.wait()

        def release_pause(self) -> None:
            self._pause_released.set()

        def mark_finished(self, actor_id: str) -> None:
            self._finished_actor_ids.add(actor_id)

        def snapshot(self):
            return {
                "kill_after_yields": self._kill_after_yields,
                "attempt_actor_ids": list(self._attempt_actor_ids),
                "actor_id_to_attempt_index": dict(self._actor_id_to_attempt_index),
                "actor_id_to_node_id": dict(self._actor_id_to_node_id),
                "yield_counts_by_actor_id": dict(self._yield_counts_by_actor_id),
                "kill_point_reached": self._kill_point_reached,
                "finished_actor_ids": sorted(self._finished_actor_ids),
            }

    @ray.remote
    class StreamingWorker:
        def stream(self, coordinator):
            ctx = ray.get_runtime_context()
            actor_id = ctx.get_actor_id()
            ray.get(
                coordinator.register_attempt.remote(
                    actor_id,
                    ctx.get_node_id(),
                )
            )

            for i in range(_STREAM_ITEMS):
                # Force each yielded value through the object store so node loss
                # triggers retry/reconstruction on the produced objects.
                yield np.full(_PAYLOAD_BYTES, i, dtype=np.uint8)
                ray.get(coordinator.after_yield.remote(actor_id))

            ray.get(coordinator.mark_finished.remote(actor_id))

    def submit_stream(pool: ActorPool, coordinator) -> ObjectRefGenerator:
        gen_ref = pool.submit(
            "stream",
            coordinator,
            num_returns=STREAMING_GENERATOR_RETURN,
        )
        return ObjectRefGenerator(gen_ref, ray._private.worker.global_worker)

    def drain_stream(
        gen: ObjectRefGenerator, expected_count: int, timeout_s: float = 30.0
    ):
        refs = []
        deadline = time.time() + timeout_s

        while len(refs) < expected_count and time.time() < deadline:
            ref = gen._next_sync(timeout_s=1)
            if ref.is_nil():
                continue
            refs.append(ref)

        if len(refs) != expected_count:
            pytest.fail(
                f"Timed out draining stream: got {len(refs)} refs, "
                f"expected {expected_count}"
            )

        return refs

    def first_attempt_finished(snapshot: dict) -> bool:
        if not snapshot["attempt_actor_ids"]:
            return False
        return snapshot["attempt_actor_ids"][0] in snapshot["finished_actor_ids"]

    def wait_for_additional_attempt(expected_attempts: int, timeout_s: float):
        try:
            wait_for_condition(
                lambda: len(ray.get(coordinator.snapshot.remote())["attempt_actor_ids"])
                >= expected_attempts,
                timeout=timeout_s,
            )
        except RuntimeError as exc:
            snapshot = ray.get(coordinator.snapshot.remote())
            pytest.fail(
                "Streaming generator reconstruction did not trigger a new attempt. "
                f"snapshot={snapshot}. original_error={exc}"
            )

    def wait_for_attempt_finished(actor_id: str, timeout_s: float):
        try:
            wait_for_condition(
                lambda: actor_id
                in ray.get(coordinator.snapshot.remote())["finished_actor_ids"],
                timeout=timeout_s,
            )
        except RuntimeError as exc:
            snapshot = ray.get(coordinator.snapshot.remote())
            pytest.fail(
                "Retried streaming-generator attempt did not report completion. "
                f"actor_id={actor_id} snapshot={snapshot}. original_error={exc}"
            )

    @ray.remote(num_cpus=0, resources={"head": 1})
    def consume_stream_values(refs):
        return [int(ray.get(ref)[0]) for ref in refs]

    cluster, worker_nodes_by_id = actor_pool_ft_cluster

    coordinator = StreamingCoordinator.remote(kill_after_yields)
    pool = ActorPool(
        StreamingWorker,
        size=3,
        actor_options={"num_cpus": 1},
        retry=RetryPolicy(max_attempts=3, backoff_ms=100),
        max_tasks_in_flight_per_actor=1,
    )

    # wait for pool to be ready and track initial actor ids
    wait_for_condition(
        lambda: len(pool.actors) == 3,
    )
    initial_actor_ids = [actor._actor_id.hex() for actor in pool.actors]

    try:
        gen = submit_stream(pool, coordinator)

        if kill_after_yields < _STREAM_ITEMS:
            # Pause the first attempt mid-stream, kill its node, then continue
            # consuming from the same generator handle after retry.
            refs = drain_stream(gen, kill_after_yields)

            wait_for_condition(
                lambda: ray.get(coordinator.snapshot.remote())["kill_point_reached"],
                timeout=20,
            )
            snapshot = ray.get(coordinator.snapshot.remote())
            first_actor_id = snapshot["attempt_actor_ids"][0]
            victim_node_id = snapshot["actor_id_to_node_id"][first_actor_id]

            cluster.remove_node(
                worker_nodes_by_id[victim_node_id],
                allow_graceful=False,
            )
            ray.get(coordinator.release_pause.remote())

            refs.extend(drain_stream(gen, _STREAM_ITEMS - kill_after_yields))
            wait_for_additional_attempt(expected_attempts=2, timeout_s=20)
            snapshot = ray.get(coordinator.snapshot.remote())
            second_actor_id = snapshot["attempt_actor_ids"][1]
            wait_for_attempt_finished(second_actor_id, timeout_s=20)
        else:
            # Let the original task finish, then kill the producing node so the
            # downstream consumer has to trigger object reconstruction.
            refs = drain_stream(gen, _STREAM_ITEMS)
            with pytest.raises(StopIteration):
                gen._next_sync(timeout_s=1)

            wait_for_condition(
                lambda: first_attempt_finished(ray.get(coordinator.snapshot.remote())),
                timeout=20,
            )
            snapshot = ray.get(coordinator.snapshot.remote())
            first_actor_id = snapshot["attempt_actor_ids"][0]
            victim_node_id = snapshot["actor_id_to_node_id"][first_actor_id]

            cluster.remove_node(
                worker_nodes_by_id[victim_node_id],
                allow_graceful=False,
            )

            snapshot_after_kill = ray.get(coordinator.snapshot.remote())
            assert len(snapshot_after_kill["attempt_actor_ids"]) == 1

        consumer_ref = consume_stream_values.remote(refs)

        if kill_after_yields == _STREAM_ITEMS:
            wait_for_additional_attempt(expected_attempts=2, timeout_s=20)

        try:
            values = ray.get(consumer_ref, timeout=30)
        except ray.exceptions.GetTimeoutError as exc:
            snapshot = ray.get(coordinator.snapshot.remote())
            pytest.fail(
                "Downstream consumer timed out reading streaming-generator outputs "
                f"after retry/reconstruction. snapshot={snapshot}. original_error={exc}"
            )

        snapshot = ray.get(coordinator.snapshot.remote())

        attempt_actor_ids = snapshot["attempt_actor_ids"]
        first_actor_id = attempt_actor_ids[0]
        second_actor_id = attempt_actor_ids[1]

        assert values == [0, 1, 2, 3, 4]
        assert len(attempt_actor_ids) >= 2
        assert first_actor_id in initial_actor_ids
        assert second_actor_id in initial_actor_ids
        # retry/reconstruction should use a new actor, from the existing pool actors
        assert first_actor_id != second_actor_id
        assert (
            snapshot["actor_id_to_node_id"][first_actor_id]
            != snapshot["actor_id_to_node_id"][second_actor_id]
        )
        assert snapshot["yield_counts_by_actor_id"][second_actor_id] == _STREAM_ITEMS

        if kill_after_yields < _STREAM_ITEMS:
            assert (
                snapshot["yield_counts_by_actor_id"][first_actor_id]
                == kill_after_yields
            )
            assert first_actor_id not in snapshot["finished_actor_ids"]
        else:
            assert snapshot["yield_counts_by_actor_id"][first_actor_id] == _STREAM_ITEMS
            assert first_actor_id in snapshot["finished_actor_ids"]
            assert len(snapshot["finished_actor_ids"]) == 2
    finally:
        pool.shutdown()


@pytest.mark.parametrize(
    "pause_before_return",
    [True, False],
    ids=["retry_incomplete", "reconstruct_completed"],
)
def test_actor_pool_non_streaming_fault_tolerance(
    actor_pool_ft_cluster, pause_before_return
):
    # Verify that ordinary actor-pool task outputs survive both
    # mid-task retry and post-completion lineage reconstruction.

    _PAYLOAD_BYTES = 1_000_000

    @ray.remote(num_cpus=0, resources={"head": 1}, max_concurrency=8)
    class NonStreamingCoordinator:
        def __init__(self, pause_before_return: bool):
            self._pause_before_return = pause_before_return
            self._attempt_actor_ids = []
            self._actor_id_to_attempt_index = {}
            self._actor_id_to_node_id = {}
            self._pause_point_reached = False
            self._finished_actor_ids = set()
            self._pause_released = threading.Event()

        def register_attempt(self, actor_id: str, node_id: str) -> int:
            if actor_id not in self._actor_id_to_attempt_index:
                attempt_index = len(self._attempt_actor_ids)
                self._attempt_actor_ids.append(actor_id)
                self._actor_id_to_attempt_index[actor_id] = attempt_index
                self._actor_id_to_node_id[actor_id] = node_id
            return self._actor_id_to_attempt_index[actor_id]

        def maybe_pause_before_return(self, actor_id: str) -> None:
            attempt_index = self._actor_id_to_attempt_index[actor_id]
            if attempt_index == 0 and self._pause_before_return:
                self._pause_point_reached = True
                self._pause_released.wait()

        def release_pause(self) -> None:
            self._pause_released.set()

        def mark_finished(self, actor_id: str) -> None:
            self._finished_actor_ids.add(actor_id)

        def snapshot(self):
            return {
                "attempt_actor_ids": list(self._attempt_actor_ids),
                "actor_id_to_attempt_index": dict(self._actor_id_to_attempt_index),
                "actor_id_to_node_id": dict(self._actor_id_to_node_id),
                "pause_point_reached": self._pause_point_reached,
                "finished_actor_ids": sorted(self._finished_actor_ids),
            }

    @ray.remote
    class NonStreamingWorker:
        def run(self, coordinator, value):
            ctx = ray.get_runtime_context()
            actor_id = ctx.get_actor_id()
            ray.get(
                coordinator.register_attempt.remote(
                    actor_id,
                    ctx.get_node_id(),
                )
            )

            # Force the return value through the object store so node loss
            # triggers retry/reconstruction of the actor-pool output.
            result = np.full(_PAYLOAD_BYTES, value, dtype=np.uint8)

            ray.get(coordinator.maybe_pause_before_return.remote(actor_id))
            ray.get(coordinator.mark_finished.remote(actor_id))
            return result

    def first_attempt_finished(snapshot: dict) -> bool:
        if not snapshot["attempt_actor_ids"]:
            return False
        return snapshot["attempt_actor_ids"][0] in snapshot["finished_actor_ids"]

    def wait_for_additional_attempt(expected_attempts: int, timeout_s: float):
        try:
            wait_for_condition(
                lambda: len(ray.get(coordinator.snapshot.remote())["attempt_actor_ids"])
                >= expected_attempts,
                timeout=timeout_s,
            )
        except RuntimeError as exc:
            snapshot = ray.get(coordinator.snapshot.remote())
            pytest.fail(
                "Non-streaming reconstruction did not trigger a new attempt. "
                f"snapshot={snapshot}. original_error={exc}"
            )

    def wait_for_attempt_finished(actor_id: str, timeout_s: float):
        try:
            wait_for_condition(
                lambda: actor_id
                in ray.get(coordinator.snapshot.remote())["finished_actor_ids"],
                timeout=timeout_s,
            )
        except RuntimeError as exc:
            snapshot = ray.get(coordinator.snapshot.remote())
            pytest.fail(
                "Retried non-streaming attempt did not report completion. "
                f"actor_id={actor_id} snapshot={snapshot}. original_error={exc}"
            )

    @ray.remote(num_cpus=0, resources={"head": 1})
    def consume_value(value):
        return int(value[0])

    cluster, worker_nodes_by_id = actor_pool_ft_cluster

    coordinator = NonStreamingCoordinator.remote(pause_before_return)
    pool = ActorPool(
        NonStreamingWorker,
        size=3,
        actor_options={"num_cpus": 1},
        retry=RetryPolicy(max_attempts=3, backoff_ms=100),
        max_tasks_in_flight_per_actor=1,
    )
    wait_for_condition(lambda: len(pool.actors) == 3)
    initial_actor_ids = [actor._actor_id.hex() for actor in pool.actors]

    try:
        ref = pool.submit("run", coordinator, 7)

        if pause_before_return:
            wait_for_condition(
                lambda: ray.get(coordinator.snapshot.remote())["pause_point_reached"],
                timeout=20,
            )
            snapshot = ray.get(coordinator.snapshot.remote())
            first_actor_id = snapshot["attempt_actor_ids"][0]
            victim_node_id = snapshot["actor_id_to_node_id"][first_actor_id]

            cluster.remove_node(
                worker_nodes_by_id[victim_node_id],
                allow_graceful=False,
            )

            wait_for_additional_attempt(expected_attempts=2, timeout_s=20)
            snapshot = ray.get(coordinator.snapshot.remote())
            second_actor_id = snapshot["attempt_actor_ids"][1]
            wait_for_attempt_finished(second_actor_id, timeout_s=20)
        else:
            # Let the original task finish, then kill the producing node so the
            # downstream consumer has to trigger lineage reconstruction.
            wait_for_condition(
                lambda: first_attempt_finished(ray.get(coordinator.snapshot.remote())),
                timeout=20,
            )
            snapshot = ray.get(coordinator.snapshot.remote())
            first_actor_id = snapshot["attempt_actor_ids"][0]
            victim_node_id = snapshot["actor_id_to_node_id"][first_actor_id]

            cluster.remove_node(
                worker_nodes_by_id[victim_node_id],
                allow_graceful=False,
            )

            snapshot_after_kill = ray.get(coordinator.snapshot.remote())
            assert len(snapshot_after_kill["attempt_actor_ids"]) == 1

        consumer_ref = consume_value.remote(ref)

        if not pause_before_return:
            wait_for_additional_attempt(expected_attempts=2, timeout_s=20)
            snapshot = ray.get(coordinator.snapshot.remote())
            second_actor_id = snapshot["attempt_actor_ids"][1]
            wait_for_attempt_finished(second_actor_id, timeout_s=20)

        try:
            value = ray.get(consumer_ref, timeout=30)
        except ray.exceptions.GetTimeoutError as exc:
            snapshot = ray.get(coordinator.snapshot.remote())
            pytest.fail(
                "Downstream consumer timed out reading non-streaming outputs "
                f"after retry/reconstruction. snapshot={snapshot}. original_error={exc}"
            )

        snapshot = ray.get(coordinator.snapshot.remote())

        attempt_actor_ids = snapshot["attempt_actor_ids"]
        first_actor_id = attempt_actor_ids[0]
        second_actor_id = attempt_actor_ids[1]

        assert value == 7
        assert len(attempt_actor_ids) >= 2
        assert first_actor_id in initial_actor_ids
        assert second_actor_id in initial_actor_ids
        assert first_actor_id != second_actor_id
        assert (
            snapshot["actor_id_to_node_id"][first_actor_id]
            != snapshot["actor_id_to_node_id"][second_actor_id]
        )

        if pause_before_return:
            assert first_actor_id not in snapshot["finished_actor_ids"]
            assert second_actor_id in snapshot["finished_actor_ids"]
        else:
            assert first_actor_id in snapshot["finished_actor_ids"]
            assert second_actor_id in snapshot["finished_actor_ids"]
            assert len(snapshot["finished_actor_ids"]) == 2
    finally:
        pool.shutdown()


# ---------------------------------------------------------------------------
# Backpressure integration tests
# ---------------------------------------------------------------------------


def get_slot_info(pool):
    """Query both C++ occupied task slots and Python pool stats."""
    worker = ray._private.worker.global_worker
    occupied = worker.core_worker.get_occupied_task_slots(pool.pool_id)
    stats = pool.stats()
    return occupied, stats


@ray.remote(num_cpus=0, max_concurrency=64)
class BookkeepingActor:
    """Push-based tracker that records task lifecycle events and detects
    oversubmission.  Every worker task must call ``task_started`` /
    ``task_completed`` so the bookkeeper can maintain a running count of
    concurrently inflight tasks.  If the count ever exceeds
    *max_allowed_inflight* the violation is recorded immediately.
    """

    def __init__(self, max_allowed_inflight: int):
        self._max_allowed = max_allowed_inflight
        self._inflight = 0
        self._peak_inflight = 0
        self._total_started = 0
        self._total_completed = 0
        self._violations = []
        self._gates = {}
        self._events = []

    def task_started(self, task_id: str):
        self._inflight += 1
        self._total_started += 1
        self._peak_inflight = max(self._peak_inflight, self._inflight)
        if self._inflight > self._max_allowed:
            self._violations.append(
                {
                    "event": "oversubmission",
                    "task_id": task_id,
                    "inflight": self._inflight,
                    "max_allowed": self._max_allowed,
                }
            )
        self._events.append(("started", task_id, self._inflight))

    def task_completed(self, task_id: str):
        self._inflight -= 1
        self._total_completed += 1
        self._events.append(("completed", task_id, self._inflight))

    def wait_gate(self, gate_name: str):
        if gate_name not in self._gates:
            self._gates[gate_name] = threading.Event()
        self._gates[gate_name].wait()

    def open_gate(self, gate_name: str):
        if gate_name not in self._gates:
            self._gates[gate_name] = threading.Event()
        self._gates[gate_name].set()

    def snapshot(self):
        return {
            "inflight": self._inflight,
            "peak_inflight": self._peak_inflight,
            "total_started": self._total_started,
            "total_completed": self._total_completed,
            "violations": list(self._violations),
            "events": list(self._events),
        }


@ray.remote
class BackpressureWorker:
    def blocking_task(self, bookkeeper, task_id):
        ray.get(bookkeeper.task_started.remote(task_id))
        ray.get(bookkeeper.wait_gate.remote(f"release_{task_id}"))
        ray.get(bookkeeper.task_completed.remote(task_id))
        return task_id

    def streaming_task(self, bookkeeper, task_id, num_items):
        ray.get(bookkeeper.task_started.remote(task_id))
        for i in range(num_items):
            ray.get(bookkeeper.wait_gate.remote(f"yield_{task_id}_{i}"))
            yield i
        ray.get(bookkeeper.task_completed.remote(task_id))


@pytest.mark.parametrize(
    "ray_start_regular,pool_size,max_tasks_in_flight,num_extra_tasks",
    [
        ({"num_cpus": 4}, 2, 1, 3),
        ({"num_cpus": 4}, 1, 2, 2),
        ({"num_cpus": 4}, 3, 1, 0),
    ],
    ids=["2x1_backlog3", "1x2_backlog2", "3x1_no_backlog"],
    indirect=["ray_start_regular"],
)
def test_backpressure_inflight_and_backlog_accounting(
    ray_start_regular, pool_size, max_tasks_in_flight, num_extra_tasks
):
    capacity = pool_size * max_tasks_in_flight
    total_tasks = capacity + num_extra_tasks

    bookkeeper = BookkeepingActor.remote(max_allowed_inflight=capacity)
    pool = ActorPool(
        BackpressureWorker,
        size=pool_size,
        actor_options={"num_cpus": 1},
        max_tasks_in_flight_per_actor=max_tasks_in_flight,
    )
    wait_for_condition(lambda: len(pool.actors) == pool_size)

    try:
        task_ids = [f"task_{i}" for i in range(total_tasks)]
        refs = []
        for tid in task_ids:
            ref = pool.submit("blocking_task", bookkeeper, tid)
            refs.append(ref)

        # Wait until all capacity slots are occupied by running tasks.
        wait_for_condition(
            lambda: ray.get(bookkeeper.snapshot.remote())["total_started"] == capacity,
            timeout=20,
        )

        # Release tasks one at a time and verify counts decrease.
        for i in range(total_tasks):
            tid = task_ids[i]
            ray.get(bookkeeper.open_gate.remote(f"release_{tid}"))

            completed = i + 1
            wait_for_condition(
                lambda expected=completed: ray.get(bookkeeper.snapshot.remote())[
                    "total_completed"
                ]
                >= expected,
                timeout=10,
            )

            if completed < total_tasks:
                # A backlog task may start after release; wait for it if capacity
                # allows more work to begin.
                wait_for_condition(
                    lambda expected=min(completed + capacity, total_tasks): ray.get(
                        bookkeeper.snapshot.remote()
                    )["total_started"]
                    >= expected,
                    timeout=10,
                )

        # Verify bookkeeper never saw oversubmission.
        snap = ray.get(bookkeeper.snapshot.remote())
        assert (
            snap["violations"] == []
        ), f"Oversubmission detected: {snap['violations']}"
        assert snap["peak_inflight"] <= capacity
        assert snap["total_completed"] == total_tasks

        # Collect results to ensure correctness.
        results = [ray.get(ref) for ref in refs]
        assert sorted(results) == sorted(task_ids)
    finally:
        pool.shutdown()


@pytest.mark.xfail(
    strict=True,
    reason="Core actor-pool stats do not reflect blocked non-streaming tasks/backlog",
)
@pytest.mark.parametrize(
    "ray_start_regular",
    [{"num_cpus": 4}],
    indirect=True,
)
def test_backpressure_pool_stats_track_blocked_and_backlogged_tasks(
    ray_start_regular,
):
    pool_size = 2
    max_tasks_in_flight = 1
    num_extra_tasks = 3
    capacity = pool_size * max_tasks_in_flight
    total_tasks = capacity + num_extra_tasks

    bookkeeper = BookkeepingActor.remote(max_allowed_inflight=capacity)
    pool = ActorPool(
        BackpressureWorker,
        size=pool_size,
        actor_options={"num_cpus": 1},
        max_tasks_in_flight_per_actor=max_tasks_in_flight,
    )
    wait_for_condition(lambda: len(pool.actors) == pool_size)

    try:
        for task_idx in range(total_tasks):
            pool.submit("blocking_task", bookkeeper, f"task_{task_idx}")

        wait_for_condition(
            lambda: ray.get(bookkeeper.snapshot.remote())["total_started"] == capacity,
            timeout=20,
        )

        occupied, stats = get_slot_info(pool)
        assert occupied == total_tasks
        assert stats["total_in_flight"] == capacity
        assert stats["backlog_size"] == num_extra_tasks
    finally:
        for task_idx in range(total_tasks):
            ray.get(bookkeeper.open_gate.remote(f"release_task_{task_idx}"))
        pool.shutdown()


@pytest.mark.xfail(
    reason="Core pool frees streaming generator slot on execution finish, "
    "not output consumption"
)
@pytest.mark.parametrize(
    "ray_start_regular",
    [{"num_cpus": 4}],
    indirect=True,
)
def test_streaming_generator_completion_on_consumption(ray_start_regular):
    _NUM_YIELDS = 5

    bookkeeper = BookkeepingActor.remote(max_allowed_inflight=2)
    pool = ActorPool(
        BackpressureWorker,
        size=2,
        actor_options={"num_cpus": 1},
        max_tasks_in_flight_per_actor=1,
    )
    wait_for_condition(lambda: len(pool.actors) == 2)

    try:
        # --- Part 1: Single generator, verify slot held until consumption ---
        gen_ref = pool.submit(
            "streaming_task",
            bookkeeper,
            "gen_0",
            _NUM_YIELDS,
            num_returns=STREAMING_GENERATOR_RETURN,
        )
        gen = ObjectRefGenerator(gen_ref, ray._private.worker.global_worker)

        # Open all yield gates so the generator runs to completion quickly.
        for i in range(_NUM_YIELDS):
            ray.get(bookkeeper.open_gate.remote(f"yield_gen_0_{i}"))

        # Wait for the generator function to finish executing.
        wait_for_condition(
            lambda: ray.get(bookkeeper.snapshot.remote())["total_completed"] >= 1,
            timeout=20,
        )

        # The slot should STILL be occupied because we haven't consumed the
        # output.  This is the assertion expected to fail with current behavior.
        occupied, stats = get_slot_info(pool)
        assert occupied == 1, (
            f"Expected slot still occupied (output not consumed), "
            f"got occupied={occupied}, stats={stats}"
        )

        # Now consume all output.
        values = []
        for _ in range(_NUM_YIELDS):
            ref = gen._next_sync(timeout_s=5)
            assert not ref.is_nil()
            values.append(ray.get(ref))
        # Drain the sentinel.
        try:
            gen._next_sync(timeout_s=1)
        except StopIteration:
            pass

        # After consumption, the slot should be freed.
        wait_for_condition(
            lambda: get_slot_info(pool)[0] == 0,
            timeout=10,
        )
        assert values == list(range(_NUM_YIELDS))

        # --- Part 2: Two generators, verify 3rd task is backlogged ---
        for gen_id in ["gen_1", "gen_2"]:
            ref = pool.submit(
                "streaming_task",
                bookkeeper,
                gen_id,
                3,
                num_returns=STREAMING_GENERATOR_RETURN,
            )
            # Open all gates immediately so generators finish fast.
            for i in range(3):
                ray.get(bookkeeper.open_gate.remote(f"yield_{gen_id}_{i}"))

        # Wait for both generators to finish executing.
        wait_for_condition(
            lambda: ray.get(bookkeeper.snapshot.remote())["total_completed"] >= 3,
            timeout=20,
        )

        # Submit a 3rd task.  Under correct behavior both generator slots are
        # still held (output not consumed), so this should be backlogged.
        pool.submit("blocking_task", bookkeeper, "task_after_gens")

        occupied, stats = get_slot_info(pool)
        # 2 generator slots + 1 backlogged blocking task = 3
        assert occupied == 3, (
            f"Expected 3 occupied slots (2 generators + 1 backlog), "
            f"got occupied={occupied}, stats={stats}"
        )
        assert (
            stats["backlog_size"] >= 1
        ), f"Expected 3rd task in backlog, got backlog={stats['backlog_size']}"

        # Also verify normal (non-streaming) task completion is prompt:
        # release the blocking task (if it got dispatched) and check slot frees.
        ray.get(bookkeeper.open_gate.remote("release_task_after_gens"))

    finally:
        pool.shutdown()

    snap = ray.get(bookkeeper.snapshot.remote())
    assert snap["violations"] == [], f"Oversubmission: {snap['violations']}"


@pytest.mark.parametrize(
    "ray_start_regular",
    [{"num_cpus": 4}],
    indirect=True,
)
def test_streaming_generator_execution_completion_releases_capacity_for_new_work(
    ray_start_regular,
):
    num_yields = 2
    capacity = 2
    bookkeeper = BookkeepingActor.remote(max_allowed_inflight=capacity)
    pool = ActorPool(
        BackpressureWorker,
        size=2,
        actor_options={"num_cpus": 1},
        max_tasks_in_flight_per_actor=1,
    )
    wait_for_condition(lambda: len(pool.actors) == 2)

    try:
        generators = []
        for gen_id in ("gen_0", "gen_1"):
            gen_ref = pool.submit(
                "streaming_task",
                bookkeeper,
                gen_id,
                num_yields,
                num_returns=STREAMING_GENERATOR_RETURN,
            )
            generators.append(
                ObjectRefGenerator(gen_ref, ray._private.worker.global_worker)
            )
            for i in range(num_yields):
                ray.get(bookkeeper.open_gate.remote(f"yield_{gen_id}_{i}"))

        wait_for_condition(
            lambda: ray.get(bookkeeper.snapshot.remote())["total_completed"] == 2,
            timeout=20,
        )

        blocking_refs = [
            pool.submit("blocking_task", bookkeeper, "task_0"),
            pool.submit("blocking_task", bookkeeper, "task_1"),
        ]
        wait_for_condition(
            lambda: ray.get(bookkeeper.snapshot.remote())["total_started"] == 4,
            timeout=20,
        )

        occupied, stats = get_slot_info(pool)
        assert occupied == 2, f"Expected only blocking tasks to be occupied: {stats}"
        assert stats["total_in_flight"] == 2
        assert stats["backlog_size"] == 0

        for task_id in ("task_0", "task_1"):
            ray.get(bookkeeper.open_gate.remote(f"release_{task_id}"))

        assert ray.get(blocking_refs) == ["task_0", "task_1"]
        wait_for_condition(lambda: get_slot_info(pool)[0] == 0, timeout=10)

        generator_values = []
        for generator in generators:
            values = []
            for _ in range(num_yields):
                ref = generator._next_sync(timeout_s=5)
                assert not ref.is_nil()
                values.append(ray.get(ref))
            with pytest.raises(StopIteration):
                generator._next_sync(timeout_s=1)
            generator_values.append(values)

        assert generator_values == [list(range(num_yields)), list(range(num_yields))]
        snapshot = ray.get(bookkeeper.snapshot.remote())
        assert snapshot["violations"] == []
        assert snapshot["peak_inflight"] <= capacity
        assert snapshot["total_completed"] == 4
    finally:
        pool.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
