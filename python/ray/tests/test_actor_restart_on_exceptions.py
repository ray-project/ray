import os
import sys
import time

import pytest

import ray


class TransientGPUError(Exception):
    """Stands in for a CUDA/GPU soft fault that poisons the process."""


@ray.remote(num_cpus=0)
class FailOnceGate:
    """Allows one failure across worker restarts; an in-worker gate would reset."""

    def __init__(self):
        self._used = False

    def should_fail(self) -> bool:
        if not self._used:
            self._used = True
            return True
        return False

    def calls(self) -> int:
        return 1 if self._used else 0


@ray.remote(num_cpus=0)
class AttemptCounter:
    """Records attempts outside the actor process being restarted."""

    def __init__(self):
        self.pids = []

    def record(self, pid):
        self.pids.append(pid)
        return len(self.pids)

    def get_pids(self):
        return self.pids


def test_method_matching_exception_triggers_restart(ray_start_regular):
    """A matching method exception restarts the actor (does not surface)."""
    gate = FailOnceGate.remote()

    @ray.remote(max_restarts=2, restart_on_exceptions=["TransientGPUError"])
    class Worker:
        def __init__(self, gate) -> None:
            self._gate = gate

        def maybe_fail(self) -> str:
            if ray.get(self._gate.should_fail.remote()):
                raise TransientGPUError("CUDA error: device-side assert")
            return "ok"

        def pid(self) -> int:
            return os.getpid()

    worker = Worker.remote(gate)
    first_pid = ray.get(worker.pid.remote(), timeout=60)

    # ActorUnavailableError or ActorDiedError can surface, depending on timing.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(worker.maybe_fail.remote(), timeout=60)

    second_pid = ray.get(worker.pid.remote(), timeout=60)
    assert second_pid != first_pid
    assert ray.get(worker.maybe_fail.remote(), timeout=60) == "ok"
    assert ray.get(gate.calls.remote(), timeout=60) == 1


def test_non_matching_exception_surfaces_normally(ray_start_regular):
    """A non-matching exception is surfaced to the caller, no restart."""

    @ray.remote(max_restarts=2, restart_on_exceptions=["TransientGPUError"])
    class Worker:
        def pid(self) -> int:
            return os.getpid()

        def raise_value_error(self) -> None:
            raise ValueError("ordinary bug")

    worker = Worker.remote()
    first_pid = ray.get(worker.pid.remote(), timeout=60)

    with pytest.raises(ValueError):
        ray.get(worker.raise_value_error.remote(), timeout=60)

    assert ray.get(worker.pid.remote(), timeout=60) == first_pid


def test_init_matching_exception_recovers(ray_start_regular):
    """A matching __init__ exception restarts the actor, which then succeeds."""
    gate = FailOnceGate.remote()

    @ray.remote(max_restarts=2, restart_on_exceptions=["TransientGPUError"])
    class Worker:
        def __init__(self, gate) -> None:
            if ray.get(gate.should_fail.remote()):
                raise TransientGPUError("CUDA error during init")

        def ping(self) -> str:
            return "pong"

    worker = Worker.remote(gate)
    # The handle can be temporarily unavailable during constructor recovery.
    deadline = time.time() + 60
    while True:
        try:
            assert ray.get(worker.ping.remote(), timeout=60) == "pong"
            break
        except ray.exceptions.RayActorError:
            assert time.time() < deadline, "actor did not come back after restart"
            time.sleep(0.5)
    assert ray.get(gate.calls.remote(), timeout=60) == 1


def test_matching_exception_exhausts_max_restarts(ray_start_regular):
    """A matching __init__ exception consumes max_restarts and eventually fails."""

    from ray._private.test_utils import wait_for_condition

    counter = AttemptCounter.remote()

    @ray.remote(max_restarts=1, restart_on_exceptions=["TransientGPUError"])
    class AlwaysFailsInit:
        def __init__(self, counter) -> None:
            ray.get(counter.record.remote(os.getpid()), timeout=30)
            raise TransientGPUError("CUDA error during init")

        def ping(self) -> str:
            return "pong"

    actor = AlwaysFailsInit.remote(counter)
    actor_id = actor._actor_id.hex()

    def permanently_dead():
        info = ray._private.state.actors(actor_id)
        return info is not None and info["State"] == "DEAD"

    wait_for_condition(permanently_dead, timeout=60)
    pids = ray.get(counter.get_pids.remote(), timeout=30)
    assert len(pids) == 2
    assert pids[0] != pids[1]
    for _ in range(2):
        with pytest.raises(ray.exceptions.ActorDiedError):
            ray.get(actor.ping.remote(), timeout=30)
    assert permanently_dead()
    assert ray.get(counter.get_pids.remote(), timeout=30) == pids


def test_max_task_retries_reruns_failed_call_on_restarted_actor(ray_start_regular):
    """Matching restart takes precedence over retry_exceptions in the same process."""
    counter = AttemptCounter.remote()

    @ray.remote(
        max_restarts=1,
        max_task_retries=1,
        restart_on_exceptions=["TransientGPUError"],
    )
    class Worker:
        def __init__(self, counter) -> None:
            self._counter = counter

        def pid(self):
            return os.getpid()

        @ray.method(retry_exceptions=True)
        def maybe_fail(self):
            if ray.get(self._counter.record.remote(os.getpid()), timeout=30) == 1:
                raise TransientGPUError("CUDA error: device-side assert")
            return os.getpid()

    worker = Worker.remote(counter)
    first_pid = ray.get(worker.pid.remote(), timeout=60)
    second_pid = ray.get(worker.maybe_fail.remote(), timeout=60)
    assert second_pid != first_pid
    assert ray.get(counter.get_pids.remote(), timeout=30) == [first_pid, second_pid]


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_partial_stream_retries_same_call(ray_start_regular, is_async):
    counter = AttemptCounter.remote()

    def sync_stream(self):
        attempt = ray.get(self.counter.record.remote(os.getpid()), timeout=30)
        yield 0
        yield 1
        if attempt == 1:
            raise TransientGPUError("CUDA fault after partial output")
        yield 2
        yield 3

    async def async_stream(self):
        attempt = await self.counter.record.remote(os.getpid())
        yield 0
        yield 1
        if attempt == 1:
            raise TransientGPUError("CUDA fault after partial output")
        yield 2
        yield 3

    @ray.remote(
        max_restarts=1,
        max_task_retries=1,
        restart_on_exceptions=["TransientGPUError"],
    )
    class Worker:
        def __init__(self, counter):
            self.counter = counter

        def pid(self):
            return os.getpid()

        stream = ray.method(num_returns="streaming")(
            async_stream if is_async else sync_stream
        )

    worker = Worker.remote(counter)
    first_pid = ray.get(worker.pid.remote(), timeout=60)
    # Consume one invocation, not a manually resubmitted stream after failure.
    stream = worker.stream.remote()
    values = []
    while True:
        try:
            ref = stream._next_sync(timeout_s=60)
        except StopIteration:
            break
        assert not ref.is_nil(), "timed out waiting for retried stream"
        values.append(ray.get(ref, timeout=30))
        assert len(values) <= 4, "duplicate output during replay"
    assert values == [0, 1, 2, 3]
    second_pid = ray.get(worker.pid.remote(), timeout=60)
    assert second_pid != first_pid
    assert ray.get(counter.get_pids.remote(), timeout=30) == [first_pid, second_pid]


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_streaming_generator_matching_exception_triggers_restart(
    ray_start_regular, is_async
):
    """Without task retries, the failed stream errors; a new call recovers."""
    gate = FailOnceGate.remote()

    def sync_stream(self):
        if ray.get(self._gate.should_fail.remote()):
            raise TransientGPUError("CUDA error: device-side assert in stream")
        yield 0
        yield 1

    async def async_stream(self):
        if await self._gate.should_fail.remote():
            raise TransientGPUError("CUDA error: device-side assert in stream")
        yield 0
        yield 1

    @ray.remote(max_restarts=2, restart_on_exceptions=["TransientGPUError"])
    class StreamingWorker:
        def __init__(self, gate) -> None:
            self._gate = gate

        def pid(self) -> int:
            return os.getpid()

        stream = ray.method(num_returns="streaming")(
            async_stream if is_async else sync_stream
        )

    worker = StreamingWorker.remote(gate)
    first_pid = ray.get(worker.pid.remote(), timeout=60)

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(next(worker.stream.remote()), timeout=60)

    second_pid = ray.get(worker.pid.remote(), timeout=60)
    assert second_pid != first_pid
    gen = worker.stream.remote()
    assert ray.get(next(gen), timeout=60) == 0
    assert ray.get(next(gen), timeout=60) == 1


def test_streaming_generator_non_matching_exception_reports_error(ray_start_regular):
    """A non-matching generator exception is reported, the actor survives."""

    @ray.remote(max_restarts=2, restart_on_exceptions=["TransientGPUError"])
    class StreamingWorker:
        def pid(self) -> int:
            return os.getpid()

        @ray.method(num_returns="streaming")
        def stream(self, fail: bool):
            if fail:
                raise ValueError("ordinary bug")
            yield 0

    worker = StreamingWorker.remote()
    first_pid = ray.get(worker.pid.remote(), timeout=60)

    with pytest.raises(ValueError):
        ray.get(next(worker.stream.remote(True)), timeout=60)

    assert ray.get(worker.pid.remote(), timeout=60) == first_pid
    gen = worker.stream.remote(False)
    assert ray.get(next(gen), timeout=60) == 0


def test_static_generator_matching_exception_triggers_restart(ray_start_regular):
    """A matching exception from a `num_returns=2` generator restarts the actor."""
    gate = FailOnceGate.remote()

    @ray.remote(max_restarts=2, restart_on_exceptions=["TransientGPUError"])
    class Worker:
        def __init__(self, gate) -> None:
            self._gate = gate

        def pid(self) -> int:
            return os.getpid()

        @ray.method(num_returns=2)
        def two_outputs(self):
            if ray.get(self._gate.should_fail.remote()):
                raise TransientGPUError("CUDA error: device-side assert")
            yield 0
            yield 1

    worker = Worker.remote(gate)
    first_pid = ray.get(worker.pid.remote(), timeout=60)

    refs = worker.two_outputs.remote()
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(refs[0], timeout=60)

    second_pid = ray.get(worker.pid.remote(), timeout=60)
    assert second_pid != first_pid

    refs = worker.two_outputs.remote()
    assert ray.get(refs[0], timeout=60) == 0
    assert ray.get(refs[1], timeout=60) == 1


def test_dynamic_generator_matching_exception_triggers_restart(ray_start_regular):
    """A matching exception from a `num_returns="dynamic"` generator restarts it."""
    gate = FailOnceGate.remote()

    @ray.remote(max_restarts=2, restart_on_exceptions=["TransientGPUError"])
    class Worker:
        def __init__(self, gate) -> None:
            self._gate = gate

        def pid(self) -> int:
            return os.getpid()

        @ray.method(num_returns="dynamic")
        def dynamic_outputs(self, n: int):
            if ray.get(self._gate.should_fail.remote()):
                raise TransientGPUError("CUDA error: device-side assert")
            for i in range(n):
                yield i

    worker = Worker.remote(gate)
    first_pid = ray.get(worker.pid.remote(), timeout=60)

    # `num_returns="dynamic"` first returns an ObjectRef to a generator of
    # ObjectRefs; the worker exit surfaces while fetching that generator.
    with pytest.raises(ray.exceptions.RayActorError):
        gen = ray.get(
            worker.dynamic_outputs.remote(2),
            timeout=60,
        )

    second_pid = ray.get(worker.pid.remote(), timeout=60)
    assert second_pid != first_pid

    gen = ray.get(worker.dynamic_outputs.remote(2), timeout=60)
    values = [ray.get(ref, timeout=60) for ref in gen]
    assert values == [0, 1]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
