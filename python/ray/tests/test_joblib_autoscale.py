import asyncio
import concurrent.futures
import gc
import os
import queue
import sys
import threading
import time
import weakref

import joblib
import pytest
from joblib.pool import PicklingPool

import ray
from ray.util.joblib import register_ray
from ray.util.joblib.ray_backend import RayBackend, _configure_pool_args
from ray.util.multiprocessing import Pool
from ray.util.multiprocessing.pool import (
    PoolTaskError,
    _ActorSlotSet,
    _ActorSlotState,
    _LegacyActorSet,
)


class _FakeObjectRef:
    def __init__(self):
        self.completion = concurrent.futures.Future()

    def future(self):
        return self.completion


class _FakeRemoteMethod:
    def __init__(self, function):
        self._function = function
        self.calls = 0

    def remote(self, *args):
        self.calls += 1
        return self._function(*args)


class _RaisingRemoteMethod:
    def __init__(self, error):
        self._error = error
        self.calls = 0

    def remote(self, *args):
        self.calls += 1
        raise self._error


class _BrokenCallbackObjectRef:
    def future(self):
        return self

    def add_done_callback(self, _callback):
        raise RuntimeError("callback registration failed")


class _FakeActor:
    def __init__(self, ready=True):
        self.batch_refs = []
        self.readiness_ref = _FakeObjectRef()
        if ready:
            self.readiness_ref.completion.set_result(None)
        self.exit_ref = _FakeObjectRef()
        self.ping = _FakeRemoteMethod(lambda: self.readiness_ref)
        self.run_batch = _FakeRemoteMethod(self._run_batch)
        self.__ray_terminate__ = _FakeRemoteMethod(lambda: self.exit_ref)

    def _run_batch(self, _func, _batch):
        ref = _FakeObjectRef()
        self.batch_refs.append(ref)
        return ref


def _wait_for(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("condition was not reached")
        time.sleep(0.01)


def _state_count(pool, state):
    return sum(
        slot_state is state for slot_state, _outstanding in pool._actor_set.snapshot()
    )


def _assert_actor_set_invariants(actor_set):
    with actor_set._condition:
        assert len(actor_set._slots) == actor_set.max_size
        for slot in actor_set._slots:
            assert slot.outstanding >= 0
            assert slot.tasks_submitted >= 0
            if actor_set._maxtasksperchild is not None:
                assert slot.tasks_submitted <= actor_set._maxtasksperchild
            if slot.state is _ActorSlotState.EMPTY:
                assert slot.actor is None
                assert slot.outstanding == 0
                assert slot.tasks_submitted == 0
                assert slot.idle_since is None
                assert slot.readiness_ref is None
                assert slot.exit_ref is None
            else:
                assert slot.actor is not None

            if slot.state is _ActorSlotState.STARTING:
                assert slot.idle_since is None
                assert slot.exit_ref is None
                assert slot.readiness_ref is not None or actor_set._error is not None
            elif slot.state is _ActorSlotState.ACTIVE:
                assert slot.readiness_ref is None
                assert slot.exit_ref is None
                assert (slot.idle_since is not None) == (slot.outstanding == 0)
            elif slot.state is _ActorSlotState.DRAINING:
                assert (
                    slot.outstanding > 0
                    or slot.exit_ref is not None
                    or actor_set._error is not None
                )


def test_slot_invariants_hold_across_the_complete_lifecycle():
    actor = _FakeActor(ready=False)
    actor_set = _ActorSlotSet(lambda: actor, min_size=0, max_size=1, idle_timeout_s=60)
    _assert_actor_set_invariants(actor_set)

    batch_ref = actor_set.submit(None, [])
    _assert_actor_set_invariants(actor_set)
    actor.readiness_ref.completion.set_result(None)
    _assert_actor_set_invariants(actor_set)
    batch_ref.completion.set_result([])
    _assert_actor_set_invariants(actor_set)

    actor_set.close()
    _assert_actor_set_invariants(actor_set)
    actor.exit_ref.completion.set_result(None)
    actor_set.join()
    _assert_actor_set_invariants(actor_set)


def test_draining_slot_is_not_reused_before_exit_confirmation():
    actors = []

    def create_actor():
        actor = _FakeActor()
        actors.append(actor)
        return actor

    actor_set = _ActorSlotSet(create_actor, min_size=0, max_size=1, idle_timeout_s=0)
    first_ref = actor_set.submit(None, [])
    first_ref.completion.set_result([])
    _wait_for(lambda: actor_set.snapshot()[0][0] is _ActorSlotState.DRAINING)

    submitted = threading.Event()

    def submit_again():
        actor_set.submit(None, [])
        submitted.set()

    submitter = threading.Thread(target=submit_again)
    submitter.start()
    assert not submitted.wait(0.05)

    actors[0].exit_ref.completion.set_result(None)
    assert submitted.wait(1)
    assert len(actors) == 2

    actors[1].batch_refs[0].completion.set_result([])
    actor_set.close()
    actors[1].exit_ref.completion.set_result(None)
    actor_set.join()
    submitter.join()


def test_maxtasksperchild_retires_actor_and_reuses_slot_after_exit():
    actors = []

    def create_actor():
        actor = _FakeActor()
        actors.append(actor)
        return actor

    actor_set = _ActorSlotSet(
        create_actor,
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        maxtasksperchild=2,
    )

    first_refs = [actor_set.submit(None, []) for _ in range(2)]
    assert actor_set.snapshot() == [(_ActorSlotState.DRAINING, 2)]
    assert actor_set._slots[0].tasks_submitted == 2
    assert actors[0].__ray_terminate__.calls == 0
    _assert_actor_set_invariants(actor_set)

    submitted = threading.Event()
    replacement_refs = []

    def submit_again():
        replacement_refs.append(actor_set.submit(None, []))
        submitted.set()

    submitter = threading.Thread(target=submit_again)
    submitter.start()
    assert not submitted.wait(0.05)

    first_refs[0].completion.set_result([])
    assert actors[0].__ray_terminate__.calls == 0
    first_refs[1].completion.set_result([])
    assert actors[0].__ray_terminate__.calls == 1
    actors[0].exit_ref.completion.set_result(None)

    assert submitted.wait(1)
    assert len(actors) == 2
    assert actor_set.snapshot() == [(_ActorSlotState.ACTIVE, 1)]
    assert actor_set._slots[0].tasks_submitted == 1
    _assert_actor_set_invariants(actor_set)

    replacement_refs[0].completion.set_result([])
    actor_set.close()
    actors[1].exit_ref.completion.set_result(None)
    actor_set.join()
    submitter.join()


def test_maxtasksperchild_restores_min_size_after_actor_exit():
    actors = []

    def create_actor():
        actor = _FakeActor()
        actors.append(actor)
        return actor

    actor_set = _ActorSlotSet(
        create_actor,
        min_size=1,
        max_size=1,
        idle_timeout_s=60,
        maxtasksperchild=1,
    )

    batch_ref = actor_set.submit(None, [])
    assert actor_set.snapshot() == [(_ActorSlotState.DRAINING, 1)]
    batch_ref.completion.set_result([])
    actors[0].exit_ref.completion.set_result(None)

    assert len(actors) == 2
    assert actor_set.snapshot() == [(_ActorSlotState.ACTIVE, 0)]
    assert actor_set._slots[0].tasks_submitted == 0
    _assert_actor_set_invariants(actor_set)

    actor_set.close()
    actors[1].exit_ref.completion.set_result(None)
    actor_set.join()


def test_actor_creation_failure_leaves_slot_reusable():
    actor = _FakeActor()
    attempts = 0

    def create_actor():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("actor creation failed")
        return actor

    actor_set = _ActorSlotSet(create_actor, min_size=0, max_size=1, idle_timeout_s=60)

    with pytest.raises(RuntimeError, match="actor creation failed"):
        actor_set.submit(None, [])
    assert actor_set.snapshot() == [(_ActorSlotState.EMPTY, 0)]

    batch_ref = actor_set.submit(None, [])
    batch_ref.completion.set_result([])
    actor_set.close()
    actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_readiness_submission_failure_retains_actor_and_fails_closed(monkeypatch):
    actor = _FakeActor(ready=False)
    actor.ping = _RaisingRemoteMethod(RuntimeError("readiness submission failed"))
    killed = []
    monkeypatch.setattr(ray, "kill", killed.append)
    actor_set = _ActorSlotSet(lambda: actor, min_size=0, max_size=1, idle_timeout_s=60)

    with pytest.raises(RuntimeError, match="readiness submission failed"):
        actor_set.submit(None, [])
    assert actor_set.snapshot() == [(_ActorSlotState.STARTING, 0)]
    assert actor_set._slots[0].actor is actor
    with pytest.raises(RuntimeError, match="actor management failed"):
        actor_set.submit(None, [])

    actor_set.close()
    assert actor_set.snapshot() == [(_ActorSlotState.DRAINING, 0)]
    assert killed == [actor]
    actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_constructor_callback_failure_kills_owned_actor(monkeypatch):
    actor = _FakeActor(ready=False)
    actor.readiness_ref = _BrokenCallbackObjectRef()
    actor.ping = _FakeRemoteMethod(lambda: actor.readiness_ref)
    killed = []
    condition = threading.Condition()
    monkeypatch.setattr(threading, "Condition", lambda: condition)

    def assert_kill_outside_condition(target):
        assert not condition._is_owned()
        killed.append(target)

    monkeypatch.setattr(ray, "kill", assert_kill_outside_condition)

    with pytest.raises(RuntimeError, match="callback registration failed"):
        _ActorSlotSet(lambda: actor, min_size=1, max_size=1, idle_timeout_s=60)

    assert killed == [actor]


def test_legacy_actor_set_cleans_up_after_readiness_failure(monkeypatch):
    actors = [_FakeActor(), _FakeActor()]
    created = []
    killed = []

    def create_actor():
        actor = actors[len(created)]
        created.append(actor)
        return actor

    def fail_readiness(_refs):
        raise RuntimeError("readiness failed")

    monkeypatch.setattr(ray, "get", fail_readiness)
    monkeypatch.setattr(ray, "kill", killed.append)

    with pytest.raises(RuntimeError, match="readiness failed"):
        _LegacyActorSet(create_actor, size=2, maxtasksperchild=-1)

    assert killed == created


def test_starting_slot_becomes_active_only_after_readiness():
    actor = _FakeActor(ready=False)
    actor_set = _ActorSlotSet(lambda: actor, min_size=0, max_size=1, idle_timeout_s=60)

    batch_ref = actor_set.submit(None, [])
    assert actor_set.snapshot() == [(_ActorSlotState.STARTING, 1)]

    actor.readiness_ref.completion.set_result(None)
    assert actor_set.snapshot() == [(_ActorSlotState.ACTIVE, 1)]
    batch_ref.completion.set_result([])
    assert actor_set.snapshot() == [(_ActorSlotState.ACTIVE, 0)]

    actor_set.close()
    actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_cancelled_starting_slot_waits_for_dedicated_exit_ref(monkeypatch):
    actor = _FakeActor(ready=False)
    killed = []
    actor_set = _ActorSlotSet(lambda: actor, min_size=1, max_size=1, idle_timeout_s=60)

    def assert_kill_outside_condition(target):
        assert not actor_set._condition._is_owned()
        killed.append(target)

    monkeypatch.setattr(ray, "kill", assert_kill_outside_condition)

    actor_set.close()
    assert actor_set.snapshot() == [(_ActorSlotState.DRAINING, 0)]
    assert killed == [actor]

    actor.readiness_ref.completion.set_result(None)
    assert actor_set.snapshot() == [(_ActorSlotState.DRAINING, 0)]

    actor.exit_ref.completion.set_result(None)
    actor_set.join()
    assert actor_set.snapshot() == [(_ActorSlotState.EMPTY, 0)]


def test_ready_actor_yields_capacity_to_pending_work():
    actors = [_FakeActor(ready=False), _FakeActor(ready=False)]
    actor_set = _ActorSlotSet(
        lambda: actors.pop(0), min_size=0, max_size=2, idle_timeout_s=60
    )

    first_ref = actor_set.submit(None, [])
    second_ref = actor_set.submit(None, [])
    assert actor_set.snapshot() == [
        (_ActorSlotState.STARTING, 1),
        (_ActorSlotState.STARTING, 1),
    ]

    first_actor, second_actor = actor_set._slots[0].actor, actor_set._slots[1].actor
    first_actor.readiness_ref.completion.set_result(None)
    first_ref.completion.set_result([])
    assert actor_set.snapshot() == [
        (_ActorSlotState.DRAINING, 0),
        (_ActorSlotState.STARTING, 1),
    ]

    first_actor.exit_ref.completion.set_result(None)
    second_actor.readiness_ref.completion.set_result(None)
    second_ref.completion.set_result([])
    actor_set.close()
    second_actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_hot_actor_hedges_short_burst_then_scales_deeper_backlog():
    hot_actor = _FakeActor()
    cold_actor = _FakeActor(ready=False)
    actors = [hot_actor, cold_actor]
    actor_set = _ActorSlotSet(
        lambda: actors.pop(0), min_size=1, max_size=2, idle_timeout_s=60
    )

    refs = [actor_set.submit(None, []) for _ in range(3)]

    assert len(hot_actor.batch_refs) == 2
    assert len(cold_actor.batch_refs) == 1
    assert actor_set.snapshot() == [
        (_ActorSlotState.ACTIVE, 2),
        (_ActorSlotState.STARTING, 1),
    ]
    _assert_actor_set_invariants(actor_set)

    refs[0].completion.set_result([])
    refs[1].completion.set_result([])
    assert actor_set.snapshot()[0] == (_ActorSlotState.DRAINING, 0)
    hot_actor.exit_ref.completion.set_result(None)

    cold_actor.readiness_ref.completion.set_result(None)
    refs[2].completion.set_result([])
    actor_set.close()
    cold_actor.exit_ref.completion.set_result(None)
    actor_set.join()
    _assert_actor_set_invariants(actor_set)


def test_batch_callback_registration_failure_fails_closed():
    actor = _FakeActor()
    actor.run_batch = _FakeRemoteMethod(lambda *_: _BrokenCallbackObjectRef())
    actor_set = _ActorSlotSet(lambda: actor, min_size=0, max_size=1, idle_timeout_s=60)

    actor_set.submit(None, [])
    assert actor_set.snapshot() == [(_ActorSlotState.ACTIVE, 1)]
    with pytest.raises(RuntimeError, match="actor management failed"):
        actor_set.submit(None, [])

    actor_set.close()
    actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_termination_callback_failure_retains_slot_and_fails_closed():
    actor = _FakeActor()
    actor.exit_ref = _BrokenCallbackObjectRef()
    actor.__ray_terminate__ = _FakeRemoteMethod(lambda: actor.exit_ref)
    actor_set = _ActorSlotSet(lambda: actor, min_size=1, max_size=1, idle_timeout_s=60)

    actor_set.close()
    assert actor_set.snapshot() == [(_ActorSlotState.DRAINING, 0)]
    assert actor_set._slots[0].actor is actor
    with pytest.raises(RuntimeError, match="cleanup failed"):
        actor_set.join()

    # Simulate a separately confirmed exit so the test-owned reaper can stop.
    slot = actor_set._slots[0]
    confirmed_exit = concurrent.futures.Future()
    confirmed_exit.set_exception(ray.exceptions.ActorDiedError())
    actor_set._actor_exited(slot, slot.generation, confirmed_exit)
    actor_set.join()


def test_ambiguous_termination_completion_does_not_release_slot():
    first_actor = _FakeActor()
    replacement_actor = _FakeActor()
    actors = [first_actor, replacement_actor]
    actor_set = _ActorSlotSet(
        lambda: actors.pop(0), min_size=0, max_size=1, idle_timeout_s=60
    )

    batch_ref = actor_set.submit(None, [])
    batch_ref.completion.set_result([])
    slot = actor_set._slots[0]
    with actor_set._condition:
        actor_set._begin_draining_locked(slot)

    unavailable = ray.exceptions.ActorUnavailableError("actor is restarting", None)
    first_actor.exit_ref.completion.set_exception(unavailable)

    assert actor_set.snapshot() == [(_ActorSlotState.DRAINING, 0)]
    assert slot.actor is first_actor
    assert actor_set._error is unavailable
    with pytest.raises(RuntimeError, match="actor management failed"):
        actor_set.submit(None, [])
    assert actors == [replacement_actor]

    actor_set.close()
    confirmed_exit = concurrent.futures.Future()
    confirmed_exit.set_exception(ray.exceptions.ActorDiedError())
    actor_set._actor_exited(slot, slot.generation, confirmed_exit)
    actor_set.join()


def test_idle_termination_submission_failure_has_no_retry_loop(monkeypatch):
    actor = _FakeActor()
    actor.__ray_terminate__ = _RaisingRemoteMethod(
        RuntimeError("termination submission failed")
    )
    monkeypatch.setattr(ray, "kill", lambda _actor: None)
    actor_set = _ActorSlotSet(lambda: actor, min_size=0, max_size=1, idle_timeout_s=0)

    batch_ref = actor_set.submit(None, [])
    batch_ref.completion.set_result([])
    _wait_for(lambda: actor.__ray_terminate__.calls == 1)
    time.sleep(0.05)

    assert actor.__ray_terminate__.calls == 1
    assert actor_set.snapshot() == [(_ActorSlotState.DRAINING, 0)]
    actor_set.terminate()
    with pytest.raises(RuntimeError, match="cleanup failed"):
        actor_set.join()


def test_actor_init_failure_releases_slot_for_retry():
    failed_actor = _FakeActor(ready=False)
    replacement_actor = _FakeActor()
    actors = [failed_actor, replacement_actor]
    actor_set = _ActorSlotSet(
        lambda: actors.pop(0), min_size=0, max_size=1, idle_timeout_s=60
    )

    actor_set.submit(None, [])
    failed_actor.readiness_ref.completion.set_exception(ray.exceptions.ActorDiedError())

    assert actor_set.snapshot() == [(_ActorSlotState.EMPTY, 0)]
    replacement_ref = actor_set.submit(None, [])
    replacement_ref.completion.set_result([])
    assert actor_set.snapshot() == [(_ActorSlotState.ACTIVE, 0)]

    actor_set.close()
    replacement_actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_min_size_startup_death_fails_closed_without_retry_churn():
    failed_actor = _FakeActor(ready=False)
    replacement_actor = _FakeActor()
    actors = [failed_actor, replacement_actor]
    actor_set = _ActorSlotSet(
        lambda: actors.pop(0), min_size=1, max_size=1, idle_timeout_s=60
    )

    startup_error = ray.exceptions.ActorDiedError()
    failed_actor.readiness_ref.completion.set_exception(startup_error)

    assert actor_set.snapshot() == [(_ActorSlotState.EMPTY, 0)]
    assert actor_set._error is startup_error
    with pytest.raises(RuntimeError, match="actor management failed"):
        actor_set.submit(None, [])
    assert actors == [replacement_actor]

    actor_set.close()
    actor_set.join()


@pytest.mark.parametrize("first_callback", ["batch", "readiness"])
def test_min_size_startup_death_is_callback_order_independent(first_callback):
    failed_actor = _FakeActor(ready=False)
    replacement_actor = _FakeActor()
    actors = [failed_actor, replacement_actor]
    actor_set = _ActorSlotSet(
        lambda: actors.pop(0), min_size=1, max_size=1, idle_timeout_s=60
    )

    batch_ref = actor_set.submit(None, [])
    startup_error = ray.exceptions.ActorDiedError()
    readiness_error = ray.exceptions.ActorDiedError()
    callbacks = {
        "batch": lambda: batch_ref.completion.set_exception(startup_error),
        "readiness": lambda: failed_actor.readiness_ref.completion.set_exception(
            readiness_error
        ),
    }
    callbacks[first_callback]()
    callbacks["readiness" if first_callback == "batch" else "batch"]()

    assert actor_set.snapshot() == [(_ActorSlotState.EMPTY, 0)]
    expected_error = startup_error if first_callback == "batch" else readiness_error
    assert actor_set._error is expected_error
    with pytest.raises(RuntimeError, match="actor management failed"):
        actor_set.submit(None, [])
    assert actors == [replacement_actor]

    actor_set.close()
    actor_set.join()


def test_startup_death_above_min_size_keeps_pool_available():
    active_actor = _FakeActor()
    failed_actor = _FakeActor(ready=False)
    actors = [active_actor, failed_actor]
    actor_set = _ActorSlotSet(
        lambda: actors.pop(0), min_size=1, max_size=2, idle_timeout_s=60
    )

    refs = [actor_set.submit(None, []) for _ in range(3)]
    refs[2].completion.set_exception(ray.exceptions.ActorDiedError())
    failed_actor.readiness_ref.completion.set_exception(ray.exceptions.ActorDiedError())

    assert actor_set.snapshot() == [
        (_ActorSlotState.ACTIVE, 2),
        (_ActorSlotState.EMPTY, 0),
    ]
    assert actor_set._error is None

    refs[0].completion.set_result([])
    refs[1].completion.set_result([])
    continuation_ref = actor_set.submit(None, [])
    continuation_ref.completion.set_result([])

    actor_set.close()
    active_actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_ambiguous_readiness_failure_retains_actor_and_fails_closed():
    actor = _FakeActor(ready=False)
    created = []

    def create_actor():
        created.append(actor)
        return actor

    actor_set = _ActorSlotSet(create_actor, min_size=0, max_size=1, idle_timeout_s=60)

    batch_ref = actor_set.submit(None, [])
    actor.readiness_ref.completion.set_exception(
        RuntimeError("readiness result became unavailable")
    )

    assert actor_set.snapshot() == [(_ActorSlotState.STARTING, 1)]
    assert actor_set._slots[0].actor is actor
    assert created == [actor]
    with pytest.raises(RuntimeError, match="actor management failed"):
        actor_set.submit(None, [])
    assert created == [actor]

    actor_set.close()
    batch_ref.completion.set_result([])
    actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_actor_unavailable_readiness_retains_actor_and_fails_closed():
    actor = _FakeActor(ready=False)
    actor_set = _ActorSlotSet(lambda: actor, min_size=0, max_size=1, idle_timeout_s=60)

    batch_ref = actor_set.submit(None, [])
    unavailable = ray.exceptions.ActorUnavailableError("actor is restarting", None)
    actor.readiness_ref.completion.set_exception(unavailable)

    assert actor_set.snapshot() == [(_ActorSlotState.STARTING, 1)]
    assert actor_set._slots[0].actor is actor
    assert actor_set._error is unavailable
    with pytest.raises(RuntimeError, match="actor management failed"):
        actor_set.submit(None, [])

    actor_set.close()
    batch_ref.completion.set_exception(unavailable)
    actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_actor_unavailable_batch_retains_actor_and_fails_closed():
    actor = _FakeActor()
    actor_set = _ActorSlotSet(lambda: actor, min_size=0, max_size=1, idle_timeout_s=60)

    batch_ref = actor_set.submit(None, [])
    unavailable = ray.exceptions.ActorUnavailableError("actor is restarting", None)
    batch_ref.completion.set_exception(unavailable)

    assert actor_set.snapshot() == [(_ActorSlotState.ACTIVE, 0)]
    assert actor_set._slots[0].actor is actor
    assert actor_set._error is unavailable
    with pytest.raises(RuntimeError, match="actor management failed"):
        actor_set.submit(None, [])

    actor_set.close()
    actor.exit_ref.completion.set_result(None)
    actor_set.join()


def test_cancelled_readiness_does_not_reuse_live_actor(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote(num_cpus=0, max_concurrency=4)
    class ProbeActor:
        async def ping(self):
            await asyncio.sleep(60)

        async def run_batch(self, _func, _batch):
            await asyncio.sleep(60)

        def alive(self):
            return True

    actors = []

    def create_actor():
        actor = ProbeActor.remote()
        actors.append(actor)
        return actor

    actor_set = _ActorSlotSet(create_actor, min_size=0, max_size=1, idle_timeout_s=60)
    batch_ref = actor_set.submit(None, [])
    with actor_set._condition:
        readiness_ref = actor_set._slots[0].readiness_ref

    ray.cancel(readiness_ref, recursive=False)
    _wait_for(lambda: actor_set._error is not None)

    assert isinstance(actor_set._error, ray.exceptions.TaskCancelledError)
    assert actor_set.snapshot() == [(_ActorSlotState.STARTING, 1)]
    assert ray.get(actors[0].alive.remote())
    with pytest.raises(RuntimeError, match="actor management failed"):
        actor_set.submit(None, [])
    assert len(actors) == 1

    actor_set.terminate()
    _wait_for(lambda: actor_set.snapshot() == [(_ActorSlotState.EMPTY, 0)], timeout=20)
    actor_set.join()
    with pytest.raises(ray.exceptions.RayError):
        ray.get(batch_ref)


@pytest.mark.parametrize(
    "options",
    [
        {"max_size": 0},
        {"min_size": -1, "max_size": 1},
        {"min_size": 2, "max_size": 1},
        {"max_size": 1, "idle_timeout_s": -1},
    ],
)
def test_elastic_pool_validates_capacity(shutdown_only, options):
    ray.init(num_cpus=1)
    with pytest.raises(ValueError):
        Pool(**options)


@pytest.mark.parametrize("maxtasksperchild", [0, -1, 1.5])
def test_pool_validates_maxtasksperchild(maxtasksperchild):
    with pytest.raises(ValueError, match="maxtasksperchild"):
        Pool(max_size=1, maxtasksperchild=maxtasksperchild)
    assert not ray.is_initialized()


def test_elastic_pool_recycles_actors_after_maxtasksperchild(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        maxtasksperchild=2,
    )

    pids = [pool.apply(os.getpid) for _ in range(4)]

    assert pids[0] == pids[1]
    assert pids[2] == pids[3]
    assert pids[0] != pids[2]
    pool.close()
    pool.join()


def test_joblib_elastic_pool_supports_maxtasksperchild(shutdown_only):
    ray.init(num_cpus=1)
    register_ray()

    with joblib.parallel_backend(
        "ray",
        n_jobs=2,
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        maxtasksperchild=1,
    ):
        pids = joblib.Parallel(batch_size=1, pre_dispatch=1)(
            joblib.delayed(os.getpid)() for _ in range(3)
        )

    assert len(set(pids)) == 3


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("max_concurrency", 2),
        ("max_restarts", 1),
        ("max_task_retries", 1),
    ],
)
def test_elastic_pool_rejects_incompatible_actor_options(option, value):
    assert not ray.is_initialized()
    with pytest.raises(ValueError, match=rf"{option}=.*got {value}"):
        Pool(max_size=1, ray_remote_args={option: value})
    assert not ray.is_initialized()


def test_elastic_pool_accepts_serial_nonrestarting_actor_options(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        max_size=1,
        ray_remote_args={
            "max_concurrency": 1,
            "max_restarts": 0,
            "max_task_retries": 0,
        },
    )

    assert pool.apply(abs, (-1,)) == 1
    pool.close()
    pool.join()


def test_elastic_pool_scales_on_submission_and_reaps_on_idle(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=0.05,
        ray_remote_args={"num_cpus": 1},
    )

    results = [pool.apply_async(time.sleep, (0.1,)) for _ in range(6)]
    _wait_for(lambda: _state_count(pool, _ActorSlotState.ACTIVE) == 2)
    assert [result.get() for result in results] == [None] * 6
    _wait_for(lambda: _state_count(pool, _ActorSlotState.EMPTY) == 2)

    pool.close()
    pool.join()


def test_active_pool_scales_deep_burst_across_actors(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(
        min_size=1,
        max_size=2,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    hot_actor_pid = pool.apply(os.getpid)

    def sleep_and_return_pid(delay):
        time.sleep(delay)
        return os.getpid()

    results = [pool.apply_async(sleep_and_return_pid, (0.1,)) for _ in range(3)]
    burst_actor_pids = {result.get(timeout=20) for result in results}

    assert hot_actor_pid in burst_actor_pids
    assert len(burst_actor_pids) == 2
    pool.close()
    pool.join()


def test_elastic_pool_close_preserves_accepted_results(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    results = [pool.apply_async(time.sleep, (0.02,)) for _ in range(4)]

    pool.close()

    assert [result.get() for result in results] == [None] * 4
    with pytest.raises(ValueError, match="Pool not running"):
        pool.apply_async(abs, (-1,))
    pool.join()
    assert _state_count(pool, _ActorSlotState.EMPTY) == 2


def test_elastic_pool_terminate_kills_outstanding_work(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    result = pool.apply_async(time.sleep, (30,))
    _wait_for(lambda: _state_count(pool, _ActorSlotState.ACTIVE) == 1)

    pool.terminate()
    pool.join()

    with pytest.raises(ray.exceptions.RayError):
        result.get()
    assert _state_count(pool, _ActorSlotState.EMPTY) == 1


def test_concurrent_close_and_terminate_converge(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    result = pool.apply_async(time.sleep, (30,))
    _wait_for(lambda: _state_count(pool, _ActorSlotState.ACTIVE) == 1)
    barrier = threading.Barrier(3)
    errors = []

    def run_lifecycle(method):
        try:
            barrier.wait()
            method()
        except BaseException as error:
            errors.append(error)

    threads = [
        threading.Thread(target=run_lifecycle, args=(pool.close,)),
        threading.Thread(target=run_lifecycle, args=(pool.terminate,)),
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=20)
        assert not thread.is_alive()

    assert not errors
    pool.join()
    result.wait(timeout=20)
    assert result.ready()
    assert _state_count(pool, _ActorSlotState.EMPTY) == 1


def test_elastic_pool_recovers_after_actor_death(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )

    with pytest.raises(ray.exceptions.RayError):
        pool.apply(os._exit, (1,))
    _wait_for(lambda: _state_count(pool, _ActorSlotState.EMPTY) == 1)
    assert pool.apply(abs, (-1,)) == 1

    pool.close()
    pool.join()


def test_elastic_pool_retries_after_initializer_failure(shutdown_only, tmp_path):
    ray.init(num_cpus=1)
    marker = tmp_path / "initializer-attempted"

    def fail_once(marker_path):
        if not os.path.exists(marker_path):
            open(marker_path, "w").close()
            raise RuntimeError("initializer failed once")

    pool = Pool(
        min_size=0,
        max_size=1,
        idle_timeout_s=60,
        initializer=fail_once,
        initargs=(str(marker),),
    )

    with pytest.raises(ray.exceptions.RayActorError):
        pool.apply_async(abs, (-1,)).get(timeout=20)
    _wait_for(lambda: _state_count(pool, _ActorSlotState.EMPTY) == 1)
    assert pool.apply(abs, (-2,)) == 2

    pool.close()
    pool.join()


def test_elastic_pool_preserves_exception_values(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)
    callback_values = queue.Queue()

    for returned_exception in (
        ValueError("returned as data"),
        PoolTaskError(ValueError("wrapper returned as data")),
    ):

        def return_exception(value=returned_exception):
            return value

        result = pool.apply_async(return_exception, callback=callback_values.put)
        value = result.get(timeout=10)
        callback_value = callback_values.get(timeout=10)

        assert type(value) is type(returned_exception)
        assert type(callback_value) is type(returned_exception)
        assert result.successful()

    imap_value = next(pool.imap(lambda _: ValueError("imap data"), [None], chunksize=1))
    assert isinstance(imap_value, ValueError)
    assert str(imap_value) == "imap data"
    pool.close()
    pool.join()


def test_map_variants_stop_at_iteration_end(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)

    class UnderreportedIterable:
        def __len__(self):
            return 0

        def __iter__(self):
            return iter([1, 2, 3])

    assert pool.map(abs, UnderreportedIterable(), chunksize=1) == [1, 2, 3]
    assert list(pool.imap(abs, UnderreportedIterable(), chunksize=1)) == [1, 2, 3]
    assert pool.starmap(pow, [(2, 3), (3, 2)], chunksize=1) == [8, 9]
    assert pool.map(abs, [], chunksize=None) == []
    assert list(pool.imap(abs, [], chunksize=None)) == []
    pool.close()
    pool.join()


def test_map_iteration_failure_precedes_all_admission():
    accepted = []

    class RecordingActorSet:
        def submit(self, _func, batch):
            accepted.append(batch)
            return _FakeObjectRef()

    class FailingIterator:
        def __init__(self):
            self._index = 0

        def __iter__(self):
            return self

        def __next__(self):
            self._index += 1
            if self._index == 1:
                return 1
            raise RuntimeError("iterator failed after yielding")

    pool = object.__new__(Pool)
    pool._closed = False
    pool._pool_lock = threading.Lock()
    pool._pool_size = 1
    pool._actor_set = RecordingActorSet()

    with pytest.raises(RuntimeError, match="iterator failed after yielding"):
        pool._chunk_and_run(abs, FailingIterator(), chunksize=1)

    assert accepted == []


def test_map_admission_is_atomic_with_close():
    first_submission = threading.Event()
    release_submission = threading.Event()
    close_finished = threading.Event()
    accepted = []

    class BlockingActorSet:
        def submit(self, _func, batch):
            accepted.append(batch)
            if len(accepted) == 1:
                first_submission.set()
                assert release_submission.wait(1)
            return _FakeObjectRef()

        def close(self):
            pass

    pool = object.__new__(Pool)
    pool._closed = False
    pool._pool_lock = threading.Lock()
    pool._pool_size = 1
    pool._actor_set = BlockingActorSet()
    pool._registry = []
    pool._registry_hashable = {}
    map_result = []

    mapper = threading.Thread(
        target=lambda: map_result.extend(pool._chunk_and_run(abs, [1, 2], chunksize=1))
    )
    closer = threading.Thread(target=lambda: (pool.close(), close_finished.set()))
    mapper.start()
    assert first_submission.wait(1)
    closer.start()
    assert not close_finished.wait(0.05)
    release_submission.set()
    mapper.join()
    closer.join()

    assert len(accepted) == 2
    assert len(map_result) == 2
    assert close_finished.is_set()


def test_callback_failure_does_not_replace_task_result(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)
    callback_called = threading.Event()

    def fail():
        raise ZeroDivisionError("task failed")

    def bad_error_callback(_):
        callback_called.set()
        raise RuntimeError("callback failed")

    result = pool.apply_async(fail, error_callback=bad_error_callback)
    assert callback_called.wait(timeout=30)
    with pytest.raises(ZeroDivisionError, match="task failed"):
        result.get(timeout=30)

    pool.close()
    pool.join()


def test_serialization_failures_do_not_poison_elastic_actor(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)

    with pytest.raises(TypeError):
        pool.apply_async(abs, (threading.Lock(),))
    assert pool.apply(abs, (-2,)) == 2

    def return_unserializable_value():
        return threading.Lock()

    with pytest.raises(ray.exceptions.RayTaskError):
        pool.apply_async(return_unserializable_value).get(timeout=10)
    assert pool.apply(abs, (-3,)) == 3

    pool.close()
    pool.join()


def test_close_with_backlog_preserves_order(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    result = pool.map_async(abs, range(-24, 0), chunksize=1)

    pool.close()
    pool.join()

    assert result.get(timeout=30) == list(range(24, 0, -1))
    assert _state_count(pool, _ActorSlotState.EMPTY) == 2


def test_busy_actor_is_not_reaped_and_idle_capacity_regrows(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=0.05)

    result = pool.apply_async(time.sleep, (0.2,))
    _wait_for(lambda: _state_count(pool, _ActorSlotState.ACTIVE) == 1)
    time.sleep(0.1)
    assert _state_count(pool, _ActorSlotState.ACTIVE) == 1
    assert result.get(timeout=10) is None
    _wait_for(lambda: _state_count(pool, _ActorSlotState.EMPTY) == 1)

    for value in (-1, -2, -3):
        assert pool.apply(abs, (value,)) == abs(value)
        _wait_for(lambda: _state_count(pool, _ActorSlotState.EMPTY) == 1)

    pool.close()
    pool.join()


def test_idle_reaping_preserves_minimum_capacity(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(min_size=1, max_size=2, idle_timeout_s=0.05)

    results = [pool.apply_async(time.sleep, (0.1,)) for _ in range(4)]
    assert [result.get(timeout=20) for result in results] == [None] * 4
    _wait_for(
        lambda: _state_count(pool, _ActorSlotState.ACTIVE) == 1
        and _state_count(pool, _ActorSlotState.EMPTY) == 1
    )

    pool.close()
    pool.join()


def test_concurrent_driver_submissions_have_no_loss_or_duplication(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(
        min_size=0,
        max_size=2,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )
    outputs = [None] * 4
    errors = []

    def submit(thread_index):
        try:
            start = thread_index * 20
            outputs[thread_index] = pool.map(
                abs, range(-start - 20, -start), chunksize=1
            )
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=submit, args=(index,)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert sorted(value for output in outputs for value in output) == list(range(1, 81))
    pool.close()
    pool.join()


def test_capacity_above_available_resources_does_not_strand_work(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(
        min_size=0,
        max_size=4,
        idle_timeout_s=60,
        ray_remote_args={"num_cpus": 1},
    )

    result = pool.map_async(abs, range(-12, 0), chunksize=1)
    try:
        assert result.get(timeout=20) == list(range(12, 0, -1))
    finally:
        pool.terminate()
        pool.join()


def test_imap_error_does_not_stop_later_lazy_submissions(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)

    def maybe_fail(value):
        if value == 1:
            raise ValueError("user task failed")
        return value

    result = pool.imap(maybe_fail, iter(range(3)), chunksize=1)
    assert result.next(timeout=20) == 0
    error = result.next(timeout=20)
    assert isinstance(error, PoolTaskError)
    assert isinstance(error.underlying, ValueError)
    assert result.next(timeout=20) == 2
    with pytest.raises(StopIteration):
        result.next(timeout=20)

    pool.close()
    pool.join()


def test_closing_lazy_imap_settles_collector(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)
    result = pool.imap(abs, iter(range(-3, 0)), chunksize=1)
    _wait_for(lambda: result._result_thread._num_ready == 1, timeout=20)

    pool.close()

    assert result.next(timeout=20) == 3
    rejected = result.next(timeout=20)
    assert isinstance(rejected, PoolTaskError)
    assert isinstance(rejected.underlying, ValueError)
    with pytest.raises(StopIteration):
        result.next(timeout=20)
    _wait_for(lambda: not result._result_thread.is_alive())
    pool.join()


def test_abandoned_imap_releases_collector_thread(shutdown_only):
    ray.init(num_cpus=1)
    pool = Pool(min_size=0, max_size=1, idle_timeout_s=60)
    result = pool.imap(abs, iter(range(-100, 0)), chunksize=1)
    result_thread = result._result_thread
    result_ref = weakref.ref(result)
    assert result.next(timeout=20) == 100

    del result
    gc.collect()

    _wait_for(lambda: result_ref() is None and not result_thread.is_alive(), timeout=20)
    pool.terminate()
    pool.join()


def test_fixed_pool_uses_unified_actor_set(shutdown_only):
    ray.init(num_cpus=2)
    pool = Pool(processes=2)
    callback_values = queue.Queue()

    assert pool._actor_set is not None
    assert pool._actor_set.max_size == 2
    assert pool._actor_set._reaper is None
    assert pool._actor_set.snapshot() == [
        (_ActorSlotState.ACTIVE, 0),
        (_ActorSlotState.ACTIVE, 0),
    ]
    assert pool.map(abs, [-1, -2], chunksize=1) == [1, 2]

    successful = pool.apply_async(abs, (-3,), callback=callback_values.put)
    assert successful.get(timeout=20) == 3
    assert callback_values.get(timeout=20) == 3

    def fail():
        raise ValueError("fixed pool failure")

    failed = pool.apply_async(fail, error_callback=callback_values.put)
    with pytest.raises(ValueError, match="fixed pool failure"):
        failed.get(timeout=20)
    callback_error = callback_values.get(timeout=20)
    assert isinstance(callback_error, ValueError)

    pool.close()
    pool.join()


def test_fixed_pool_waits_for_and_propagates_initializer_failure(shutdown_only):
    ray.init(num_cpus=1)

    def fail_initializer():
        raise RuntimeError("fixed initializer failed")

    with pytest.raises(ray.exceptions.RayActorError, match="fixed initializer failed"):
        Pool(processes=1, initializer=fail_initializer)


def test_fixed_pool_cleans_up_if_initial_readiness_wait_fails(monkeypatch):
    class FailingActorSet:
        terminated = False

        def wait_until_ready(self):
            raise RuntimeError("readiness observation failed")

        def terminate(self):
            self.terminated = True

    actor_set = FailingActorSet()
    monkeypatch.setattr(Pool, "_init_ray", lambda *_args: (1, 1))
    monkeypatch.setattr(
        "ray.util.multiprocessing.pool._ActorSlotSet",
        lambda *_args: actor_set,
    )

    with pytest.raises(RuntimeError, match="readiness observation failed"):
        Pool(processes=1)

    assert actor_set.terminated


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("max_concurrency", 2),
        ("max_restarts", 1),
        ("max_task_retries", 1),
    ],
)
def test_fixed_pool_keeps_advanced_actor_option_compatibility(
    shutdown_only, option, value
):
    ray.init(num_cpus=1)
    pool = Pool(processes=1, ray_remote_args={option: value})

    assert isinstance(pool._actor_set, _LegacyActorSet)
    assert pool.apply(abs, (-1,)) == 1

    pool.close()
    pool.join()


def test_joblib_pool_argument_filter_is_explicit():
    assert _configure_pool_args(
        {
            "min_size": 0,
            "idle_timeout_s": 1,
            "maxtasksperchild": 2,
            "temp_folder": "/joblib-only",
            "context": "spawn",
        }
    ) == {"min_size": 0, "idle_timeout_s": 1, "maxtasksperchild": 2}

    with pytest.raises(TypeError, match="unexpected Pool argument: max_sze"):
        _configure_pool_args({"max_sze": 2})


def test_joblib_uses_n_jobs_as_elastic_ceiling(shutdown_only):
    ray.init(num_cpus=2)
    register_ray()
    original_pickling_pool_bases = PicklingPool.__bases__

    backend = RayBackend(
        min_size=0,
        max_size=8,
        idle_timeout_s=0.05,
        ray_remote_args={"num_cpus": 1},
    )
    assert backend.configure(n_jobs=2) == 2
    assert backend._pool._actor_set.max_size == 2
    backend.terminate()

    with joblib.parallel_backend(
        "ray",
        n_jobs=2,
        min_size=0,
        max_size=8,
        idle_timeout_s=0.05,
        ray_remote_args={"num_cpus": 1},
    ):
        values = joblib.Parallel()(joblib.delayed(abs)(value) for value in range(-4, 0))

    assert values == [4, 3, 2, 1]
    assert PicklingPool.__bases__ == original_pickling_pool_bases


def test_joblib_failure_propagates_and_backend_can_be_reused(shutdown_only):
    ray.init(num_cpus=2)
    register_ray()

    def maybe_fail(value):
        if value == 3:
            raise ValueError("expected joblib failure")
        return value

    with (
        pytest.raises(ValueError, match="expected joblib failure"),
        joblib.parallel_backend(
            "ray",
            n_jobs=2,
            min_size=0,
            max_size=2,
            idle_timeout_s=60,
        ),
    ):
        joblib.Parallel(pre_dispatch=2)(
            joblib.delayed(maybe_fail)(value) for value in range(8)
        )

    with joblib.parallel_backend(
        "ray",
        n_jobs=2,
        min_size=0,
        max_size=2,
        idle_timeout_s=60,
    ):
        values = joblib.Parallel(pre_dispatch=2)(
            joblib.delayed(abs)(value) for value in range(-8, 0)
        )

    assert values == list(range(8, 0, -1))


def test_zero_cpu_head_can_create_pending_pool_demand():
    from ray.cluster_utils import Cluster

    cluster = Cluster()
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    try:
        pool = Pool(
            min_size=0,
            max_size=1,
            idle_timeout_s=60,
            ray_remote_args={"num_cpus": 1},
        )
        result = pool.apply_async(str, ("scaled",))
        assert not result.ready()

        cluster.add_node(num_cpus=1)
        assert result.get(timeout=20) == "scaled"
        pool.close()
        pool.join()
    finally:
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
