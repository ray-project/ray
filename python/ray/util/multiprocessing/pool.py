import collections
import copy
import gc
import itertools
import logging
import os
import queue
import sys
import threading
import time
import weakref
from dataclasses import dataclass
from enum import Enum, auto
from multiprocessing import TimeoutError
from typing import Any, Callable, Dict, Hashable, Iterable, List, Optional, Tuple

import ray
from ray._common.usage import usage_lib
from ray.util import log_once

try:
    from joblib._parallel_backends import SafeFunction
    from joblib.parallel import BatchedCalls, parallel_backend
except ImportError:
    BatchedCalls = None
    parallel_backend = None
    SafeFunction = None


logger = logging.getLogger(__name__)

RAY_ADDRESS_ENV = "RAY_ADDRESS"


def _put_in_dict_registry(
    obj: Any, registry_hashable: Dict[Hashable, ray.ObjectRef]
) -> ray.ObjectRef:
    if obj not in registry_hashable:
        ret = ray.put(obj)
        registry_hashable[obj] = ret
    else:
        ret = registry_hashable[obj]
    return ret


def _put_in_list_registry(
    obj: Any, registry: List[Tuple[Any, ray.ObjectRef]]
) -> ray.ObjectRef:
    try:
        ret = next((ref for o, ref in registry if o is obj))
    except StopIteration:
        ret = ray.put(obj)
        registry.append((obj, ret))
    return ret


def ray_put_if_needed(
    obj: Any,
    registry: Optional[List[Tuple[Any, ray.ObjectRef]]] = None,
    registry_hashable: Optional[Dict[Hashable, ray.ObjectRef]] = None,
) -> ray.ObjectRef:
    """ray.put obj in object store if it's not an ObjRef and bigger than 100 bytes,
    with support for list and dict registries"""
    if isinstance(obj, ray.ObjectRef) or sys.getsizeof(obj) < 100:
        return obj
    ret = obj
    if registry_hashable is not None:
        try:
            ret = _put_in_dict_registry(obj, registry_hashable)
        except TypeError:
            if registry is not None:
                ret = _put_in_list_registry(obj, registry)
    elif registry is not None:
        ret = _put_in_list_registry(obj, registry)
    return ret


def ray_get_if_needed(obj: Any) -> Any:
    """If obj is an ObjectRef, do ray.get, otherwise return obj"""
    if isinstance(obj, ray.ObjectRef):
        return ray.get(obj)
    return obj


if BatchedCalls is not None:

    class RayBatchedCalls(BatchedCalls):
        """Joblib's BatchedCalls with basic Ray object store management

        This functionality is provided through the put_items_in_object_store,
        which uses external registries (list and dict) containing objects
        and their ObjectRefs."""

        def put_items_in_object_store(
            self,
            registry: Optional[List[Tuple[Any, ray.ObjectRef]]] = None,
            registry_hashable: Optional[Dict[Hashable, ray.ObjectRef]] = None,
        ):
            """Puts all applicable (kw)args in self.items in object store

            Takes two registries - list for unhashable objects and dict
            for hashable objects. The registries are a part of a Pool object.
            The method iterates through all entries in items list (usually,
            there will be only one, but the number depends on joblib Parallel
            settings) and puts all of the args and kwargs into the object
            store, updating the registries.
            If an arg or kwarg is already in a registry, it will not be
            put again, and instead, the cached object ref will be used."""
            new_items = []
            for func, args, kwargs in self.items:
                args = [
                    ray_put_if_needed(arg, registry, registry_hashable) for arg in args
                ]
                kwargs = {
                    k: ray_put_if_needed(v, registry, registry_hashable)
                    for k, v in kwargs.items()
                }
                new_items.append((func, args, kwargs))
            self.items = new_items

        def __call__(self):
            # Exactly the same as in BatchedCalls, with the
            # difference being that it gets args and kwargs from
            # object store (which have been put in there by
            # put_items_in_object_store)

            # Set the default nested backend to self._backend but do
            # not set the change the default number of processes to -1
            with parallel_backend(self._backend, n_jobs=self._n_jobs):
                return [
                    func(
                        *[ray_get_if_needed(arg) for arg in args],
                        **{k: ray_get_if_needed(v) for k, v in kwargs.items()},
                    )
                    for func, args, kwargs in self.items
                ]

        def __reduce__(self):
            # Exactly the same as in BatchedCalls, with the
            # difference being that it returns RayBatchedCalls
            # instead
            if self._reducer_callback is not None:
                self._reducer_callback()
            # no need pickle the callback.
            return (
                RayBatchedCalls,
                (self.items, (self._backend, self._n_jobs), None, self._pickle_cache),
            )

else:
    RayBatchedCalls = None


# Helper function to divide a by b and round the result up.
def div_round_up(a, b):
    return -(-a // b)


class PoolTaskError(Exception):
    def __init__(self, underlying):
        self.underlying = underlying


@dataclass(frozen=True)
class _TaskSuccess:
    value: Any


@dataclass(frozen=True)
class _TaskFailure:
    error: BaseException


_ELASTIC_ACTOR_OPTION_DEFAULTS = {
    "max_concurrency": 1,
    "max_restarts": 0,
    "max_task_retries": 0,
}


def _validate_elastic_actor_options(ray_remote_args: Dict[str, Any]) -> None:
    """Reject actor lifecycle options that invalidate elastic slot ownership."""
    for option, required_value in _ELASTIC_ACTOR_OPTION_DEFAULTS.items():
        if option in ray_remote_args and ray_remote_args[option] != required_value:
            raise ValueError(
                f"Elastic Ray Pools require {option}={required_value}; "
                f"got {ray_remote_args[option]!r}"
            )


class _LegacyActorSet:
    """Compatibility adapter for fixed pools with advanced actor options."""

    def __init__(
        self,
        create_actor: Callable[[], Any],
        size: int,
        maxtasksperchild: int,
    ):
        self._create_actor = create_actor
        self._maxtasksperchild = maxtasksperchild
        self._actors = [(create_actor(), 0) for _ in range(size)]
        self._deletion_refs = []
        self._current_index = 0
        ray.get([actor.ping.remote() for actor, _ in self._actors])

    @property
    def max_size(self) -> int:
        return len(self._actors)

    def submit(self, func: Callable, batch: Iterable) -> ray.ObjectRef:
        if self._current_index == self.max_size - 1:
            self._current_index = 0
        else:
            self._current_index += 1
        actor, count = self._actors[self._current_index]
        object_ref = actor.run_batch.remote(func, batch)
        count += 1
        assert self._maxtasksperchild == -1 or count <= self._maxtasksperchild
        if count == self._maxtasksperchild:
            self._stop_actor(actor)
            actor, count = self._create_actor(), 0
        self._actors[self._current_index] = (actor, count)
        return object_ref

    def close(self) -> None:
        for actor, _ in self._actors:
            self._stop_actor(actor)

    def terminate(self) -> None:
        for actor, _ in self._actors:
            ray.kill(actor)

    def join(self) -> None:
        self._wait_for_stopping_actors()

    def _wait_for_stopping_actors(self, timeout=None) -> None:
        if not self._deletion_refs:
            return
        if timeout is not None:
            timeout = float(timeout)
        _, self._deletion_refs = ray.wait(
            self._deletion_refs,
            num_returns=len(self._deletion_refs),
            timeout=timeout,
        )

    def _stop_actor(self, actor) -> None:
        self._wait_for_stopping_actors(timeout=0.0)
        self._deletion_refs.append(actor.__ray_terminate__.remote())


class _ElasticSlotState(Enum):
    EMPTY = auto()
    STARTING = auto()
    ACTIVE = auto()
    DRAINING = auto()


@dataclass
class _ElasticSlot:
    """One bounded actor slot owned by ``_ElasticActorSet``.

    All fields are protected by the actor set's condition. ``EMPTY`` owns no
    actor or refs. Every other state owns an actor. ``STARTING`` may own a
    readiness ref, ``ACTIVE`` owns neither a readiness nor exit ref, and
    ``DRAINING`` may first wait for outstanding batches and then owns an exit
    ref until Ray confirms that the actor has exited. ``STARTING`` without a
    readiness ref is retained only after observation setup failed and the actor
    set has failed closed.
    """

    generation: int = 0
    state: _ElasticSlotState = _ElasticSlotState.EMPTY
    actor: Any = None
    outstanding: int = 0
    tasks_submitted: int = 0
    idle_since: Optional[float] = None
    readiness_ref: Any = None
    exit_ref: Any = None


class _ElasticActorSet:
    """Own actor capacity without taking ownership of tasks or results.

    Ray actor mailboxes remain the task queue and ObjectRefs remain the result
    protocol. This class only chooses an actor, counts its outstanding calls,
    and retires it after an idle deadline or per-actor task limit.
    """

    def __init__(
        self,
        create_actor: Callable[[], Any],
        min_size: int,
        max_size: int,
        idle_timeout_s: float,
        maxtasksperchild: Optional[int] = None,
    ):
        self._create_actor = create_actor
        self._min_size = min_size
        self._idle_timeout_s = idle_timeout_s
        self._maxtasksperchild = maxtasksperchild
        self._slots = [_ElasticSlot() for _ in range(max_size)]
        self._condition = threading.Condition()
        self._closed = False
        self._error: Optional[BaseException] = None

        actors_to_kill = []
        try:
            with self._condition:
                try:
                    for _ in range(min_size):
                        self._create_slot_locked()
                except BaseException:
                    actors_to_kill = [
                        slot.actor for slot in self._slots if slot.actor is not None
                    ]
                    raise
        except BaseException:
            # Construction has no caller-visible owner yet. Freeze the owned
            # handles under the lock, then terminate them without the lock.
            for actor in actors_to_kill:
                try:
                    ray.kill(actor)
                except BaseException:
                    logger.exception(
                        "Failed to clean up a Pool actor after construction failed"
                    )
            raise

        self._reaper = None
        if min_size < max_size:
            self._reaper = threading.Thread(
                target=self._reap_idle_actors,
                name="ray-pool-idle-reaper",
                daemon=True,
            )
            self._reaper.start()

    @property
    def max_size(self) -> int:
        return len(self._slots)

    def wait_until_ready(self) -> None:
        """Wait for initial capacity, preserving fixed Pool construction."""
        with self._condition:
            while any(slot.state is _ElasticSlotState.STARTING for slot in self._slots):
                if self._error is not None:
                    raise self._error
                self._condition.wait()
            if self._error is not None:
                raise self._error

    def submit(self, func: Callable, batch: Iterable) -> ray.ObjectRef:
        """Submit directly to Ray while pending actors only express demand."""
        with self._condition:
            while True:
                self._raise_if_unavailable_locked()
                active = [
                    slot
                    for slot in self._slots
                    if slot.state is _ElasticSlotState.ACTIVE
                ]
                empty = next(
                    (
                        slot
                        for slot in self._slots
                        if slot.state is _ElasticSlotState.EMPTY
                    ),
                    None,
                )
                starting = [
                    slot
                    for slot in self._slots
                    if slot.state is _ElasticSlotState.STARTING
                ]
                capacity = active + starting
                if empty is not None and (
                    not capacity
                    or sum(slot.outstanding for slot in capacity) >= len(capacity)
                ):
                    self._create_slot_locked(empty)
                    active = [
                        slot
                        for slot in self._slots
                        if slot.state is _ElasticSlotState.ACTIVE
                    ]
                    starting = [
                        slot
                        for slot in self._slots
                        if slot.state is _ElasticSlotState.STARTING
                    ]
                capacity = active + starting
                if capacity:
                    # Prefer a hot actor when its queue is only one batch
                    # longer, but assign deeper backlog to new actors so the
                    # current burst can benefit from scale-out. The actor
                    # mailbox still expresses demand without local polling.
                    slot = min(
                        capacity,
                        key=lambda candidate: (
                            candidate.outstanding
                            + (candidate.state is _ElasticSlotState.STARTING),
                            candidate.state is _ElasticSlotState.STARTING,
                        ),
                    )
                    break
                # Every bounded slot is finishing an idle retirement. Waiting
                # for one exit preserves the strict max-size bound.
                self._condition.wait()

            generation = slot.generation
            object_ref = slot.actor.run_batch.remote(func, batch)
            slot.outstanding += 1
            slot.tasks_submitted += 1
            slot.idle_since = None
            try:
                object_ref.future().add_done_callback(
                    lambda future, target=slot, expected=generation: (
                        self._batch_completed(target, expected, future)
                    )
                )
            except BaseException as error:
                # The Ray call was accepted, so decrementing would make an
                # idle retirement unsafe. Fail closed while the slot retains
                # ownership; close/terminate can still reclaim it.
                self._error = error
                self._condition.notify_all()
                logger.exception("Failed to observe a submitted Pool actor call")
            if (
                self._maxtasksperchild is not None
                and slot.tasks_submitted == self._maxtasksperchild
                and slot.state in (_ElasticSlotState.STARTING, _ElasticSlotState.ACTIVE)
            ):
                # Actor calls are serial and the termination request is queued
                # after every accepted batch. Mark the slot as draining now so
                # no later submission can exceed the per-actor task limit.
                self._begin_draining_locked(slot)
            return object_ref

    def close(self) -> None:
        """Reject new calls and queue graceful exit after accepted calls."""
        actors_to_kill = []
        with self._condition:
            if self._closed:
                return
            self._closed = True
            for slot in self._slots:
                if slot.state in (
                    _ElasticSlotState.STARTING,
                    _ElasticSlotState.ACTIVE,
                ):
                    if (
                        slot.state is _ElasticSlotState.STARTING
                        and slot.outstanding == 0
                    ):
                        actors_to_kill.append(slot.actor)
                    self._begin_draining_locked(slot)
            self._condition.notify_all()
        # ray.kill can block on Ray control-plane work. Never call it while
        # holding the condition or from an ObjectRef completion callback.
        for actor in actors_to_kill:
            try:
                ray.kill(actor)
            except BaseException as error:
                with self._condition:
                    self._error = error
                    self._condition.notify_all()

    def terminate(self) -> None:
        """Force actors already moved to DRAINING to exit."""
        self.close()
        with self._condition:
            actors = [
                slot.actor
                for slot in self._slots
                if slot.state is _ElasticSlotState.DRAINING
            ]
        for actor in actors:
            ray.kill(actor)

    def join(self) -> None:
        with self._condition:
            while any(
                slot.state is not _ElasticSlotState.EMPTY for slot in self._slots
            ):
                if self._error is not None:
                    raise RuntimeError(
                        "Elastic Pool actor cleanup failed"
                    ) from self._error
                self._condition.wait()
        if self._reaper is not None:
            self._reaper.join()

    def snapshot(self) -> List[Tuple[_ElasticSlotState, int]]:
        """Return state and outstanding counts for tests and diagnostics."""
        with self._condition:
            return [(slot.state, slot.outstanding) for slot in self._slots]

    def _raise_if_unavailable_locked(self) -> None:
        if self._closed:
            raise ValueError("Pool not running")
        if self._error is not None:
            raise RuntimeError("Elastic Pool actor management failed") from self._error

    def _create_slot_locked(self, slot: Optional[_ElasticSlot] = None) -> _ElasticSlot:
        if slot is None:
            slot = next(
                slot for slot in self._slots if slot.state is _ElasticSlotState.EMPTY
            )
        # Actor creation is an external side effect and may fail synchronously.
        # Do not publish ACTIVE until a handle exists, so the slot remains
        # reusable after a failed submission.
        actor = self._create_actor()
        # Publish ownership immediately after actor creation. Every later
        # failure must retain this slot instead of making it reusable while
        # the actor may still exist.
        slot.generation += 1
        slot.state = _ElasticSlotState.STARTING
        slot.actor = actor
        slot.outstanding = 0
        slot.tasks_submitted = 0
        slot.idle_since = None
        slot.readiness_ref = None
        slot.exit_ref = None
        generation = slot.generation
        try:
            readiness_ref = actor.ping.remote()
            slot.readiness_ref = readiness_ref
            readiness_ref.future().add_done_callback(
                lambda future, target=slot, expected=generation: self._actor_ready(
                    target, expected, future
                )
            )
        except BaseException as error:
            self._error = error
            self._condition.notify_all()
            raise
        return slot

    def _actor_ready(self, slot: _ElasticSlot, generation: int, future) -> None:
        with self._condition:
            if (
                slot.generation != generation
                or slot.state is not _ElasticSlotState.STARTING
            ):
                return
            try:
                error = future.exception()
            except BaseException as callback_error:
                self._error = callback_error
                self._condition.notify_all()
                return
            if error is not None:
                if isinstance(error, ray.exceptions.ActorDiedError):
                    # Ray has confirmed that this actor can no longer execute
                    # work, so its bounded slot is safe to reuse.
                    self._release_dead_actor_locked(slot, error)
                else:
                    # An ObjectRef can fail while its actor remains alive (for
                    # example, if the readiness task is cancelled). Retain the
                    # handle and fail closed until shutdown confirms actor exit.
                    self._error = error
                self._condition.notify_all()
                return
            slot.state = _ElasticSlotState.ACTIVE
            slot.readiness_ref = None
            if slot.outstanding == 0:
                slot.idle_since = time.monotonic()
                self._yield_to_pending_work_locked(slot)
            self._condition.notify_all()

    def _batch_completed(
        self,
        slot: _ElasticSlot,
        generation: int,
        future,
    ) -> None:
        with self._condition:
            if slot.generation != generation or slot.state not in (
                _ElasticSlotState.STARTING,
                _ElasticSlotState.ACTIVE,
                _ElasticSlotState.DRAINING,
            ):
                return
            try:
                error = future.exception()
            except BaseException as callback_error:
                self._error = callback_error
                self._condition.notify_all()
                return
            if isinstance(error, ray.exceptions.ActorDiedError):
                self._release_dead_actor_locked(slot, error)
                self._condition.notify_all()
                return
            if isinstance(error, ray.exceptions.RayActorError):
                # ActorUnavailableError is terminal for this ObjectRef but not
                # proof that the actor has exited. Retain the owned slot and
                # reject later submissions instead of creating duplicate
                # capacity while the actor may recover.
                self._error = error

            slot.outstanding -= 1
            if slot.outstanding < 0:
                self._error = RuntimeError("Pool actor completion count underflow")
            elif slot.outstanding == 0:
                if slot.state is _ElasticSlotState.DRAINING:
                    if slot.exit_ref is None:
                        self._submit_termination_locked(slot)
                elif slot.state is _ElasticSlotState.ACTIVE:
                    slot.idle_since = time.monotonic()
                    self._yield_to_pending_work_locked(slot)
                for pending_slot in self._slots:
                    if (
                        pending_slot.state is _ElasticSlotState.STARTING
                        and pending_slot.outstanding == 0
                    ):
                        self._begin_draining_locked(pending_slot)
            self._condition.notify_all()

    def _yield_to_pending_work_locked(self, slot: _ElasticSlot) -> None:
        if slot.state is not _ElasticSlotState.ACTIVE or slot.outstanding != 0:
            return
        if any(
            candidate.state is _ElasticSlotState.STARTING and candidate.outstanding > 0
            for candidate in self._slots
        ):
            self._begin_draining_locked(slot)

    def _begin_draining_locked(self, slot: _ElasticSlot) -> None:
        assert slot.state in (
            _ElasticSlotState.STARTING,
            _ElasticSlotState.ACTIVE,
        )
        slot.state = _ElasticSlotState.DRAINING
        slot.idle_since = None
        if slot.outstanding > 0 and self._error is None:
            # Ray's special actor termination method is not a completion
            # barrier for previously submitted calls. Wait until every
            # accepted batch has settled before requesting graceful exit.
            return
        # A management failure such as callback registration failure can make
        # the outstanding count permanently unobservable. The Pool is already
        # fail-closed, so prioritize confirmed cleanup over graceful results.
        self._submit_termination_locked(slot, allow_outstanding=self._error is not None)

    def _submit_termination_locked(
        self, slot: _ElasticSlot, allow_outstanding: bool = False
    ) -> None:
        assert slot.state is _ElasticSlotState.DRAINING
        assert allow_outstanding or slot.outstanding == 0
        assert slot.exit_ref is None
        # Readiness success cannot prove actor exit, so even a STARTING actor
        # with no accepted work needs this dedicated termination ref.
        try:
            exit_ref = slot.actor.__ray_terminate__.remote()
        except BaseException as error:
            # Whether the termination request reached Ray is ambiguous. Keep
            # the actor owned by a non-reusable slot and fail the Pool closed.
            self._error = error
            self._condition.notify_all()
            return

        slot.exit_ref = exit_ref
        generation = slot.generation
        try:
            exit_ref.future().add_done_callback(
                lambda future, target=slot, expected=generation: self._actor_exited(
                    target, expected, future
                )
            )
        except BaseException as error:
            self._error = error
            self._condition.notify_all()

    def _actor_exited(self, slot: _ElasticSlot, generation: int, future) -> None:
        with self._condition:
            if (
                slot.generation != generation
                or slot.state is not _ElasticSlotState.DRAINING
            ):
                return
            try:
                error = future.exception()
            except BaseException as callback_error:
                self._error = callback_error
                self._condition.notify_all()
                return
            if error is not None and not isinstance(
                error, ray.exceptions.ActorDiedError
            ):
                # ActorUnavailableError only proves that this termination call
                # has settled. The actor may recover, so keep the slot owned
                # and non-reusable until a separate path confirms its death.
                self._error = error
                self._condition.notify_all()
                return
            self._release_dead_actor_locked(slot)
            self._condition.notify_all()

    def _release_dead_actor_locked(
        self,
        slot: _ElasticSlot,
        startup_error: Optional[BaseException] = None,
    ) -> None:
        """Release one actor whose exit Ray has unambiguously confirmed."""
        previous_state = slot.state
        assert previous_state in (
            _ElasticSlotState.STARTING,
            _ElasticSlotState.ACTIVE,
            _ElasticSlotState.DRAINING,
        )
        self._clear_slot_locked(slot)
        if self._closed:
            return
        if previous_state is _ElasticSlotState.STARTING:
            if self._capacity_locked() < self._min_size:
                # Automatically recreating a permanently failing initializer
                # would create an unbounded actor-start loop. Converge
                # explicitly instead of silently violating the capacity floor.
                assert startup_error is not None
                self._error = startup_error
            return
        self._restore_min_size_locked()

    @staticmethod
    def _clear_slot_locked(slot: _ElasticSlot) -> None:
        slot.state = _ElasticSlotState.EMPTY
        slot.actor = None
        slot.outstanding = 0
        slot.tasks_submitted = 0
        slot.idle_since = None
        slot.readiness_ref = None
        slot.exit_ref = None

    def _restore_min_size_locked(self) -> None:
        if self._closed:
            return
        capacity = self._capacity_locked()
        while capacity < self._min_size:
            empty = next(
                (slot for slot in self._slots if slot.state is _ElasticSlotState.EMPTY),
                None,
            )
            if empty is None:
                return
            try:
                self._create_slot_locked(empty)
            except BaseException as error:
                self._error = error
                return
            capacity += 1

    def _capacity_locked(self) -> int:
        return sum(
            slot.state in (_ElasticSlotState.STARTING, _ElasticSlotState.ACTIVE)
            for slot in self._slots
        )

    def _reap_idle_actors(self) -> None:
        while True:
            with self._condition:
                if self._closed:
                    if all(
                        slot.state is _ElasticSlotState.EMPTY for slot in self._slots
                    ):
                        return
                    self._condition.wait()
                    continue
                if self._error is not None:
                    return

                idle = [
                    slot
                    for slot in self._slots
                    if slot.state is _ElasticSlotState.ACTIVE
                    and slot.outstanding == 0
                    and slot.idle_since is not None
                ]
                excess = max(
                    0,
                    sum(slot.state is _ElasticSlotState.ACTIVE for slot in self._slots)
                    - self._min_size,
                )
                idle.sort(key=lambda slot: slot.idle_since)
                candidates = idle[:excess]
                now = time.monotonic()
                expired = [
                    slot
                    for slot in candidates
                    if now - slot.idle_since >= self._idle_timeout_s
                ]
                for slot in expired:
                    self._begin_draining_locked(slot)
                if expired:
                    continue

                timeout = None
                if candidates:
                    timeout = max(
                        0.0,
                        candidates[0].idle_since + self._idle_timeout_s - now,
                    )
                self._condition.wait(timeout)


class ResultThread(threading.Thread):
    """Thread that collects results from distributed actors.

    It winds down when either:
        - A pre-specified number of objects has been processed
        - When the END_SENTINEL (submitted through self.add_object_ref())
            has been received and all objects received before that have been
            processed.

    Initialize the thread with total_object_refs = float('inf') to wait for the
    END_SENTINEL.

    Args:
        object_refs: ObjectRefs to Ray Actor calls.
            Thread tracks whether they are ready. More ObjectRefs may be added
            with add_object_ref (or _add_object_ref internally) until the object
            count reaches total_object_refs.
        single_result: Should be True if the thread is managing function
            with a single result (like apply_async). False if the thread is managing
            a function with a List of results.
        callback: called only once at the end of the thread
            if no results were errors. If single_result=True, and result is
            not an error, callback is invoked with the result as the only
            argument. If single_result=False, callback is invoked with
            a list of all the results as the only argument.
        error_callback: called only once on the first result
            that errors. Should take an Exception as the only argument.
            If no result errors, this callback is not called.
        total_object_refs: Number of ObjectRefs that this thread
            expects to be ready. May be more than len(object_refs) since
            more ObjectRefs can be submitted after the thread starts.
            If None, defaults to len(object_refs). If float("inf"), thread runs
            until END_SENTINEL (submitted through self.add_object_ref())
            has been received and all objects received before that have
            been processed.
    """

    END_SENTINEL = None

    def __init__(
        self,
        object_refs: list,
        single_result: bool = False,
        callback: callable = None,
        error_callback: callable = None,
        total_object_refs: Optional[int] = None,
    ):
        threading.Thread.__init__(self, daemon=True)
        self._got_error = False
        self._object_refs = []
        self._num_ready = 0
        self._results = []
        self._ready_index_queue = queue.Queue()
        self._single_result = single_result
        self._callback = callback
        self._error_callback = error_callback
        self._total_object_refs = total_object_refs or len(object_refs)
        self._indices = {}
        # Thread-safe queue used to add ObjectRefs to fetch after creating
        # this thread (used to lazily submit for imap and imap_unordered).
        self._new_object_refs = queue.Queue()
        for object_ref in object_refs:
            self._add_object_ref(object_ref)

    def _add_object_ref(self, object_ref):
        self._indices[object_ref] = len(self._object_refs)
        self._object_refs.append(object_ref)
        self._results.append(None)

    def add_object_ref(self, object_ref):
        self._new_object_refs.put(object_ref)

    def run(self):
        unready = copy.copy(self._object_refs)
        aggregated_batch_results = []

        # Run for a specific number of objects if self._total_object_refs is finite.
        # Otherwise, process all objects received prior to the stop signal, given by
        # self.add_object(END_SENTINEL).
        while self._num_ready < self._total_object_refs:
            # Get as many new IDs from the queue as possible without blocking,
            # unless we have no IDs to wait on, in which case we block.
            ready_id = None
            while ready_id is None:
                try:
                    block = len(unready) == 0
                    new_object_ref = self._new_object_refs.get(block=block)
                    if new_object_ref is self.END_SENTINEL:
                        # Receiving the END_SENTINEL object is the signal to stop.
                        # Store the total number of objects.
                        self._total_object_refs = len(self._object_refs)
                        if self._num_ready >= self._total_object_refs:
                            break
                    else:
                        self._add_object_ref(new_object_ref)
                        unready.append(new_object_ref)
                except queue.Empty:
                    # queue.Empty means no result was retrieved if block=False.
                    pass

                # Check if any of the available IDs are done. The timeout is required
                # here to periodically check for new IDs from self._new_object_refs.
                # NOTE(edoakes): the choice of a 100ms timeout here is arbitrary. Too
                # low of a timeout would cause higher overhead from busy spinning and
                # too high would cause higher tail latency to fetch the first result in
                # some cases.
                ready, unready = ray.wait(unready, num_returns=1, timeout=0.1)
                if len(ready) > 0:
                    ready_id = ready[0]

            if ready_id is None:
                break

            try:
                batch = ray.get(ready_id)
            except ray.exceptions.RayError as e:
                batch = [_TaskFailure(e)]

            # The exception callback is called only once on the first result
            # that errors. If no result errors, it is never called.
            callback_error = None
            if not self._got_error:
                for result in batch:
                    if isinstance(result, _TaskFailure):
                        self._got_error = True
                        callback_error = result.error
                        break
                    elif isinstance(result, _TaskSuccess):
                        aggregated_batch_results.append(result.value)
                    # Accept results from actors created by an older version
                    # of this module while preserving their historical error
                    # representation.
                    elif isinstance(result, PoolTaskError):
                        self._got_error = True
                        callback_error = result.underlying
                        break
                    elif isinstance(result, Exception):
                        self._got_error = True
                        callback_error = result
                        break
                    else:
                        aggregated_batch_results.append(result)

            self._num_ready += 1
            self._results[self._indices[ready_id]] = batch
            self._ready_index_queue.put(self._indices[ready_id])
            if callback_error is not None and self._error_callback is not None:
                try:
                    self._error_callback(callback_error)
                except BaseException:
                    logger.exception("Pool error_callback raised")

        # The regular callback is called only once on the entire List of
        # results as long as none of the results were errors. If any results
        # were errors, the regular callback is never called; instead, the
        # exception callback is called on the first erroring result.
        #
        # This callback is called outside the while loop to ensure that it's
        # called on the entire list of results– not just a single batch.
        if not self._got_error and self._callback is not None:
            try:
                if not self._single_result:
                    self._callback(aggregated_batch_results)
                else:
                    # On a thread handling a function with a single result
                    # (e.g. apply_async), we call the callback on just that result
                    # instead of on a list encaspulating that result
                    self._callback(aggregated_batch_results[0])
            except BaseException:
                logger.exception("Pool callback raised")

    def got_error(self):
        # Should only be called after the thread finishes.
        return self._got_error

    def result(self, index):
        # Should only be called on results that are ready.
        return self._results[index]

    def results(self):
        # Should only be called after the thread finishes.
        return self._results

    def next_ready_index(self, timeout=None):
        try:
            return self._ready_index_queue.get(timeout=timeout)
        except queue.Empty:
            # queue.Queue signals a timeout by raising queue.Empty.
            raise TimeoutError


class AsyncResult:
    """An asynchronous interface to task results.

    This should not be constructed directly.
    """

    def __init__(
        self, chunk_object_refs, callback=None, error_callback=None, single_result=False
    ):
        self._single_result = single_result
        self._result_thread = ResultThread(
            chunk_object_refs, single_result, callback, error_callback
        )
        self._result_thread.start()

    def wait(self, timeout: Optional[float] = None):
        """
        Returns once the result is ready or the timeout expires (does not
        raise TimeoutError).

        Args:
            timeout: timeout in milliseconds.
        """

        self._result_thread.join(timeout)

    def get(self, timeout=None):
        self.wait(timeout)
        if self._result_thread.is_alive():
            raise TimeoutError

        results = []
        for batch in self._result_thread.results():
            for result in batch:
                if isinstance(result, _TaskFailure):
                    raise result.error
                if isinstance(result, _TaskSuccess):
                    results.append(result.value)
                elif isinstance(result, PoolTaskError):
                    raise result.underlying
                elif isinstance(result, Exception):
                    raise result
                else:
                    results.append(result)

        if self._single_result:
            return results[0]

        return results

    def ready(self):
        """
        Returns true if the result is ready, else false if the tasks are still
        running.
        """

        return not self._result_thread.is_alive()

    def successful(self):
        """
        Returns true if none of the submitted tasks errored, else false. Should
        only be called once the result is ready (can be checked using `ready`).
        """

        if not self.ready():
            raise ValueError(f"{self!r} not ready")
        return not self._result_thread.got_error()


def _result_for_iterator(result):
    if isinstance(result, _TaskSuccess):
        return result.value
    if isinstance(result, _TaskFailure):
        return PoolTaskError(result.error)
    return result


class IMapIterator:
    """Base class for OrderedIMapIterator and UnorderedIMapIterator."""

    def __init__(self, pool, func, iterable, chunksize=None):
        self._pool = pool
        self._func = func
        self._next_chunk_index = 0
        self._finished_iterating = False
        # List of bools indicating if the given chunk is ready or not for all
        # submitted chunks. Ordering mirrors that in the in the ResultThread.
        self._submitted_chunks = []
        self._ready_objects = collections.deque()
        self._iterator = iter(iterable)
        if chunksize is None:
            try:
                chunksize = pool._calculate_chunksize(iterable)
            except (TypeError, AttributeError):
                chunksize = 1
        self._chunksize = chunksize

        # Iteration end is the only reliable completion signal: custom
        # iterables may report a stale or approximate length.
        self._result_thread = ResultThread([], total_object_refs=float("inf"))
        self._result_thread.start()
        self._result_finalizer = weakref.finalize(
            self,
            self._stop_result_thread,
            self._result_thread,
        )

        for _ in range(self._pool._pool_size):
            self._submit_next_chunk()

    def _submit_next_chunk(self):
        # The full iterable has already been submitted, so no-op.
        if self._finished_iterating:
            return

        chunk_iterator = itertools.islice(self._iterator, self._chunksize)

        # Check whether we have run out of samples.
        # This consumes the original iterator, so we convert to a list and back
        chunk_list = list(chunk_iterator)
        if len(chunk_list) < self._chunksize:
            # Reached end of self._iterator
            self._finished_iterating = True
            if len(chunk_list) == 0:
                self._result_thread.add_object_ref(ResultThread.END_SENTINEL)
                return
        chunk_iterator = iter(chunk_list)

        try:
            new_chunk_id = self._pool._submit_chunk(
                self._func, chunk_iterator, self._chunksize
            )
        except BaseException as error:
            self._finished_iterating = True
            new_chunk_id = ray.put([_TaskFailure(error)])
        self._submitted_chunks.append(False)
        # Wait for the result
        self._result_thread.add_object_ref(new_chunk_id)
        # If we submitted the final chunk, notify the result thread
        if self._finished_iterating:
            self._result_thread.add_object_ref(ResultThread.END_SENTINEL)

    @staticmethod
    def _stop_result_thread(result_thread):
        result_thread.add_object_ref(ResultThread.END_SENTINEL)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        # Should be implemented by subclasses.
        raise NotImplementedError


class OrderedIMapIterator(IMapIterator):
    """Iterator to the results of tasks submitted using `imap`.

    The results are returned in the same order that they were submitted, even
    if they don't finish in that order. Only one batch of tasks per actor
    process is submitted at a time - the rest are submitted as results come in.

    Should not be constructed directly.
    """

    def next(self, timeout=None):
        if len(self._ready_objects) == 0:
            if self._finished_iterating and (
                self._next_chunk_index == len(self._submitted_chunks)
            ):
                # Finish when all chunks have been dispatched and processed
                # Notify the calling process that the work is done.
                raise StopIteration

            # This loop will break when the next index in order is ready or
            # self._result_thread.next_ready_index() raises a timeout.
            index = -1
            while index != self._next_chunk_index:
                start = time.time()
                index = self._result_thread.next_ready_index(timeout=timeout)
                self._submit_next_chunk()
                self._submitted_chunks[index] = True
                if timeout is not None:
                    timeout = max(0, timeout - (time.time() - start))

            while (
                self._next_chunk_index < len(self._submitted_chunks)
                and self._submitted_chunks[self._next_chunk_index]
            ):
                for result in self._result_thread.result(self._next_chunk_index):
                    self._ready_objects.append(_result_for_iterator(result))
                self._next_chunk_index += 1

        return self._ready_objects.popleft()


class UnorderedIMapIterator(IMapIterator):
    """Iterator to the results of tasks submitted using `imap`.

    The results are returned in the order that they finish. Only one batch of
    tasks per actor process is submitted at a time - the rest are submitted as
    results come in.

    Should not be constructed directly.
    """

    def next(self, timeout=None):
        if len(self._ready_objects) == 0:
            if self._finished_iterating and (
                self._next_chunk_index == len(self._submitted_chunks)
            ):
                # Finish when all chunks have been dispatched and processed
                # Notify the calling process that the work is done.
                raise StopIteration

            index = self._result_thread.next_ready_index(timeout=timeout)
            self._submit_next_chunk()

            for result in self._result_thread.result(index):
                self._ready_objects.append(_result_for_iterator(result))
            self._next_chunk_index += 1

        return self._ready_objects.popleft()


@ray.remote(num_cpus=0)
class PoolActor:
    """Actor used to process tasks submitted to a Pool."""

    def __init__(self, initializer=None, initargs=None):
        if initializer:
            initargs = initargs or ()
            initializer(*initargs)

    def ping(self):
        # Used to wait for this actor to be initialized.
        pass

    def run_batch(self, func, batch):
        results = []
        for args, kwargs in batch:
            args = args or ()
            kwargs = kwargs or {}
            try:
                results.append(_TaskSuccess(func(*args, **kwargs)))
            except Exception as e:
                results.append(_TaskFailure(e))
        return results


# https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool
class Pool:
    """A pool of actor processes that is used to process tasks in parallel.

    Args:
        processes: number of actor processes to start in the pool. Defaults to
            the number of cores in the Ray cluster if one is already running,
            otherwise the number of cores on this machine.
        initializer: function to be run in each actor when it starts up.
        initargs: iterable of arguments to the initializer function.
        maxtasksperchild: maximum number of Pool task batches accepted by each
            actor process. After accepting this many tasks, the actor is retired.
            With standard actor lifecycle options, its replacement is created
            after Ray confirms the actor exit and capacity policy requires one.
        context: Accepted for ``multiprocessing.Pool`` API compatibility but
            ignored; Ray controls process initialization. A warning is logged
            if a non-None value is supplied.
        ray_address: address of the Ray cluster to run on. If None, a new local
            Ray cluster will be started on this machine. Otherwise, this will
            be passed to `ray.init()` to connect to a running cluster. This may
            also be specified using the `RAY_ADDRESS` environment variable.
        ray_remote_args: arguments used to configure the Ray Actors making up
            the pool. See :func:`ray.remote` for details. Elastic pools require
            serial, non-restarting actors and reject non-default lifecycle and
            task-retry options.
        min_size: minimum number of actors retained by an elastic pool.
            Supplying any elastic option enables elastic capacity management.
        max_size: maximum number of actor slots in an elastic pool. Defaults
            to ``processes`` when given, otherwise the current cluster CPUs.
        idle_timeout_s: seconds an idle actor remains active before retirement.
    """

    def __init__(
        self,
        processes: Optional[int] = None,
        initializer: Optional[Callable] = None,
        initargs: Optional[Iterable] = None,
        maxtasksperchild: Optional[int] = None,
        context: Any = None,
        ray_address: Optional[str] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        idle_timeout_s: Optional[float] = None,
    ):
        usage_lib.record_library_usage("util.multiprocessing.Pool")

        self._closed = False
        self._pool_lock = threading.Lock()
        if maxtasksperchild is not None and (
            not isinstance(maxtasksperchild, int) or maxtasksperchild <= 0
        ):
            raise ValueError("maxtasksperchild must be a positive integer or None")
        self._registry: List[Tuple[Any, ray.ObjectRef]] = []
        self._registry_hashable: Dict[Hashable, ray.ObjectRef] = {}
        ray_remote_args = ray_remote_args or {}
        self._elastic_actor_set: Optional[_ElasticActorSet] = None
        autoscale = any(
            option is not None for option in (min_size, max_size, idle_timeout_s)
        )
        use_legacy_actor_pool = not autoscale and any(
            option in ray_remote_args and ray_remote_args[option] != required_value
            for option, required_value in _ELASTIC_ACTOR_OPTION_DEFAULTS.items()
        )
        if autoscale:
            _validate_elastic_actor_options(ray_remote_args)

        if context and log_once("context_argument_warning"):
            logger.warning(
                "The 'context' argument is not supported using "
                "ray. Please refer to the documentation for how "
                "to control ray initialization."
            )

        processes, ray_cpus = self._init_ray(processes, ray_address, autoscale)
        if autoscale:
            max_size = max_size if max_size is not None else processes
            max_size = ray_cpus if max_size is None else max_size
            min_size = 0 if min_size is None else min_size
            idle_timeout_s = 60.0 if idle_timeout_s is None else idle_timeout_s
            if max_size <= 0:
                raise ValueError("max_size must be greater than 0")
            if not 0 <= min_size <= max_size:
                raise ValueError("min_size must be between 0 and max_size")
            if idle_timeout_s < 0:
                raise ValueError("idle_timeout_s must be non-negative")
        elif not use_legacy_actor_pool:
            min_size = max_size = processes
            idle_timeout_s = 60.0

        pool_actor = PoolActor.options(**ray_remote_args)

        def create_actor():
            return pool_actor.remote(initializer, initargs)

        if use_legacy_actor_pool:
            self._pool_size = processes
            self._actor_set = _LegacyActorSet(
                create_actor,
                processes,
                maxtasksperchild or -1,
            )
        else:
            self._pool_size = max_size
            self._elastic_actor_set = _ElasticActorSet(
                create_actor,
                min_size,
                max_size,
                idle_timeout_s,
                maxtasksperchild,
            )
            self._actor_set = self._elastic_actor_set
            if not autoscale:
                self._elastic_actor_set.wait_until_ready()

    def _init_ray(self, processes=None, ray_address=None, autoscale=False):
        # Initialize ray. If ray is already initialized, we do nothing.
        # Else, the priority is:
        # ray_address argument > RAY_ADDRESS > start new local cluster.
        if not ray.is_initialized():
            # Cluster mode.
            if ray_address is None and (
                RAY_ADDRESS_ENV in os.environ
                or ray._private.utils.read_ray_address() is not None
            ):
                init_kwargs = {}
                if os.environ.get(RAY_ADDRESS_ENV) == "local":
                    init_kwargs["num_cpus"] = processes
                ray.init(**init_kwargs)
            elif ray_address is not None:
                init_kwargs = {}
                if ray_address == "local":
                    init_kwargs["num_cpus"] = processes
                ray.init(address=ray_address, **init_kwargs)
            # Local mode.
            else:
                ray.init(num_cpus=processes)

        ray_cpus = int(ray._private.state.cluster_resources().get("CPU", 0))
        if not autoscale:
            if processes is None:
                processes = ray_cpus
            if processes <= 0:
                raise ValueError("Processes in the pool must be >0.")
            if ray_cpus < processes:
                raise ValueError(
                    "Tried to start a pool with {} processes on an "
                    "existing ray cluster, but there are only {} "
                    "CPUs in the ray cluster.".format(processes, ray_cpus)
                )

        return processes, ray_cpus

    # Batch should be a list of tuples: (args, kwargs).
    def _run_batch_locked(self, func, batch):
        self._check_running()
        return self._actor_set.submit(func, batch)

    def _run_batch(self, func, batch):
        with self._pool_lock:
            return self._run_batch_locked(func, batch)

    def apply(
        self,
        func: Callable,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
    ):
        """Run the given function on a random actor process and return the
        result synchronously.

        Args:
            func: function to run.
            args: optional arguments to the function.
            kwargs: optional keyword arguments to the function.

        Returns:
            The result.
        """

        return self.apply_async(func, args, kwargs).get()

    def apply_async(
        self,
        func: Callable,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        callback: Callable[[Any], None] = None,
        error_callback: Callable[[Exception], None] = None,
    ):
        """Run the given function on a random actor process and return an
        asynchronous interface to the result.

        Args:
            func: function to run.
            args: optional arguments to the function.
            kwargs: optional keyword arguments to the function.
            callback: callback to be executed on the result once it is finished
                only if it succeeds.
            error_callback: callback to be executed the result once it is
                finished only if the task errors. The exception raised by the
                task will be passed as the only argument to the callback.

        Returns:
            AsyncResult containing the result.
        """

        with self._pool_lock:
            self._check_running()
            func = self._convert_to_ray_batched_calls_if_needed(func)
            object_ref = self._run_batch_locked(func, [(args, kwargs)])
        return AsyncResult([object_ref], callback, error_callback, single_result=True)

    def _convert_to_ray_batched_calls_if_needed(self, func: Callable) -> Callable:
        """Convert joblib's BatchedCalls to RayBatchedCalls for ObjectRef caching.

        This converts joblib's BatchedCalls callable, which is a collection of
        functions with their args and kwargs to be ran sequentially in an
        Actor, to a RayBatchedCalls callable, which provides identical
        functionality in addition to a method which ensures that common
        args and kwargs are put into the object store just once, saving time
        and memory. That method is then ran.

        If func is not a BatchedCalls instance, it is returned without changes.

        The ObjectRefs are cached inside two registries (_registry and
        _registry_hashable), which are common for the entire Pool and are
        cleaned on close."""
        if RayBatchedCalls is None:
            return func
        orginal_func = func
        # SafeFunction is a Python 2 leftover and can be
        # safely removed.
        if isinstance(func, SafeFunction):
            func = func.func
        if isinstance(func, BatchedCalls):
            func = RayBatchedCalls(
                func.items,
                (func._backend, func._n_jobs),
                func._reducer_callback,
                func._pickle_cache,
            )
            # go through all the items and replace args and kwargs with
            # ObjectRefs, caching them in registries
            func.put_items_in_object_store(self._registry, self._registry_hashable)
        else:
            func = orginal_func
        return func

    def _calculate_chunksize(self, iterable):
        chunksize, extra = divmod(len(iterable), self._pool_size * 4)
        if extra:
            chunksize += 1
        return max(chunksize, 1)

    def _submit_chunk(self, func, iterator, chunksize, unpack_args=False):
        chunk = []
        while len(chunk) < chunksize:
            try:
                args = next(iterator)
                if not unpack_args:
                    args = (args,)
                chunk.append((args, {}))
            except StopIteration:
                break

        if not chunk:
            return None

        return self._run_batch(func, chunk)

    def _chunk_and_run(self, func, iterable, chunksize=None, unpack_args=False):
        if chunksize is None:
            try:
                chunksize = self._calculate_chunksize(iterable)
            except (TypeError, AttributeError):
                chunksize = 1

        iterator = iter(iterable)
        chunks = []
        while True:
            chunk = []
            while len(chunk) < chunksize:
                try:
                    args = next(iterator)
                except StopIteration:
                    break
                if not unpack_args:
                    args = (args,)
                chunk.append((args, {}))
            if not chunk:
                break
            chunks.append(chunk)

        # Input iteration is complete before the admission transaction begins.
        # close() therefore observes either no submissions or the complete map.
        with self._pool_lock:
            self._check_running()
            return [self._run_batch_locked(func, chunk) for chunk in chunks]

    def _map_async(
        self,
        func,
        iterable,
        chunksize=None,
        unpack_args=False,
        callback=None,
        error_callback=None,
    ):
        self._check_running()
        object_refs = self._chunk_and_run(
            func, iterable, chunksize=chunksize, unpack_args=unpack_args
        )
        return AsyncResult(object_refs, callback, error_callback)

    def map(self, func: Callable, iterable: Iterable, chunksize: Optional[int] = None):
        """Run the function on each iterable element across the actor processes.

        Args:
            func: function to run.
            iterable: iterable of objects to be passed as the sole argument to
                func.
            chunksize: number of tasks to submit as a batch to each actor
                process. If unspecified, a suitable chunksize will be chosen.

        Returns:
            A list of results.
        """

        return self._map_async(
            func, iterable, chunksize=chunksize, unpack_args=False
        ).get()

    def map_async(
        self,
        func: Callable,
        iterable: Iterable,
        chunksize: Optional[int] = None,
        callback: Callable[[List], None] = None,
        error_callback: Callable[[Exception], None] = None,
    ):
        """Run the function on each iterable element across the actor processes
        and return an asynchronous interface to the results.

        Args:
            func: function to run.
            iterable: iterable of objects to be passed as the only argument to
                func.
            chunksize: number of tasks to submit as a batch to each actor
                process. If unspecified, a suitable chunksize will be chosen.
            callback: Will only be called if none of the results were errors,
                and will only be called once after all results are finished.
                A Python List of all the finished results will be passed as the
                only argument to the callback.
            error_callback: callback executed on the first errored result.
                The Exception raised by the task will be passed as the only
                argument to the callback.

        Returns:
            AsyncResult
        """
        return self._map_async(
            func,
            iterable,
            chunksize=chunksize,
            unpack_args=False,
            callback=callback,
            error_callback=error_callback,
        )

    def starmap(self, func, iterable, chunksize=None):
        """Same as `map`, but unpacks each element of the iterable as the
        arguments to func like: [func(*args) for args in iterable].
        """

        return self._map_async(
            func, iterable, chunksize=chunksize, unpack_args=True
        ).get()

    def starmap_async(
        self,
        func: Callable,
        iterable: Iterable,
        callback: Callable[[List], None] = None,
        error_callback: Callable[[Exception], None] = None,
    ):
        """Same as `map_async`, but unpacks each element of the iterable as the
        arguments to func like: [func(*args) for args in iterable].
        """

        return self._map_async(
            func,
            iterable,
            unpack_args=True,
            callback=callback,
            error_callback=error_callback,
        )

    def imap(self, func: Callable, iterable: Iterable, chunksize: Optional[int] = 1):
        """Same as `map`, but only submits one batch of tasks to each actor
        process at a time.

        This can be useful if the iterable of arguments is very large or each
        task's arguments consumes a large amount of resources.

        The results are returned in the order corresponding to their arguments
        in the iterable.

        Args:
            func: Function to apply to each element of ``iterable``.
            iterable: Iterable of arguments to ``func``.
            chunksize: Number of elements to send to each worker per batch.

        Returns:
            OrderedIMapIterator
        """

        self._check_running()
        return OrderedIMapIterator(self, func, iterable, chunksize=chunksize)

    def imap_unordered(
        self, func: Callable, iterable: Iterable, chunksize: Optional[int] = 1
    ):
        """Same as `map`, but only submits one batch of tasks to each actor
        process at a time.

        This can be useful if the iterable of arguments is very large or each
        task's arguments consumes a large amount of resources.

        The results are returned in the order that they finish.

        Args:
            func: Function to apply to each element of ``iterable``.
            iterable: Iterable of arguments to ``func``.
            chunksize: Number of elements to send to each worker per batch.

        Returns:
            UnorderedIMapIterator
        """

        self._check_running()
        return UnorderedIMapIterator(self, func, iterable, chunksize=chunksize)

    def _check_running(self):
        if self._closed:
            raise ValueError("Pool not running")

    def __enter__(self):
        self._check_running()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()

    def close(self):
        """Close the pool.

        Prevents any more tasks from being submitted on the pool but allows
        outstanding work to finish.
        """

        with self._pool_lock:
            if self._closed:
                return
            self._registry.clear()
            self._registry_hashable.clear()
            self._closed = True
            self._actor_set.close()
        gc.collect()

    def terminate(self):
        """Close the pool.

        Prevents any more tasks from being submitted on the pool and stops
        outstanding work.
        """

        if not self._closed:
            self.close()
        self._actor_set.terminate()

    def join(self):
        """Wait for the actors in a closed pool to exit.

        If the pool was closed using `close`, this will return once all
        outstanding work is completed.

        If the pool was closed using `terminate`, this will return quickly.
        """

        if not self._closed:
            raise ValueError("Pool is still running")
        self._actor_set.join()
