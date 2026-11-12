import asyncio
import functools
import inspect
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Hashable, List, Optional, Set

from ray.serve import metrics
from ray.serve._private.common import ReplicaID, RequestRoutingInfo
from ray.serve._private.constants import (
    MODEL_LOAD_LATENCY_BUCKETS_MS,
    PUSH_MULTIPLEXED_MODEL_IDS_INTERVAL_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.metrics_utils import MetricsPusher
from ray.serve._private.usage import ServeUsageTag
from ray.serve.context import (
    _get_global_client,
    _get_internal_replica_context,
    _serve_batch_request_context,
    _serve_request_context,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class _ModelMultiplexWrapper:
    """A wrapper class that wraps the model load function and
    provides the LRU caching functionality.

    The model multiplexer is a wrapper class that wraps the model load function
    and provides the LRU caching functionality, and the model load function should
    be a coroutine function that takes the model ID as the first argument and
    returns the user-constructed model object.
    The model multiplexer will also ensure that the number of models on the current
    replica does not exceed the specified limit.
    The model will be unloaded in the LRU order, the model multiplexer will call the
    model's __del__ attribute if it exists to clean up the model resources eagerly.

    A model looked up inside a request is pinned until that request finishes.
    Pinned models are not unloaded: a cache miss on a full replica unloads the
    least recently used unpinned model, or waits until a request finishes.
    While a miss waits, the least recently used model takes no new requests.

    """

    _PUSH_MULTIPLEXED_MODEL_IDS_TASK_NAME = "push_multiplexed_model_ids"

    def __init__(
        self,
        model_load_func: Callable[[str], Any],
        self_arg: Any,
        max_num_models_per_replica: int,
    ):
        """Initialize the model multiplexer.
        Args:
            model_load_func: the model load async function.
            self_arg: self argument when model_load_func is class method.
            max_num_models_per_replica: the maximum number of models to be loaded on the
                current replica. If it is -1, there is no limit for the number of models
                per replica.
        """

        ServeUsageTag.MULTIPLEXED_API_USED.record("1")

        self.models = OrderedDict()
        self._func: Callable = model_load_func
        self.self_arg: Any = self_arg
        self.max_num_models_per_replica: int = max_num_models_per_replica

        # log MODEL_LOAD_LATENCY_BUCKET_MS
        logger.debug(f"MODEL_LOAD_LATENCY_BUCKET_MS: {MODEL_LOAD_LATENCY_BUCKETS_MS}")
        self.model_load_latency_ms = metrics.Histogram(
            "serve_multiplexed_model_load_latency_ms",
            description="The time it takes to load a model.",
            boundaries=MODEL_LOAD_LATENCY_BUCKETS_MS,
        )
        self.model_unload_latency_ms = metrics.Histogram(
            "serve_multiplexed_model_unload_latency_ms",
            description="The time it takes to unload a model.",
            boundaries=MODEL_LOAD_LATENCY_BUCKETS_MS,
        )
        self.num_models_gauge = metrics.Gauge(
            "serve_num_multiplexed_models",
            description="The number of models loaded on the current replica.",
        )

        self.registered_model_gauge = metrics.Gauge(
            "serve_registered_multiplexed_model_id",
            description="The model id registered on the current replica.",
            tag_keys=("model_id",),
        )
        self.get_model_requests_counter = metrics.Counter(
            "serve_multiplexed_get_model_requests_counter",
            description="The counter for get model requests on the current replica.",
        )
        self.models_unload_counter = metrics.Counter(
            "serve_multiplexed_models_unload_counter",
            description="The counter for unloaded models on the current replica.",
        )
        self.models_load_counter = metrics.Counter(
            "serve_multiplexed_models_load_counter",
            description="The counter for loaded models on the current replica.",
        )

        context = _get_internal_replica_context()
        if context is None:
            raise RuntimeError(
                "`@serve.multiplex` can only be used within a deployment "
                "(failed to retrieve Serve replica context)."
            )

        self._app_name: str = context.app_name
        self._deployment_name: str = context.deployment
        self._replica_id: ReplicaID = context.replica_id

        # Whether to push the multiplexed replica info to the controller.
        self._push_multiplexed_replica_info: bool = False

        # Model cache lock to ensure that only one model is loading/unloading at a time.
        self._model_cache_lock = asyncio.Lock()
        # The set of model IDs that are being loaded. This is used to early push
        # model ids info to the controller. The tasks will be added when there is cache
        # miss, and will be removed when the model is loaded successfully or
        # failed to load.
        self._model_load_tasks: Set[str] = set()

        # Pin owner -> IDs of the cached models it pins. An owner is a future
        # or task (see `_current_pin_owner`); its pins are dropped when it is done.
        self._pinned_models: Dict[Hashable, Set[str]] = {}
        # One entry per lookup waiting for a model to be unpinned.
        self._room_waiters: List[_RoomWaiter] = []

        self.metrics_pusher = MetricsPusher()
        self.metrics_pusher.register_or_update_task(
            self._PUSH_MULTIPLEXED_MODEL_IDS_TASK_NAME,
            self._push_model_ids_info,
            PUSH_MULTIPLEXED_MODEL_IDS_INTERVAL_S,
        )
        self.metrics_pusher.start()

    def _get_loading_and_loaded_model_ids(self) -> List[str]:
        """Get the model IDs of the loaded models & loading models in the replica.
        This is to push the model id information early to the controller, so that
        requests can be routed to the replica.
        """
        models_list = set(self.models.keys())
        models_list.update(self._model_load_tasks)
        return list(models_list)

    def _push_model_ids_info(self):
        """Push the multiplexed replica info to the controller."""
        try:
            self.num_models_gauge.set(len(self.models))

            for model_id in self.models:
                self.registered_model_gauge.set(1, tags={"model_id": model_id})

            if self._push_multiplexed_replica_info:
                _get_global_client().record_request_routing_info(
                    RequestRoutingInfo(
                        replica_id=self._replica_id,
                        multiplexed_model_ids=self._get_loading_and_loaded_model_ids(),
                    )
                )
                self._push_multiplexed_replica_info = False
        except Exception as e:
            logger.warning(
                "Failed to push the multiplexed replica info "
                f"to the controller. Error: {e}"
            )

    async def shutdown(self):
        """Unload all the models when the model multiplexer is deleted."""
        while len(self.models) > 0:
            try:
                await self.unload_model_lru()
            except Exception as e:
                logger.exception(
                    f"Failed to unload model. Error: {e}",
                )

    async def load_model(self, model_id: str) -> Any:
        """Load the model if it is not loaded yet, and return
            the user-constructed model object.

        The model is pinned for the request that is calling, see
        `_current_pin_owner`.

        Args:
            model_id: the model ID.

        Returns:
            The user-constructed model object.
        """

        if not isinstance(model_id, str):
            raise TypeError("The model ID must be a string.")

        if not model_id:
            raise ValueError("The model ID cannot be empty.")

        self.get_model_requests_counter.inc()
        owner = self._current_pin_owner()

        if self._can_use_cached(model_id, owner):
            # Move the model to the end of the OrderedDict to mark it as
            # most-recently-used. Using move_to_end() instead of pop()+reinsert
            # avoids a race condition where concurrent coroutines could see the
            # key as missing during the brief window between pop and reinsert.
            self.models.move_to_end(model_id)
            return self._pin(model_id, owner)

        # Set the flag to push the multiplexed replica info to the controller
        # before loading the model. This is to make sure we can push the model
        # id info to the controller/router early, so that requests can be routed to
        # the replica.
        self._push_multiplexed_replica_info = True
        self._model_load_tasks.add(model_id)
        try:
            return await self._load_when_there_is_room(model_id, owner)
        finally:
            self._model_load_tasks.discard(model_id)

    async def _load_when_there_is_room(self, model_id: str, owner: Hashable) -> Any:
        """Load and pin the model, first waiting for room in the cache."""
        while True:
            async with self._model_cache_lock:
                # Check if the model has been loaded by another request.
                if self._can_use_cached(model_id, owner):
                    return self._pin(model_id, owner)
                if model_id not in self.models and await self._make_room(owner):
                    await self._load_into_cache(model_id)
                    return self._pin(model_id, owner)
                # Registered before the lock is released, so no wake-up is missed.
                waiter = self._add_waiter(model_id, owner)
            try:
                await waiter.cache_changed
            finally:
                self._room_waiters.remove(waiter)
                if waiter.cache_changed.cancelled():
                    # Lookups held back for this waiter can go ahead.
                    self._wake_waiters()

    def _add_waiter(self, model_id: str, owner: Hashable) -> "_RoomWaiter":
        """Register a lookup that waits for pins to be dropped."""
        # A waiting miss drains the least recently used model.
        draining_model = None if model_id in self.models else next(iter(self.models))
        cache_changed = asyncio.get_running_loop().create_future()
        waiter = _RoomWaiter(owner, cache_changed, draining_model)
        self._room_waiters.append(waiter)
        return waiter

    def _can_use_cached(self, model_id: str, owner: Hashable) -> bool:
        """Return whether a lookup by `owner` may take the model from the cache.

        A model that a waiting miss drains takes no new pin owners, so that a
        busy model cannot starve the miss.
        """
        if model_id not in self.models:
            return False
        draining = {waiter.draining_model for waiter in self._room_waiters}
        return (
            model_id not in draining
            or owner is None
            or model_id in self._pinned_models.get(owner, ())
            or not self._other_owners_will_unpin(owner)
        )

    async def _make_room(self, owner: Hashable) -> bool:
        """Unload one model if the cache is full. Return whether there is room."""
        if (
            self.max_num_models_per_replica <= 0
            or len(self.models) < self.max_num_models_per_replica
        ):
            return True
        victim = self._pick_model_to_unload(owner)
        if victim is None:
            return False
        await self._unload_model(victim)
        self._push_multiplexed_replica_info = True
        return True

    def _pick_model_to_unload(self, owner: Hashable) -> Optional[str]:
        """Return the model ID to unload for a lookup by `owner`, or None to wait."""
        pinned = set().union(*self._pinned_models.values())
        for model_id in self.models:
            if model_id not in pinned:
                return model_id

        if self._other_owners_will_unpin(owner):
            return None

        # Only blocked requests hold pins, because they use more models than
        # the replica may hold. Unload in LRU order, preferring the caller's.
        own_models = self._pinned_models.get(owner, set())
        only_own = [
            model_id
            for model_id in self.models
            if model_id in own_models and self._pin_count(model_id) == 1
        ]
        victim = only_own[0] if only_own else next(iter(self.models))
        logger.warning(
            f"Unloading model '{victim}' while a request still pins it: the "
            "requests in flight need more models at once than "
            f"max_num_models_per_replica={self.max_num_models_per_replica}."
        )
        return victim

    def _other_owners_will_unpin(self, owner: Hashable) -> bool:
        """Return whether waiting can end without unloading a pinned model.

        A pin is released only when its owner finishes, and an owner with a
        waiting lookup cannot finish. So waiting ends only if an owner other
        than `owner` holds a pin and has no waiting lookup.
        """
        blocked = {waiter.owner for waiter in self._room_waiters} | {owner}
        return any(
            models and pin_owner not in blocked
            for pin_owner, models in self._pinned_models.items()
        )

    def _pin_count(self, model_id: str) -> int:
        return sum(model_id in pinned for pinned in self._pinned_models.values())

    async def _load_into_cache(self, model_id: str) -> None:
        """Call the user's load function and cache its result."""
        logger.debug(f"Loading model '{model_id}'.")
        self.models_load_counter.inc()
        load_start_time = time.time()
        try:
            if self.self_arg is None:
                self.models[model_id] = await self._func(model_id)
            else:
                self.models[model_id] = await self._func(self.self_arg, model_id)
        except Exception as e:
            logger.error(f"Failed to load model '{model_id}'. Error: {e}")
            raise e
        load_latency_ms = (time.time() - load_start_time) * 1000.0
        logger.debug(
            f"Successfully loaded model '{model_id}' in {load_latency_ms:.1f}ms."
        )
        self.model_load_latency_ms.observe(load_latency_ms)

    def _current_pin_owner(self) -> Optional[Hashable]:
        """Return the object whose completion ends the caller's use of a model.

        Inside a batch this is the asyncio task running the batch. Inside a
        request it is the future the replica resolves when the request
        finishes, which child tasks of the request share. Other lookups return
        None and are not pinned.
        """
        if _serve_batch_request_context.get():
            return asyncio.current_task()
        request_finished = getattr(_serve_request_context.get(), "_finished", None)
        # A task that outlives the request that created it keeps its context.
        if request_finished is None or request_finished.done():
            return None
        return request_finished

    def _pin(self, model_id: str, owner: Hashable) -> Any:
        """Pin the cached model for `owner` and return the model."""
        if owner is not None:
            if owner not in self._pinned_models:
                self._pinned_models[owner] = set()
                # A request future resolves on the replica's event loop thread.
                unpin = functools.partial(
                    asyncio.get_running_loop().call_soon_threadsafe,
                    self._unpin_models_of,
                )
                owner.add_done_callback(unpin)
            self._pinned_models[owner].add(model_id)
        return self.models[model_id]

    def _unpin_models_of(self, owner: Hashable) -> None:
        """Drop every pin of a finished owner and wake the waiting lookups."""
        self._pinned_models.pop(owner, None)
        self._wake_waiters()

    def _wake_waiters(self) -> None:
        """Make every waiting lookup look at the cache again."""
        for waiter in self._room_waiters:
            if not waiter.cache_changed.done():
                waiter.cache_changed.set_result(None)

    async def unload_model_lru(self) -> None:
        """Unload the least recently used model, pinned or not."""
        await self._unload_model(next(iter(self.models)))

    async def _unload_model(self, model_id: str) -> None:
        """Remove the model from the cache and run its cleanup hook."""

        self.models_unload_counter.inc()
        unload_start_time = time.time()
        model = self.models.pop(model_id)
        for pinned in self._pinned_models.values():
            pinned.discard(model_id)
        # Lookups held back from a draining model look at the cache again.
        self._wake_waiters()
        logger.debug(f"Unloading model '{model_id}'.")

        # If the model has __del__ attribute, call it.
        # This is to clean up the model resources eagerly.
        if hasattr(model, "__del__"):
            if not inspect.iscoroutinefunction(model.__del__):
                await asyncio.get_running_loop().run_in_executor(None, model.__del__)
            else:
                await model.__del__()
            model.__del__ = lambda _: None
        unload_latency_ms = (time.time() - unload_start_time) * 1000.0
        self.model_unload_latency_ms.observe(unload_latency_ms)
        logger.debug(
            f"Successfully unloaded model '{model_id}' in {unload_latency_ms:.1f}ms."
        )
        self.registered_model_gauge.set(0, tags={"model_id": model_id})


@dataclass(frozen=True)
class _RoomWaiter:
    """A lookup that waits for a pinned model to be unpinned."""

    # Pin owner of the waiting lookup, None if the lookup is not pinned.
    owner: Hashable
    # Resolved when pins are dropped or a model is unloaded.
    cache_changed: asyncio.Future
    # Model a waiting miss unloads once it is unpinned. None for a lookup that
    # is held back from such a model.
    draining_model: Optional[str]
