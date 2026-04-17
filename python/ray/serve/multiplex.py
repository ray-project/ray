import asyncio
import inspect
import logging
import time
from collections import OrderedDict
from typing import Any, Callable, List, Set

from ray.serve import metrics
from ray.serve._private.common import ReplicaID, RequestRoutingInfo
from ray.serve._private.constants import (
    MULTIPLEXED_LOAD_LATENCY_BUCKETS_MS,
    PUSH_MULTIPLEXED_MODEL_IDS_INTERVAL_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.metrics_utils import MetricsPusher
from ray.serve._private.usage import ServeUsageTag
from ray.serve.context import _get_global_client, _get_internal_replica_context

logger = logging.getLogger(SERVE_LOGGER_NAME)


class _MultiplexWrapper:
    """Wraps a user-defined loader for a single multiplex dimension and
    provides LRU caching of its loaded entries.

    Each multiplex dimension (e.g. "model", "session") gets its own wrapper
    instance with its own independent LRU cache. The wrapper periodically
    reports its cached IDs (keyed by dimension name) to the controller so
    the router can make dimension-aware routing decisions.
    """

    _PUSH_MULTIPLEXED_MODEL_IDS_TASK_NAME = "push_multiplexed_model_ids"

    def __init__(
        self,
        load_func: Callable[[str], Any],
        self_arg: Any,
        name: str,
        size_per_replica: int,
        multiplex_model_legacy_metrics: bool = False,
    ):
        """Initialize the multiplex wrapper.

        Args:
            load_func: the async load function.
            self_arg: self argument when load_func is a class method.
            name: the multiplex dimension name (e.g. "model", "session").
            size_per_replica: the maximum number of entries to cache on the
                current replica. If -1, the cache size is unbounded.
            multiplex_model_legacy_metrics: if True, emit the original multiplexed model
                metric names (e.g. `serve_multiplexed_model_load_latency_ms`) without
                a `dimension` tag. Used for the deprecated `max_num_models_per_replica`
                path to preserve dashboard compatibility. If False, emit the generalized
                metric names (e.g. `serve_multiplexed_load_latency_ms`) with a `dimension`
                tag so multiple dimensions share one series.
        """

        ServeUsageTag.MULTIPLEXED_API_USED.record("1")
        self._name = name
        self._multiplex_model_legacy_metrics = multiplex_model_legacy_metrics

        # OrderedDict used as LRU cache of loaded entries (id -> user object).
        self.entries: OrderedDict[str, Any] = OrderedDict()
        self._func: Callable = load_func
        self.self_arg: Any = self_arg
        self.size_per_replica: int = size_per_replica

        logger.debug(
            f"MULTIPLEXED_LOAD_LATENCY_BUCKETS_MS: {MULTIPLEXED_LOAD_LATENCY_BUCKETS_MS}"
        )
        if multiplex_model_legacy_metrics:
            self._metric_dimension_tag: dict = {}
            self.load_latency_ms = metrics.Histogram(
                "serve_multiplexed_model_load_latency_ms",
                description="The time it takes to load a model.",
                boundaries=MULTIPLEXED_LOAD_LATENCY_BUCKETS_MS,
            )
            self.unload_latency_ms = metrics.Histogram(
                "serve_multiplexed_model_unload_latency_ms",
                description="The time it takes to unload a model.",
                boundaries=MULTIPLEXED_LOAD_LATENCY_BUCKETS_MS,
            )
            self.num_entries_gauge = metrics.Gauge(
                "serve_num_multiplexed_models",
                description="The number of models loaded on the current replica.",
            )
            self.registered_entry_gauge = metrics.Gauge(
                "serve_registered_multiplexed_model_id",
                description="The model id registered on the current replica.",
                tag_keys=("model_id",),
            )
            self.get_entry_requests_counter = metrics.Counter(
                "serve_multiplexed_get_model_requests_counter",
                description=(
                    "The counter for get model requests on the current replica."
                ),
            )
            self.entries_unload_counter = metrics.Counter(
                "serve_multiplexed_models_unload_counter",
                description=("The counter for unloaded models on the current replica."),
            )
            self.entries_load_counter = metrics.Counter(
                "serve_multiplexed_models_load_counter",
                description=("The counter for loaded models on the current replica."),
            )
        else:
            self._metric_dimension_tag = {"dimension": name}
            self.load_latency_ms = metrics.Histogram(
                "serve_multiplexed_load_latency_ms",
                description="The time it takes to load a multiplexed entry.",
                boundaries=MULTIPLEXED_LOAD_LATENCY_BUCKETS_MS,
                tag_keys=("dimension",),
            )
            self.unload_latency_ms = metrics.Histogram(
                "serve_multiplexed_unload_latency_ms",
                description="The time it takes to unload a multiplexed entry.",
                boundaries=MULTIPLEXED_LOAD_LATENCY_BUCKETS_MS,
                tag_keys=("dimension",),
            )
            self.num_entries_gauge = metrics.Gauge(
                "serve_multiplexed_num_entries",
                description="Number of entries cached on the current replica.",
                tag_keys=("dimension",),
            )
            self.registered_entry_gauge = metrics.Gauge(
                "serve_multiplexed_registered_id",
                description="The id registered on the current replica.",
                tag_keys=("dimension", "id"),
            )
            self.get_entry_requests_counter = metrics.Counter(
                "serve_multiplexed_get_requests_counter",
                description=(
                    "The counter for get-entry requests on the current replica."
                ),
                tag_keys=("dimension",),
            )
            self.entries_unload_counter = metrics.Counter(
                "serve_multiplexed_unload_counter",
                description=(
                    "The counter for unloaded entries on the current replica."
                ),
                tag_keys=("dimension",),
            )
            self.entries_load_counter = metrics.Counter(
                "serve_multiplexed_load_counter",
                description=("The counter for loaded entries on the current replica."),
                tag_keys=("dimension",),
            )

        context = _get_internal_replica_context()
        if context is None:
            raise RuntimeError(
                "`@serve.multiplexed` can only be used within a deployment "
                "(failed to retrieve Serve replica context)."
            )

        self._app_name: str = context.app_name
        self._deployment_name: str = context.deployment
        self._replica_id: ReplicaID = context.replica_id

        # Whether to push the multiplexed replica info to the controller.
        self._push_replica_info: bool = False

        # Cache lock to ensure that only one entry is loading/unloading at a time.
        self._cache_lock = asyncio.Lock()
        # The set of IDs that are being loaded. This is used to push IDs info
        # to the controller early so that requests can be routed to the replica
        # before the load completes.
        self._load_tasks: Set[str] = set()

        self.metrics_pusher = MetricsPusher()
        self.metrics_pusher.register_or_update_task(
            self._PUSH_MULTIPLEXED_MODEL_IDS_TASK_NAME,
            self._push_ids_info,
            PUSH_MULTIPLEXED_MODEL_IDS_INTERVAL_S,
        )
        self.metrics_pusher.start()

    def _get_loading_and_loaded_ids(self) -> List[str]:
        """Get the IDs of loaded + currently-loading entries on this replica.

        This is used to push IDs info early to the controller so that requests
        can be routed to this replica before an in-flight load completes.
        """
        ids = set(self.entries.keys())
        ids.update(self._load_tasks)
        return list(ids)

    def _push_ids_info(self):
        """Push the multiplexed replica info to the controller."""
        try:
            self.num_entries_gauge.set(
                len(self.entries), tags=self._metric_dimension_tag or None
            )

            for entry_id in self.entries:
                # Legacy model multiplexing tag key is "model_id"; new tag key is "id".
                tags = (
                    {"model_id": entry_id}
                    if self._multiplex_model_legacy_metrics
                    else {**self._metric_dimension_tag, "id": entry_id}
                )
                self.registered_entry_gauge.set(1, tags=tags)

            if self._push_replica_info:
                ids_list = self._get_loading_and_loaded_ids()
                info = RequestRoutingInfo(
                    replica_id=self._replica_id,
                    multiplex_dim_to_ids={self._name: ids_list},
                )
                _get_global_client().record_request_routing_info(info)
                self._push_replica_info = False
        except Exception as e:
            logger.warning(
                "Failed to push the multiplexed replica info "
                f"to the controller. Error: {e}"
            )

    async def shutdown(self):
        """Unload all entries when the wrapper is deleted."""
        while len(self.entries) > 0:
            try:
                await self._unload_lru()
            except Exception as e:
                logger.exception(
                    f"Failed to unload entry. Error: {e}",
                )

    async def load(self, multiplex_id: str) -> Any:
        """Load the entry if it's not loaded yet, and return the user object.

        Args:
            multiplex_id: the ID of the entry to load.

        Returns:
            The user-constructed object for this ID.
        """

        if not isinstance(multiplex_id, str):
            raise TypeError("The multiplex ID must be a string.")

        if not multiplex_id:
            raise ValueError("The multiplex ID cannot be empty.")

        self.get_entry_requests_counter.inc(tags=self._metric_dimension_tag or None)

        if multiplex_id in self.entries:
            # Move the entry to the end of the OrderedDict to mark it as
            # most-recently-used. Using move_to_end() instead of pop()+reinsert
            # avoids a race condition where concurrent coroutines could see the
            # key as missing during the brief window between pop and reinsert.
            self.entries.move_to_end(multiplex_id)
            return self.entries[multiplex_id]
        else:
            # Set the flag to push the multiplexed replica info to the controller
            # before loading. This is to make sure we can push the ID info to the
            # controller/router early, so that requests can be routed here.
            self._push_replica_info = True
            self._load_tasks.add(multiplex_id)
            async with self._cache_lock:
                # Check if the entry has been loaded by another request.
                if multiplex_id in self.entries:
                    return self.entries[multiplex_id]
                try:
                    # If the cache has reached its size limit, evict the LRU.
                    if (
                        self.size_per_replica > 0
                        and len(self.entries) >= self.size_per_replica
                    ):
                        await self._unload_lru()
                        self._push_replica_info = True

                    logger.debug(f"Loading entry '{multiplex_id}'.")
                    self.entries_load_counter.inc(
                        tags=self._metric_dimension_tag or None
                    )
                    load_start_time = time.time()
                    if self.self_arg is None:
                        self.entries[multiplex_id] = await self._func(multiplex_id)
                    else:
                        self.entries[multiplex_id] = await self._func(
                            self.self_arg, multiplex_id
                        )
                    load_latency_ms = (time.time() - load_start_time) * 1000.0
                    logger.debug(
                        f"Successfully loaded entry '{multiplex_id}' in "
                        f"{load_latency_ms:.1f}ms."
                    )
                    self._load_tasks.discard(multiplex_id)
                    self.load_latency_ms.observe(
                        load_latency_ms, tags=self._metric_dimension_tag or None
                    )
                    return self.entries[multiplex_id]
                except Exception as e:
                    logger.error(
                        f"Failed to load entry '{multiplex_id}'. Error: {e}",
                    )
                    self._load_tasks.discard(multiplex_id)
                    raise e

    async def _unload_lru(self) -> None:
        """Unload the least recently used entry."""

        self.entries_unload_counter.inc(tags=self._metric_dimension_tag or None)
        unload_start_time = time.time()
        multiplex_id, entry = self.entries.popitem(last=False)
        logger.debug(f"Unloading entry '{multiplex_id}'.")

        # If the entry object defines __del__, call it so the user can free
        # resources eagerly.
        if hasattr(entry, "__del__"):
            if not inspect.iscoroutinefunction(entry.__del__):
                await asyncio.get_running_loop().run_in_executor(None, entry.__del__)
            else:
                await entry.__del__()
            entry.__del__ = lambda _: None
        unload_latency_ms = (time.time() - unload_start_time) * 1000.0
        self.unload_latency_ms.observe(
            unload_latency_ms, tags=self._metric_dimension_tag or None
        )
        logger.debug(
            f"Successfully unloaded entry '{multiplex_id}' in "
            f"{unload_latency_ms:.1f}ms."
        )
        tags = (
            {"model_id": multiplex_id}
            if self._multiplex_model_legacy_metrics
            else {**self._metric_dimension_tag, "id": multiplex_id}
        )
        self.registered_entry_gauge.set(0, tags=tags)
