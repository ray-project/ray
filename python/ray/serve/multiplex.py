import asyncio
import inspect
import logging
import time
from collections import OrderedDict
from typing import Any, Callable, List, Set

from ray.serve import metrics
from ray.serve._private.common import ReplicaID, RequestRoutingInfo
from ray.serve._private.constants import (
    MODEL_LOAD_LATENCY_BUCKETS_MS,
    PUSH_MULTIPLEXED_MODEL_IDS_INTERVAL_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.metrics_utils import MetricsPusher
from ray.serve._private.usage import ServeUsageTag
from ray.serve.context import _get_global_client, _get_internal_replica_context

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

        Args:
            model_id: the model ID.

        Returns:
            The user-constructed model object.
        """

        if type(model_id) is not str:
            raise TypeError("The model ID must be a string.")

        if not model_id:
            raise ValueError("The model ID cannot be empty.")

        self.get_model_requests_counter.inc()

        if model_id in self.models:
            # Move the model to the end of the OrderedDict to ensure LRU caching.
            model = self.models.pop(model_id)
            self.models[model_id] = model
            return self.models[model_id]
        else:
            # Set the flag to push the multiplexed replica info to the controller
            # before loading the model. This is to make sure we can push the model
            # id info to the controller/router early, so that requests can be routed to
            # the replica.
            self._push_multiplexed_replica_info = True
            self._model_load_tasks.add(model_id)
            async with self._model_cache_lock:
                # Check if the model has been loaded by another request.
                if model_id in self.models:
                    return self.models[model_id]
                try:
                    # If the number of models per replica is specified, check
                    # if the number of models on the current replica has
                    # reached the limit.
                    if (
                        self.max_num_models_per_replica > 0
                        and len(self.models) >= self.max_num_models_per_replica
                    ):
                        # Unload the least recently used model.
                        await self.unload_model_lru()
                        self._push_multiplexed_replica_info = True

                    # Load the model.
                    logger.info(f"Loading model '{model_id}'.")
                    self.models_load_counter.inc()
                    load_start_time = time.time()
                    if self.self_arg is None:
                        self.models[model_id] = await self._func(model_id)
                    else:
                        self.models[model_id] = await self._func(
                            self.self_arg, model_id
                        )
                    load_latency_ms = (time.time() - load_start_time) * 1000.0
                    logger.info(
                        f"Successfully loaded model '{model_id}' in "
                        f"{load_latency_ms:.1f}ms."
                    )
                    self._model_load_tasks.discard(model_id)
                    self.model_load_latency_ms.observe(load_latency_ms)
                    return self.models[model_id]
                except Exception as e:
                    logger.error(
                        f"Failed to load model '{model_id}'. Error: {e}",
                    )
                    self._model_load_tasks.discard(model_id)
                    raise e

    async def unload_model_lru(self) -> None:
        """Unload the least recently used model."""

        self.models_unload_counter.inc()
        unload_start_time = time.time()
        model_id, model = self.models.popitem(last=False)
        logger.info(f"Unloading model '{model_id}'.")

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
        logger.info(
            f"Successfully unloaded model '{model_id}' in {unload_latency_ms:.1f}ms."
        )
        self.registered_model_gauge.set(0, tags={"model_id": model_id})
