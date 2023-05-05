from ray._private.async_compat import sync_to_async
from collections import OrderedDict
from typing import Any, List
from ray.serve import metrics
import time
import logging
from ray.serve._private.constants import (
    DEFAULT_LATENCY_BUCKET_MS,
    SERVE_LOGGER_NAME,
    PUSH_MODEL_IDS_INTERVAL_S,
)
from ray.serve.context import (
    get_global_client,
    get_internal_replica_context,
)
import asyncio


logger = logging.getLogger(SERVE_LOGGER_NAME)


class _ModelMultiplexWrapper:
    def __init__(self, model_load_func, self_args, num_models_per_replica=0):
        # The models are stored in an OrderedDict to ensure LRU caching.
        self.models = OrderedDict()
        self._func = sync_to_async(model_load_func)
        self.self_args = self_args
        self.num_models_per_replica = num_models_per_replica
        self.model_load_latency_s = metrics.Histogram(
            "serve_model_load_latency_s",
            description="The time it takes to load a model.",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("model_id",),
        )
        self.model_unload_latency_s = metrics.Histogram(
            "serve_model_unload_latency_s",
            description="The time it takes to unload a model.",
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=("model_id",),
        )
        self.num_models = metrics.Gauge(
            "serve_num_models",
            description="The number of models loaded on the current replica.",
        )

        context = get_internal_replica_context()
        self._deployment_name = context.deployment
        self._replica_tag = context.replica_tag
        self._should_push_model_ids = False

        # Push the model IDs to the controller periodically.
        asyncio.get_event_loop().create_task(self._push_model_ids())

    async def load_model(self, model_id: str, *args, **kwargs) -> Any:
        """Load the model if it is not loaded yet, and return the model handle.

        Args:
            model_id: the model ID.
            *args: the arguments to be passed to the model load function.
            **kwargs: the keyword arguments to be passed to the model load function.

        Returns:
            The model handle.
        """
        if type(model_id) != str:
            raise TypeError(
                "The first argument of the multiplexed decorated function must be a "
                "string representing the model ID. Got type '{}' instead.".format(
                    type(model_id)
                )
            )

        self.num_models.set(len(self.models))

        if model_id in self.models:
            # Move the model to the end of the OrderedDict to ensure LRU caching.
            model = self.models.pop(model_id)
            self.models[model_id] = model
        else:
            # If the number of models per replica is specified, check if the number of
            # models on the current replica has reached the limit.
            if self.num_models_per_replica > 0:
                if len(self.models) >= self.num_models_per_replica:
                    # Unload the least recently used model.
                    unload_start_time = time.time()
                    await self.unload_model()
                    self.model_unload_latency_s.observe(
                        time.time() - unload_start_time, tags={"model_id": model_id}
                    )
            # Load the model.
            load_start_time = time.time()
            logger.info("Loading model '{}'.".format(model_id))
            if self.self_args is None:
                self.models[model_id] = await self._func(model_id, *args, **kwargs)
            else:
                self.models[model_id] = await self._func(
                    self.self_args, model_id, *args, **kwargs
                )
            self.model_load_latency_s.observe(
                time.time() - load_start_time, tags={"model_id": model_id}
            )
            self._should_push_model_ids = True
        return self.models[model_id]

    async def unload_model(self):
        """Unload the least recently used model."""
        tag, model = self.models.popitem(last=False)
        logger.info("Unloading model '{}'.".format(tag))

        # If the model has __del__ attribute (e.g., PyTorch models), call it.
        # This is to clean up the model resources eargerly.
        if hasattr(model, "__del__"):
            await sync_to_async(self.callable.__del__)()
            setattr(model, "__del__", lambda _: None)
        del self.models[tag]

    def get_model_ids(self) -> List[str]:
        """Get the list of model IDs."""
        return self.models.keys()

    async def _push_model_ids(self):
        """Push the model IDs to the global client."""
        while True:
            try:
                if self._should_push_model_ids:
                    controller = get_global_client()._controller
                    controller.record_model_ids.remote(
                        (self._deployment_name, self._replica_tag, self.get_model_ids())
                    )
                    self._should_push_model_ids = False
            except Exception as e:
                logger.warning(
                    "Failed to push the model IDs to the controller. Error: {}".format(
                        e
                    )
                )
            await asyncio.sleep(PUSH_MODEL_IDS_INTERVAL_S)
