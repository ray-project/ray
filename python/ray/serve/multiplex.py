from ray._private.async_compat import sync_to_async
from collections import OrderedDict
from typing import Any, List, Callable
from ray.serve import metrics
import time
import logging
from ray.serve._private.constants import SERVE_LOGGER_NAME
import inspect
import asyncio


logger = logging.getLogger(SERVE_LOGGER_NAME)


class _ModelMultiplexWrapper:
    def __init__(
        self,
        model_load_func: Callable,
        self_args: List[Any],
        max_num_models_per_replica: int = -1,
    ):
        """Initialize the model multiplexer.

        The model multiplexer is a wrapper class that wraps the model load function
        and provides the LRU caching functionality, and the model load function should
        be a coroutine function that takes the model ID as the first argument and
        returns the model handle.
        The model multiplexer will also ensure that the number of models on the current
        replica does not exceed the specified limit.
        The model will be unloaded in the LRU order, the model multiplexer will call the
        model's __del__ attribute if it exists (e.g., PyTorch models) to clean up the
        model resources eargerly.

        Args:
            model_load_func: the model load async function.
            self_args: the arguments to be passed to the model load function.
            max_num_models_per_replica: the maximum number of models to be loaded on the
                current replica. If it is -1, there is no limit for the number of models
                per replica.
        """

        self.models = OrderedDict()
        self._func = model_load_func
        self.self_args = self_args
        self.max_num_models_per_replica = max_num_models_per_replica
        self.model_load_latency_s = metrics.Gauge(
            "serve_model_load_latency_s",
            description="The time it takes to load a model.",
            tag_keys=("model_id",),
        )
        self.model_unload_latency_s = metrics.Gauge(
            "serve_model_unload_latency_s",
            description="The time it takes to unload a model.",
            tag_keys=("model_id",),
        )
        self.num_models = metrics.Gauge(
            "serve_num_models",
            description="The number of models loaded on the current replica.",
        )

        self.models_load_counter = metrics.Counter(
            "serve_models_load_counter",
            description="The counter for models loaded on the current replica.",
        )

    async def load_model(self, model_id: str) -> Any:
        """Load the model if it is not loaded yet, and return the model handle.

        Args:
            model_id: the model ID.

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

        # Raise an error if the model_id is empty string or None.
        if not model_id:
            raise ValueError("The model ID cannot be empty.")

        self.num_models.set(len(self.models))

        if model_id in self.models:
            # Move the model to the end of the OrderedDict to ensure LRU caching.
            model = self.models.pop(model_id)
            self.models[model_id] = model
        else:
            self.models_load_counter.inc()
            # If the number of models per replica is specified, check if the number of
            # models on the current replica has reached the limit.
            if self.max_num_models_per_replica > 0:
                if len(self.models) >= self.max_num_models_per_replica:
                    # Unload the least recently used model.
                    unload_start_time = time.time()
                    await self.unload_model()
                    self.model_unload_latency_s.set(
                        time.time() - unload_start_time, tags={"model_id": model_id}
                    )
            # Load the model.
            load_start_time = time.time()
            logger.info("Loading model '{}'.".format(model_id))
            if self.self_args is None:
                self.models[model_id] = await self._func(model_id)
            else:
                self.models[model_id] = await self._func(self.self_args, model_id)
            self.model_load_latency_s.set(
                time.time() - load_start_time, tags={"model_id": model_id}
            )
            self._should_push_model_ids = True
        return self.models[model_id]

    async def unload_model(self) -> None:
        """Unload the least recently used model."""
        model_id, model = self.models.popitem(last=False)
        logger.info("Unloading model '{}'.".format(model_id))

        # If the model has __del__ attribute (e.g., PyTorch models), call it.
        # This is to clean up the model resources eargerly.
        if hasattr(model, "__del__"):
            if not inspect.iscoroutinefunction(model.__del__):
                await asyncio.get_running_loop().run_in_executor(None, model.__del__)
            else:
                await sync_to_async(model.__del__)()
            setattr(model, "__del__", lambda _: None)
