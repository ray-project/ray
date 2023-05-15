from ray._private.async_compat import sync_to_async
from collections import OrderedDict
from typing import Any, Callable
import logging
from ray.serve._private.constants import SERVE_LOGGER_NAME
import inspect
import asyncio


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
        self.models = OrderedDict()
        self._func = model_load_func
        self.self_arg = self_arg
        self.max_num_models_per_replica = max_num_models_per_replica

    async def load_model(self, model_id: str) -> Any:
        """Load the model if it is not loaded yet, and return the user-constructed model object.

        Args:
            model_id: the model ID.

        Returns:
            The user-constructed model object.
        """

        if type(model_id) != str:
            raise TypeError("The model ID must be a string.")

        if not model_id:
            raise ValueError("The model ID cannot be empty.")

        if model_id in self.models:
            # Move the model to the end of the OrderedDict to ensure LRU caching.
            model = self.models.pop(model_id)
            self.models[model_id] = model
        else:
            # If the number of models per replica is specified, check if the number of
            # models on the current replica has reached the limit.
            if (
                self.max_num_models_per_replica > 0
                and len(self.models) >= self.max_num_models_per_replica
            ):
                # Unload the least recently used model.
                await self.unload_model()
            # Load the model.
            logger.info(f"Loading model '{model_id}'.")
            if self.self_arg is None:
                self.models[model_id] = await self._func(model_id)
            else:
                self.models[model_id] = await self._func(self.self_arg, model_id)
        return self.models[model_id]

    async def unload_model(self) -> None:
        """Unload the least recently used model."""
        model_id, model = self.models.popitem(last=False)
        logger.info(f"Unloading model '{model_id}'.")

        # If the model has __del__ attribute, call it.
        # This is to clean up the model resources eagerly.
        if hasattr(model, "__del__"):
            if not inspect.iscoroutinefunction(model.__del__):
                await asyncio.get_running_loop().run_in_executor(None, model.__del__)
            else:
                await sync_to_async(model.__del__)()
            setattr(model, "__del__", lambda _: None)
