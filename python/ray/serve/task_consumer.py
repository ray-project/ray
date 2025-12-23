import inspect
import logging
from functools import wraps
from typing import Callable, Optional

from ray._common.utils import import_attr
from ray.serve._private.constants import (
    DEFAULT_CONSUMER_CONCURRENCY,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.task_consumer import TaskConsumerWrapper
from ray.serve._private.utils import copy_class_metadata
from ray.serve.schema import (
    TaskProcessorAdapter,
    TaskProcessorConfig,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _instantiate_adapter(
    task_processor_config: TaskProcessorConfig,
    consumer_concurrency: int = DEFAULT_CONSUMER_CONCURRENCY,
) -> TaskProcessorAdapter:
    adapter = task_processor_config.adapter

    # Handle string-based adapter specification (module path)
    if isinstance(adapter, str):
        adapter_class = import_attr(adapter)

    elif callable(adapter):
        adapter_class = adapter

    else:
        raise TypeError(
            f"Adapter must be either a string path or a callable class, got {type(adapter).__name__}: {adapter}"
        )

    try:
        adapter_instance = adapter_class(task_processor_config)
    except Exception as e:
        raise RuntimeError(f"Failed to instantiate {adapter_class.__name__}: {e}")

    if not isinstance(adapter_instance, TaskProcessorAdapter):
        raise TypeError(
            f"{adapter_class.__name__} must inherit from TaskProcessorAdapter, got {type(adapter_instance).__name__}"
        )

    try:
        adapter_instance.initialize(consumer_concurrency)
    except Exception as e:
        raise RuntimeError(f"Failed to initialize {adapter_class.__name__}: {e}")

    return adapter_instance


@PublicAPI(stability="alpha")
def instantiate_adapter_from_config(
    task_processor_config: TaskProcessorConfig,
) -> TaskProcessorAdapter:
    """
    Create a TaskProcessorAdapter instance from the provided configuration and call .initialize(). This function supports two ways to specify an adapter:

    1. String path: A fully qualified module path to an adapter class
       Example: "ray.serve.task_processor.CeleryTaskProcessorAdapter"

    2. Class reference: A direct reference to an adapter class
       Example: CeleryTaskProcessorAdapter

    Args:
        task_processor_config: Configuration object containing adapter specification.
    Returns:
        An initialized TaskProcessorAdapter instance ready for use.

    Raises:
        ValueError: If the adapter string path is malformed or cannot be imported.
        TypeError: If the adapter is not a string or callable class.

    Example:
        .. code-block:: python

            config = TaskProcessorConfig(
                adapter="my.module.CustomAdapter",
                adapter_config={"param": "value"},
                queue_name="my_queue"
            )
            adapter = instantiate_adapter_from_config(config)
    """

    return _instantiate_adapter(task_processor_config)


@PublicAPI(stability="alpha")
def task_consumer(*, task_processor_config: TaskProcessorConfig):
    """
    Decorator to mark a class as a TaskConsumer.

    Args:
        task_processor_config: Configuration for the task processor (required)

    Note:
        This decorator must be used with parentheses:
        @task_consumer(task_processor_config=config)

    Returns:
        A wrapper class that inherits from the target class and implements the task consumer functionality.

    Example:
        .. code-block:: python

            from ray import serve
            from ray.serve.task_consumer import task_consumer, task_handler

            @serve.deployment
            @task_consumer(task_processor_config=config)
            class MyTaskConsumer:

                @task_handler(name="my_task")
                def my_task(self, *args, **kwargs):
                    pass

    """

    def decorator(target_cls):
        class _TaskConsumerWrapper(target_cls, TaskConsumerWrapper):
            _adapter: TaskProcessorAdapter

            def __init__(self, *args, **kwargs):
                target_cls.__init__(self, *args, **kwargs)

            def initialize_callable(self, consumer_concurrency: int):
                self._adapter = _instantiate_adapter(
                    task_processor_config, consumer_concurrency
                )

                for name, method in inspect.getmembers(
                    target_cls, predicate=inspect.isfunction
                ):
                    if getattr(method, "_is_task_handler", False):
                        task_name = getattr(method, "_task_name", name)

                        # Create a callable that properly binds the method to this instance
                        bound_method = getattr(self, name)

                        self._adapter.register_task_handle(bound_method, task_name)

                try:
                    self._adapter.start_consumer()
                    logger.info("task consumer started successfully")
                except Exception as e:
                    logger.error(f"Failed to start task consumer: {e}")
                    raise

            def __del__(self):
                self._adapter.stop_consumer()
                self._adapter.shutdown()

                if hasattr(target_cls, "__del__"):
                    target_cls.__del__(self)

        copy_class_metadata(_TaskConsumerWrapper, target_cls)

        return _TaskConsumerWrapper

    return decorator


@PublicAPI(stability="alpha")
def task_handler(
    _func: Optional[Callable] = None, *, name: Optional[str] = None
) -> Callable:
    """
    Decorator to mark a method as a task handler.
    Optionally specify a task name. Default is the method name.

    Arguments:
        _func: The function to decorate.
        name: The name of the task. Default is the method name.

    Returns:
        A wrapper function that is marked as a task handler.

    Example:
        .. code-block:: python

            from ray import serve
            from ray.serve.task_consumer import task_consumer, task_handler

            @serve.deployment
            @task_consumer(task_processor_config=config)
            class MyTaskConsumer:

                @task_handler(name="my_task")
                def my_task(self, *args, **kwargs):
                    pass

    """

    # Validate name parameter if provided
    if name is not None and (not isinstance(name, str) or not name.strip()):
        raise ValueError(f"Task name must be a non-empty string, got {name}")

    def decorator(f):
        # async functions are not supported yet in celery `threads` worker pool
        if not inspect.iscoroutinefunction(f):

            @wraps(f)
            def wrapper(*args, **kwargs):
                return f(*args, **kwargs)

            wrapper._is_task_handler = True  # type: ignore
            wrapper._task_name = name or f.__name__  # type: ignore
            return wrapper

        else:
            raise NotImplementedError("Async task handlers are not supported yet")

    if _func is not None:
        # Used without arguments: @task_handler
        return decorator(_func)
    else:
        # Used with arguments: @task_handler(name="...")
        return decorator
