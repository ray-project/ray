import inspect
import logging
from functools import wraps
from typing import Callable, Optional

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.schema import TaskProcessorConfig
from ray.serve.task_processor import TaskProcessorAdapter, get_task_adapter
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


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
        class TaskConsumerWrapper(target_cls):
            _adapter: TaskProcessorAdapter

            def __init__(self, *args, **kwargs):
                target_cls.__init__(self, *args, **kwargs)

                self._adapter = get_task_adapter(task_processor_config)

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
                    logger.info("Celery consumer started successfully")
                except Exception as e:
                    logger.error(f"Failed to start Celery consumer: {e}")
                    raise

            def __del__(self):
                self._adapter.stop_consumer()
                self._adapter.shutdown()

                if hasattr(target_cls, "__del__"):
                    target_cls.__del__(self)

        return TaskConsumerWrapper

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
        name: The name of the task.

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
        raise ValueError("Task name must be a non-empty string when provided")

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
            raise NotImplementedError(
                "Async task handlers are not supported yet in celery `threads` worker pool"
            )

    if _func is not None:
        # Used without arguments: @task_handler
        return decorator(_func)
    else:
        # Used with arguments: @task_handler(name="...")
        return decorator
