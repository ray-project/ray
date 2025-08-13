import logging
import threading
import time
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

from celery import Celery

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.schema import (
    CeleryAdapterConfig,
    TaskProcessorConfig,
    TaskResult,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI(stability="alpha")
class AsyncCapability(Enum):
    """
    Enum defining different async capabilities a TaskProcessor can support.

    Each capability represents an async operation that an adapter may or may not
    support. Use TaskProcessorAdapter.supports_async_capability() to check if
    a specific capability is available before using the corresponding async method.
    """

    ENQUEUE_TASK = auto()  # Ability to enqueue tasks asynchronously
    GET_TASK_STATUS = auto()  # Ability to retrieve task status asynchronously
    CANCEL_TASK = auto()  # Ability to cancel tasks asynchronously
    GET_METRICS = auto()  # Ability to retrieve metrics asynchronously
    HEALTH_CHECK = auto()  # Ability to perform health checks asynchronously


@PublicAPI(stability="alpha")
class TaskProcessorAdapter(ABC):
    """
    Abstract base class for task processing adapters.

    Subclasses can support different combinations of sync and async operations.
    Use supports_async_capability() to check if a specific async operation is supported.
    """

    def __init__(self):
        """
        Initialize the TaskProcessorAdapter.

        Sets up an empty set of async capabilities. Subclasses should add their
        supported async capabilities to self._async_capabilities in their __init__
        method.
        """
        self._async_capabilities: Set[AsyncCapability] = set()

    @property
    def async_capabilities(self) -> Set[AsyncCapability]:
        """
        Get the set of async capabilities supported by this adapter.

        Returns:
            Set[AsyncCapability]: A copy of the set containing all async capabilities
            supported by this adapter. Modifying the returned set will not affect
            the adapter's capabilities.
        """
        return self._async_capabilities.copy()

    def supports_async_capability(self, capability: AsyncCapability) -> bool:
        """
        Check if this adapter supports a specific async capability.

        Args:
            capability: The AsyncCapability enum value to check for.

        Returns:
            bool: True if the capability is supported, False otherwise.
        """
        return capability in self._async_capabilities

    def supports_any_async(self) -> bool:
        """
        Check if this adapter supports any async operations.

        Returns:
            bool: True if at least one async capability is supported, False if this is a sync-only adapter.
        """
        return len(self._async_capabilities) > 0

    @abstractmethod
    def initialize(self, config: TaskProcessorConfig):
        """
        Initialize the task processor with the given configuration.

        Args:
            config: TaskProcessorConfig containing adapter-specific configuration, queue names, retry settings, and other options.
        """
        pass

    @abstractmethod
    def register_task_handle(self, func: Callable, name: Optional[str] = None):
        """
        Register a function as a task handler.

        Args:
            func: The function to register as a task handler.
            name: Custom name for the task.
        """
        pass

    @abstractmethod
    def enqueue_task_sync(
        self,
        task_name: str,
        args: Optional[Any] = None,
        kwargs: Optional[Any] = None,
        **options,
    ) -> TaskResult:
        """
        Enqueue a task for execution synchronously.

        Args:
            task_name: Name of the registered task to execute.
            args: Positional arguments to pass to the task function.
            kwargs: Keyword arguments to pass to the task function.
            **options: Additional adapter-specific options for task execution.

        Returns:
            TaskResult: Object containing task ID, status, and other metadata.
        """
        pass

    @abstractmethod
    def get_task_status_sync(self, task_id: str) -> TaskResult:
        """
        Retrieve the current status of a task synchronously.

        Args:
            task_id: Unique identifier of the task to query.

        Returns:
            TaskResult: Object containing current task status, result, and other metadata.
        """
        pass

    @abstractmethod
    def start_consumer(self, **kwargs):
        """
        Start the task consumer/worker process.
        """
        pass

    @abstractmethod
    def stop_consumer(self, timeout: float = 10.0):
        """
        Stop the task consumer gracefully.

        Args:
            timeout: Maximum time in seconds to wait for the consumer to stop.
        """
        pass

    @abstractmethod
    def shutdown(self):
        """
        Shutdown the task processor and clean up resources.
        """
        pass

    @abstractmethod
    def cancel_task_sync(self, task_id: str) -> bool:
        """
        Cancel a task synchronously.

        Args:
            task_id: Unique identifier of the task to cancel.

        Returns:
            bool: True if cancellation was requested successfully.
        """
        pass

    @abstractmethod
    def get_metrics_sync(self) -> Dict[str, Any]:
        """
        Get metrics synchronously.

        Returns:
            Dict[str, Any]: Adapter-specific metrics data.
        """
        pass

    @abstractmethod
    def health_check_sync(self) -> List[Dict]:
        """
        Perform health check synchronously.

        Returns:
            List[Dict]: Health status information for workers/components.
        """
        pass

    async def enqueue_task_async(
        self,
        task_name: str,
        args: Optional[Any] = None,
        kwargs: Optional[Any] = None,
        **options,
    ) -> TaskResult:
        """
        Enqueue a task asynchronously.

        Args:
            task_name: Name of the registered task to execute.
            args: Positional arguments to pass to the task function.
            kwargs: Keyword arguments to pass to the task function.
            **options: Additional adapter-specific options for task execution.

        Returns:
            TaskResult: Object containing task ID, status, and other metadata.

        Raises:
            NotImplementedError: If async enqueue is not supported by this adapter.
        """
        if not self.supports_async_capability(AsyncCapability.ENQUEUE_TASK):
            raise NotImplementedError(
                f"{self.__class__.__name__} does not support async task enqueueing. "
                f"Use enqueue_task_sync() instead or check supports_async_capability() first."
            )

        raise NotImplementedError("Subclass must implement enqueue_task_async function")

    async def get_task_status_async(self, task_id: str) -> TaskResult:
        """
        Get task status asynchronously.

        Args:
            task_id: Unique identifier of the task to query.

        Returns:
            TaskResult: Object containing current task status, result, and other metadata.

        Raises:
            NotImplementedError: If async status retrieval is not supported by this adapter.
        """
        if not self.supports_async_capability(AsyncCapability.GET_TASK_STATUS):
            raise NotImplementedError(
                f"{self.__class__.__name__} does not support async task status retrieval. "
                f"Use get_task_status_sync() instead or check supports_async_capability() first."
            )

        raise NotImplementedError(
            "Subclass must implement get_task_status_async function"
        )

    async def cancel_task_async(self, task_id: str) -> bool:
        """
        Cancel a task.

        Args:
            task_id: Unique identifier of the task to cancel.

        Returns:
            bool: True if cancellation was requested successfully.

        Raises:
            NotImplementedError: If async task cancellation is not supported by this adapter.
        """
        if not self.supports_async_capability(AsyncCapability.CANCEL_TASK):
            raise NotImplementedError(
                f"{self.__class__.__name__} does not support async task cancellation. "
                f"Check supports_async_capability() first."
            )

        raise NotImplementedError("Subclass must implement cancel_task_async function")

    async def get_metrics_async(self) -> Dict[str, Any]:
        """
        Get metrics asynchronously.

        Returns:
            Dict[str, Any]: Adapter-specific metrics data.

        Raises:
            NotImplementedError: If async metrics retrieval is not supported by this adapter.
        """
        if not self.supports_async_capability(AsyncCapability.GET_METRICS):
            raise NotImplementedError(
                f"{self.__class__.__name__} does not support async metrics retrieval. "
                f"Check supports_async_capability() first."
            )

        raise NotImplementedError("Subclass must implement get_metrics_async function")

    async def health_check_async(self) -> List[Dict]:
        """
        Perform health check asynchronously.

        Returns:
            List[Dict]: Health status information for workers/components.

        Raises:
            NotImplementedError: If async health check is not supported by this adapter.
        """
        if not self.supports_async_capability(AsyncCapability.HEALTH_CHECK):
            raise NotImplementedError(
                f"{self.__class__.__name__} does not support async health check. "
                f"Check supports_async_capability() first."
            )

        raise NotImplementedError("Subclass must implement health_check_async function")


@PublicAPI(stability="alpha")
class CeleryTaskProcessorAdapter(TaskProcessorAdapter):
    """
    Celery-based task processor adapter.
    This adapter does NOT support any async operations.
    All operations must be performed synchronously.
    """

    _app: Celery
    _config: TaskProcessorConfig
    _worker_thread: Optional[threading.Thread] = None

    def __init__(self, config: TaskProcessorConfig):
        super().__init__()

        if not isinstance(config.adapter_config, CeleryAdapterConfig):
            raise TypeError(
                "TaskProcessorConfig.adapter_config must be an instance of CeleryAdapterConfig"
            )

        self._config = config

        # Celery adapter does not support any async capabilities
        # self._async_capabilities is already an empty set from parent class

    def initialize(self, config: TaskProcessorConfig):
        self._app = Celery(
            config.queue_name,
            backend=config.adapter_config.backend_url,
            broker=config.adapter_config.broker_url,
        )

        self._app.conf.update(
            loglevel="info",
            worker_pool="threads",
            worker_concurrency=config.adapter_config.worker_concurrency,
            max_retries=config.max_retries,
            task_default_queue=config.queue_name,
            # Store task results so they can be retrieved after completion
            task_ignore_result=False,
            # Acknowledge tasks only after completion (not when received) for better reliability
            task_acks_late=True,
            # Reject and requeue tasks when worker is lost to prevent data loss
            reject_on_worker_lost=True,
        )

        if config.adapter_config.broker_transport_options is not None:
            self._app.conf.update(
                broker_transport_options=config.adapter_config.broker_transport_options,
            )

        ### TODO(harshit|SERVE-987): add the failed_task_queue_name and unprocessable_task_queue_name business logic here

    def register_task_handle(self, func, name=None):
        task_options = {
            "autoretry_for": (Exception,),
            "retry_kwargs": {"max_retries": self._config.max_retries},
            "retry_backoff": True,
            "retry_backoff_max": 60,  # Max backoff of 60 seconds
            "retry_jitter": False,  # Disable jitter for predictable testing
        }

        if name:
            self._app.task(name=name, **task_options)(func)
        else:
            self._app.task(**task_options)(func)

    def enqueue_task_sync(
        self, task_name, args=None, kwargs=None, **options
    ) -> TaskResult:
        task_response = self._app.send_task(
            task_name,
            args=args,
            kwargs=kwargs,
            queue=self._config.queue_name,
            **options,
        )

        return TaskResult(
            id=task_response.id,
            status=task_response.status,
            created_at=time.time(),
            result=task_response.result,
        )

    def get_task_status_sync(self, task_id) -> TaskResult:
        task_details = self._app.AsyncResult(task_id)
        return TaskResult(
            id=task_details.id,
            result=task_details.result,
            status=task_details.status,
        )

    def start_consumer(self, **kwargs):
        """Starts the Celery worker thread."""
        if self._worker_thread is not None and self._worker_thread.is_alive():
            logger.info("Celery worker thread is already running.")
            return

        self._worker_thread = threading.Thread(
            target=self._app.worker_main,
            args=(("worker", f"--hostname={self._app.main}"),),
        )
        self._worker_thread.start()

        logger.info(f"Celery worker thread started with hostname: {self._app.main}")

    def stop_consumer(self, timeout: float = 10.0):
        """Signals the Celery worker to shut down and waits for it to terminate."""
        if self._worker_thread is None or not self._worker_thread.is_alive():
            logger.info("Celery worker thread is not running.")
            return

        logger.info("Sending shutdown signal to Celery worker...")

        # Use the worker's hostname for targeted shutdown
        self._app.control.broadcast(
            "shutdown", destination=[f"celery@{self._app.main}"]
        )
        self._worker_thread.join(timeout=timeout)

        if self._worker_thread.is_alive():
            logger.warning(f"Worker thread did not terminate after {timeout} seconds.")
        else:
            logger.info("Celery worker thread has stopped.")

        self._worker_thread = None

    def shutdown(self):
        self._app.control.shutdown()

    def cancel_task_sync(self, task_id) -> bool:
        return self._app.AsyncResult(task_id).cancel()

    def get_metrics_sync(self) -> Dict[str, Any]:
        """
        Returns the metrics of the Celery worker synchronously.
        More details can be found here: https://docs.celeryq.dev/en/stable/reference/celery.app.control.html#celery.app.control.Inspect.stats
        """
        return self._app.control.inspect().stats()

    def health_check_sync(self) -> List[Dict]:
        """
        Checks the health of the Celery worker synchronously.
        Returns a list of dictionaries, each containing the worker name and a dictionary with the health status.
        Example: [{'celery@192.168.1.100': {'ok': 'pong'}}]
        More details can be found here: https://docs.celeryq.dev/en/stable/reference/celery.app.control.html#celery.app.control.Control.ping
        """
        return self._app.control.ping()
