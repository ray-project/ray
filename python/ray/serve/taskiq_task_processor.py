import asyncio
import inspect
import logging
import threading
import time
import traceback
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, Field
from taskiq import AsyncBroker, AsyncResultBackend, TaskiqMessage, TaskiqMiddleware
from taskiq.acks import AcknowledgeType
from taskiq.middlewares import SmartRetryMiddleware
from taskiq.receiver import Receiver
from taskiq.result import TaskiqResult
from taskiq_redis import RedisAsyncResultBackend, RedisStreamBroker

from ray.serve._private.constants import (
    DEFAULT_CONSUMER_CONCURRENCY,
    SERVE_LOGGER_NAME,
)
from ray.serve.schema import (
    TaskProcessorAdapter,
    TaskProcessorConfig,
    TaskResult,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class TaskiqAdapterConfig(BaseModel):
    """
    Taskiq adapter config for async task processing in Ray Serve.

    This adapter uses RedisStreamBroker for at-least-once delivery guarantees.
    Tasks are acknowledged only after successful completion, ensuring tasks
    are not lost if a worker stops mid-processing.
    """

    broker_url: str = Field(
        ..., description="The URL of the broker (e.g., 'redis://localhost:6379')."
    )
    result_backend_url: Optional[str] = Field(
        default=None,
        description="The URL for the result backend.",
    )
    consumer_group_name: str = Field(
        default="taskiq",
        description="Name for the Redis consumer group. Workers in the same group share tasks.",
    )
    idle_timeout_ms: int = Field(
        default=600000,
        gt=0,
        description="Time in ms before unacked tasks from dead consumers are reclaimed (default: 10 min).",
    )


logger = logging.getLogger(SERVE_LOGGER_NAME)


def _create_stub_task(broker: AsyncBroker, task_name: str):
    """Create and register a stub task for routing purposes."""

    def stub(*a, **kw):
        pass

    stub.__name__ = task_name
    return broker.task(task_name=task_name)(stub)


class DLQMiddleware(TaskiqMiddleware):
    """Middleware to route failed tasks to a dead letter queue after max retries."""

    def __init__(
        self,
        broker: AsyncBroker,
        failed_task_queue_name: Optional[str] = None,
        max_retries: int = 3,
    ):
        super().__init__()
        self._broker = broker
        self._failed_task_queue_name = failed_task_queue_name
        self._max_retries = max_retries

    def _get_dlq_task(self):
        """Get or create DLQ task for routing failed messages."""
        dlq_task = self._broker.find_task(self._failed_task_queue_name)
        if dlq_task is not None:
            return dlq_task

        # Create stub task for DLQ routing. This stub only routes the message;
        # the actual DLQ handler must be registered separately if processing is needed.
        logger.warning(
            f"No handler registered for DLQ '{self._failed_task_queue_name}'. "
            "Creating stub task for routing. Register a handler to process DLQ messages."
        )
        return _create_stub_task(self._broker, self._failed_task_queue_name)

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult,
        exception: BaseException,
    ) -> None:
        """Send to DLQ only when max retries exceeded."""
        if not self._failed_task_queue_name:
            return

        # SmartRetryMiddleware computes: retries = label_value + 1
        # We must use the same calculation to determine when retries are exhausted
        retries = int(message.labels.get("_retries", 0)) + 1
        max_retries = int(message.labels.get("max_retries", self._max_retries))

        # Only send to DLQ when all retries exhausted
        # SmartRetryMiddleware retries when retries < max_retries,
        # so we send to DLQ when retries >= max_retries
        if retries >= max_retries:
            logger.error(
                f"Task {message.task_id} failed after {max_retries} retries. "
                f"Moving to DLQ: {self._failed_task_queue_name}"
            )

            try:
                dlq_task = self._get_dlq_task()
                tb_str = "".join(
                    traceback.format_exception(
                        type(exception), exception, exception.__traceback__
                    )
                )

                await dlq_task.kicker().with_labels(
                    queue_name=self._failed_task_queue_name
                ).kiq(
                    message.task_id,
                    message.task_name,
                    message.args,
                    message.kwargs,
                    str(exception),
                    tb_str,
                )
            except Exception as dlq_error:
                logger.error(
                    f"Failed to send task {message.task_id} to DLQ '{self._failed_task_queue_name}': {dlq_error}"
                )


@PublicAPI(stability="alpha")
class TaskiqTaskProcessorAdapter(TaskProcessorAdapter):
    """
    Taskiq-based task processor adapter with async support and at-least-once delivery.

    This adapter uses RedisStreamBroker with message acknowledgment to ensure tasks
    are not lost if a worker stops mid-processing. Tasks are acknowledged only after
    successful completion and result storage.

    Features:
    - Async task handlers with true async concurrency
    - At-least-once delivery guarantee via Redis Streams
    - Automatic task recovery from dead consumers (configurable idle_timeout_ms)
    - Built-in retry with exponential backoff and jitter

    Note: Task cancellation is not supported by taskiq.
    """

    def __init__(self, config: TaskProcessorConfig, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not isinstance(config.adapter_config, TaskiqAdapterConfig):
            raise TypeError(
                "TaskProcessorConfig.adapter_config must be an instance of TaskiqAdapterConfig"
            )

        self._config = config
        self._broker: Optional[AsyncBroker] = None
        self._result_backend: Optional[AsyncResultBackend] = None
        self._receiver: Optional[Receiver] = None
        self._worker_thread: Optional[threading.Thread] = None
        self._worker_loop: Optional[asyncio.AbstractEventLoop] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._registered_tasks: Dict[str, Callable] = {}
        self._tasks_lock = threading.Lock()
        self._consumer_concurrency: int = DEFAULT_CONSUMER_CONCURRENCY
        self._broker_started: bool = False

    def initialize(self, consumer_concurrency: int = DEFAULT_CONSUMER_CONCURRENCY):
        """Initialize the taskiq broker and result backend."""
        self._consumer_concurrency = consumer_concurrency
        adapter_config: TaskiqAdapterConfig = self._config.adapter_config

        # Create broker using RedisStreamBroker for at-least-once delivery
        # Tasks remain in "pending" state until explicitly acknowledged after completion
        self._broker = RedisStreamBroker(
            url=adapter_config.broker_url,
            queue_name=self._config.queue_name,
            consumer_group_name=adapter_config.consumer_group_name,
            idle_timeout=adapter_config.idle_timeout_ms,
        )

        # Create result backend only if explicitly configured
        if adapter_config.result_backend_url:
            self._result_backend = RedisAsyncResultBackend(
                redis_url=adapter_config.result_backend_url,
            )
            self._broker = self._broker.with_result_backend(self._result_backend)

        max_retries = self._config.max_retries or 3

        # Use built-in SmartRetryMiddleware for retries
        self._broker.add_middlewares(
            SmartRetryMiddleware(
                default_retry_count=max_retries + 1,
                default_retry_label=True,  # Enable retry by default
            )
        )

        # Add DLQ middleware for routing failed tasks after max retries
        if self._config.failed_task_queue_name:
            self._broker.add_middlewares(
                DLQMiddleware(
                    broker=self._broker,
                    failed_task_queue_name=self._config.failed_task_queue_name,
                    max_retries=max_retries + 1,  # total attempts = max_retries + 1
                )
            )

        logger.info(
            f"Taskiq adapter initialized with RedisStreamBroker: {adapter_config.broker_url}, "
            f"queue: {self._config.queue_name}, consumer_group: {adapter_config.consumer_group_name}"
        )

    def _create_wrapper(self, func: Callable, task_name: str) -> Callable:
        """Create a wrapper for callables with read-only __name__."""
        if asyncio.iscoroutinefunction(func):

            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)

        else:

            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

        wrapper.__name__ = task_name
        return wrapper

    def register_task_handle(self, func: Callable, name: Optional[str] = None):
        """Register a function as a task handler. Supports sync and async."""
        task_name = name or getattr(func, "__name__", "unknown_task")

        # Bound methods have read-only __name__, need wrapping
        # For other callables, try setting __name__ directly
        wrapper = func
        if inspect.ismethod(func):
            wrapper = self._create_wrapper(func, task_name)
        else:
            try:
                func.__name__ = task_name
            except AttributeError:
                wrapper = self._create_wrapper(func, task_name)

        decorated_task = self._broker.task(task_name=task_name)(wrapper)
        with self._tasks_lock:
            self._registered_tasks[task_name] = decorated_task

        logger.info(f"Registered task handler: {task_name}")

    def _run_async(self, coro):
        """Run an async coroutine from sync context."""
        if self._worker_loop and self._worker_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, self._worker_loop)
            return future.result(timeout=30)
        else:
            return asyncio.run(coro)

    def enqueue_task_sync(
        self, task_name: str, args=None, kwargs=None, **options
    ) -> TaskResult:
        """Enqueue a task synchronously."""
        return self._run_async(
            self.enqueue_task_async(task_name, args, kwargs, **options)
        )

    async def enqueue_task_async(
        self, task_name: str, args=None, kwargs=None, **options
    ) -> TaskResult:
        """Enqueue a task asynchronously."""
        # Lazy broker startup - ensures connection pools are initialized
        if not self._broker_started:
            await self._broker.startup()
            self._broker_started = True

        args = args or []
        kwargs = kwargs or {}

        # Thread-safe lookup of registered tasks
        with self._tasks_lock:
            task = self._registered_tasks.get(task_name)

        if not task:
            # Try to find task in broker
            task = self._broker.find_task(task_name)

        if not task:
            # Create a stub task for enqueueing purposes.
            # This allows sending tasks without pre-registration (like Celery's send_task).
            task = _create_stub_task(self._broker, task_name)

        # Kick the task
        task_handle = await task.kiq(*args, **kwargs)

        return TaskResult(
            id=task_handle.task_id,
            status="PENDING",
            created_at=time.time(),
            result=None,
        )

    def get_task_status_sync(self, task_id: str) -> TaskResult:
        """Get task status synchronously."""
        return self._run_async(self.get_task_status_async(task_id))

    async def get_task_status_async(self, task_id: str) -> TaskResult:
        """Get task status asynchronously."""
        # Taskiq result backend is required for getting task status.
        if not self._result_backend:
            raise RuntimeError(
                "Result backend not configured. Set 'result_backend_url' in TaskiqAdapterConfig."
            )
        result = await self._result_backend.get_result(task_id)
        if result is not None:
            if result.is_err:
                return TaskResult(
                    id=task_id,
                    status="FAILURE",
                    result=result.error,
                )
            else:
                return TaskResult(
                    id=task_id,
                    status="SUCCESS",
                    result=result.return_value,
                )
        else:
            # Taskiq result backend returns None if the task is not yet completed, or such task does not exist.
            return TaskResult(
                id=task_id,
                status="UNKNOWN",
                result=None,
            )

    def start_consumer(self, **kwargs):
        """Start the taskiq worker in a separate thread."""
        if self._worker_thread is not None and self._worker_thread.is_alive():
            logger.info("Taskiq worker thread is already running.")
            return

        self._worker_thread = threading.Thread(
            target=self._run_worker,
            daemon=True,
        )
        self._worker_thread.start()

        logger.info(
            f"Taskiq worker thread started for queue: {self._config.queue_name}"
        )

    def _run_worker(self):
        """Run the taskiq worker in its own event loop."""
        self._worker_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._worker_loop)

        self._shutdown_event = asyncio.Event()

        try:
            self._worker_loop.run_until_complete(self._worker_main())
        except Exception as e:
            logger.error(f"Taskiq worker error: {e}")
        finally:
            self._worker_loop.close()

    async def _worker_main(self):
        """Main worker coroutine using taskiq's built-in Receiver."""
        if not self._broker_started:
            await self._broker.startup()
            self._broker_started = True

        # Create the receiver which handles task parsing, execution, and result storage
        # ack_type=WHEN_SAVED ensures tasks are only acknowledged after result is saved,
        # providing at-least-once delivery guarantee
        self._receiver = Receiver(
            broker=self._broker,
            max_async_tasks=self._consumer_concurrency,
            ack_type=AcknowledgeType.WHEN_SAVED,
        )

        logger.info("Taskiq worker started, waiting for tasks...")

        try:
            # Use taskiq's built-in listen method which handles everything
            await self._receiver.listen(self._shutdown_event)
        except asyncio.CancelledError:
            logger.info("Worker cancelled")
        finally:
            # Clean up broker connection pools
            if self._broker_started:
                await self._broker.shutdown()
                self._broker_started = False

    def stop_consumer(self, timeout: float = 10.0):
        """Stop the taskiq worker gracefully."""
        if self._worker_thread is None or not self._worker_thread.is_alive():
            logger.info("Taskiq worker thread is not running.")
            return

        logger.info("Sending shutdown signal to Taskiq worker...")

        # Signal shutdown
        if self._shutdown_event and self._worker_loop:
            self._worker_loop.call_soon_threadsafe(self._shutdown_event.set)

        self._worker_thread.join(timeout=timeout)

        if self._worker_thread.is_alive():
            logger.warning(f"Worker thread did not terminate after {timeout} seconds.")
        else:
            logger.info("Taskiq worker thread has stopped.")

        self._worker_thread = None
        self._worker_loop = None
        self._shutdown_event = None

    def shutdown(self):
        """Shutdown broker connections. Call when done using the adapter."""
        if self._worker_thread and self._worker_thread.is_alive():
            # Consumer is running, use stop_consumer instead
            self.stop_consumer()
            return

        if self._broker_started:
            try:
                loop = asyncio.get_running_loop()
                # In async context, schedule shutdown
                loop.create_task(self._async_shutdown())
            except RuntimeError:
                # No running loop, safe to use asyncio.run()
                asyncio.run(self._broker.shutdown())
                self._broker_started = False
                logger.info("Taskiq broker shutdown complete.")

    async def _async_shutdown(self):
        """Async broker shutdown for use within running event loop."""
        await self._broker.shutdown()
        self._broker_started = False
        logger.info("Taskiq broker shutdown complete.")

    def cancel_task_sync(self, task_id: str):
        """
        Cancel a task synchronously.
        Note: Taskiq does not support task cancellation.
        """
        raise NotImplementedError(
            "Task cancellation is not supported by Taskiq. "
            "See: https://github.com/taskiq-python/taskiq/issues/305"
        )

    async def cancel_task_async(self, task_id: str):
        """
        Cancel a task asynchronously.
        Note: Taskiq does not support task cancellation.
        """
        raise NotImplementedError(
            "Task cancellation is not supported by Taskiq. "
            "See: https://github.com/taskiq-python/taskiq/issues/305"
        )

    def get_metrics_sync(self) -> Dict[str, Any]:
        """Get worker metrics synchronously."""
        raise NotImplementedError("Metrics collection not yet implemented for Taskiq")

    async def get_metrics_async(self) -> Dict[str, Any]:
        """Get worker metrics asynchronously."""
        raise NotImplementedError("Metrics collection not yet implemented for Taskiq")

    def health_check_sync(self) -> List[Dict]:
        """Perform health check synchronously."""
        raise NotImplementedError("Health check not yet implemented for Taskiq")

    async def health_check_async(self) -> List[Dict]:
        """Perform health check asynchronously."""
        raise NotImplementedError("Health check not yet implemented for Taskiq")
