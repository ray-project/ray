import asyncio
import concurrent.futures
import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from taskiq import AsyncTaskiqTask
from taskiq.message import TaskiqMessage
from taskiq.receiver.receiver import Receiver

from ray._common.pydantic_compat import BaseModel, Field
from ray._common.utils import import_attr
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

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Supported broker types and their required packages.
# Each entry maps a short broker_type name to the broker class import path,
# the pip package that provides it, and the constructor kwarg name for the
# queue/topic (so we can inject TaskProcessorConfig.queue_name automatically).
_BROKER_REGISTRY = {
    # Redis — standalone
    "redis_stream": {
        "import": "taskiq_redis.RedisStreamBroker",
        "package": "taskiq-redis",
        "queue_param": "queue_name",
        "required_kwargs": ["url"],
    },
    "redis_list": {
        "import": "taskiq_redis.ListQueueBroker",
        "package": "taskiq-redis",
        "queue_param": "queue_name",
        "required_kwargs": ["url"],
    },
    "redis_pubsub": {
        "import": "taskiq_redis.PubSubBroker",
        "package": "taskiq-redis",
        "queue_param": "queue_name",
        "required_kwargs": ["url"],
    },
    # Redis — cluster
    "redis_stream_cluster": {
        "import": "taskiq_redis.RedisStreamClusterBroker",
        "package": "taskiq-redis",
        "queue_param": "queue_name",
        "required_kwargs": ["url"],
    },
    "redis_list_cluster": {
        "import": "taskiq_redis.ListQueueClusterBroker",
        "package": "taskiq-redis",
        "queue_param": "queue_name",
        "required_kwargs": ["url"],
    },
    # Redis — sentinel
    "redis_stream_sentinel": {
        "import": "taskiq_redis.RedisStreamSentinelBroker",
        "package": "taskiq-redis",
        "queue_param": "queue_name",
        "required_kwargs": ["sentinels", "master_name"],
    },
    "redis_list_sentinel": {
        "import": "taskiq_redis.ListQueueSentinelBroker",
        "package": "taskiq-redis",
        "queue_param": "queue_name",
        "required_kwargs": ["sentinels", "master_name"],
    },
    "redis_pubsub_sentinel": {
        "import": "taskiq_redis.PubSubSentinelBroker",
        "package": "taskiq-redis",
        "queue_param": "queue_name",
        "required_kwargs": ["sentinels", "master_name"],
    },
    # RabbitMQ
    "rabbitmq": {
        "import": "taskiq_aio_pika.AioPikaBroker",
        "package": "taskiq-aio-pika",
        "queue_param": "queue_name",
        "required_kwargs": [],
    },
    # NATS
    "nats": {
        "import": "taskiq_nats.PullBasedJetStreamBroker",
        "package": "taskiq-nats",
        "queue_param": "subject",
        "required_kwargs": ["servers"],
    },
    # Kafka
    "kafka": {
        "import": "taskiq_aio_kafka.AioKafkaBroker",
        "package": "taskiq-aio-kafka",
        "queue_param": "kafka_topic",
        "required_kwargs": ["bootstrap_servers"],
    },
}


def _import_broker_class(broker_type: str):
    """Lazily import and return the broker class for the given broker_type."""
    entry = _BROKER_REGISTRY.get(broker_type)
    if entry is None:
        raise ValueError(
            f"Unsupported broker_type: {broker_type!r}. "
            f"Supported types: {list(_BROKER_REGISTRY.keys())}"
        )

    try:
        return import_attr(entry["import"])
    except (ImportError, ModuleNotFoundError) as e:
        raise ImportError(
            f"Broker {broker_type!r} requires package {entry['package']!r}. "
            f"Install it with: pip install {entry['package']}"
        ) from e


def _create_broker(
    broker_type: str,
    queue_name: str,
    broker_kwargs: Optional[Dict[str, Any]] = None,
):
    """Create a broker instance from the given config."""
    entry = _BROKER_REGISTRY.get(broker_type)
    if entry is None:
        raise ValueError(
            f"Unsupported broker_type: {broker_type!r}. "
            f"Supported types: {list(_BROKER_REGISTRY.keys())}"
        )

    kwargs = dict(broker_kwargs) if broker_kwargs else {}

    # Validate required kwargs are present.
    required = entry.get("required_kwargs", [])
    missing = [k for k in required if k not in kwargs]
    if missing:
        raise ValueError(
            f"Broker {broker_type!r} requires the following keys in "
            f"broker_kwargs: {missing}"
        )

    broker_cls = _import_broker_class(broker_type)

    # Inject the queue/topic name under the broker-specific parameter name.
    queue_param = entry["queue_param"]
    kwargs[queue_param] = queue_name

    return broker_cls(**kwargs)


@PublicAPI(stability="beta")
class TaskiqAdapterConfig(BaseModel):
    """
    Taskiq adapter config for async task processing in Ray Serve.

    Supports multiple brokers via ``broker_type``. Connection URLs and
    broker-specific constructor arguments are passed through ``broker_kwargs``.

    Example — Redis Streams::

        TaskiqAdapterConfig(
            broker_type="redis_stream",
            broker_kwargs={"url": "redis://localhost:6379"},
        )

    Example — Redis Sentinel::

        TaskiqAdapterConfig(
            broker_type="redis_stream_sentinel",
            broker_kwargs={
                "sentinels": [("sentinel1", 26379), ("sentinel2", 26379)],
                "master_name": "mymaster",
            },
        )

    Example — RabbitMQ::

        TaskiqAdapterConfig(
            broker_type="rabbitmq",
            broker_kwargs={
                "url": "amqp://guest:guest@localhost:5672",
                "exchange_name": "my_exchange",
            },
        )

    Example — NATS::

        TaskiqAdapterConfig(
            broker_type="nats",
            broker_kwargs={"servers": ["nats://host1:4222", "nats://host2:4222"]},
        )

    Example — Kafka::

        TaskiqAdapterConfig(
            broker_type="kafka",
            broker_kwargs={"bootstrap_servers": ["localhost:9092"]},
        )
    """

    broker_type: str = Field(
        ...,
        description=(
            "Broker backend to use. Supported values: "
            "'redis_stream', 'redis_list', 'redis_pubsub', "
            "'redis_stream_cluster', 'redis_list_cluster', "
            "'redis_stream_sentinel', 'redis_list_sentinel', 'redis_pubsub_sentinel', "
            "'rabbitmq', 'nats', 'kafka'."
        ),
    )
    broker_kwargs: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Keyword arguments passed directly to the broker constructor. "
            "Includes connection URLs and broker-specific options — refer to "
            "the taskiq broker documentation for available parameters."
        ),
    )

    # TODO(harshit): Support additional result backends (e.g., MongoDB, PostgreSQL, DynamoDB).
    # See: https://taskiq-python.github.io/available-components/result-backends.html
    result_backend_url: Optional[str] = Field(
        default=None,
        description=(
            "Redis URL for the result backend (e.g., 'redis://localhost:6379'). "
            "Currently only Redis is supported as a result backend, regardless "
            "of the broker type."
        ),
    )
    operation_timeout: Optional[float] = Field(
        default=30.0,
        description=(
            "Timeout in seconds for enqueue and status operations. "
            "Set to None to wait indefinitely."
        ),
    )


@PublicAPI(stability="beta")
class TaskiqTaskProcessorAdapter(TaskProcessorAdapter):
    """
    Taskiq-based task processor adapter for Ray Serve.

    Supports multiple brokers (Redis Streams, RabbitMQ, NATS, Kafka) via
    the ``broker_type`` field in ``TaskiqAdapterConfig``. Broker-specific
    options are passed through ``broker_kwargs``.

    Uses a persistent background event loop thread for all async taskiq
    operations. This keeps broker connections alive across sync method calls.
    """

    def __init__(self, config: TaskProcessorConfig, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not isinstance(config.adapter_config, TaskiqAdapterConfig):
            raise TypeError(
                "TaskProcessorConfig.adapter_config must be an instance of "
                "TaskiqAdapterConfig"
            )

        self._config = config
        self._broker = None
        self._result_backend = None
        self._broker_started = False
        self._consumer_concurrency = DEFAULT_CONSUMER_CONCURRENCY

        # Background event loop for async taskiq operations.
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._loop_lock = threading.Lock()

        # Consumer state.
        self._receiver = None
        self._finish_event: Optional[asyncio.Event] = None
        self._consumer_task: Optional[concurrent.futures.Future] = None

    # ------------------------------------------------------------------
    # Background event loop helpers
    # ------------------------------------------------------------------

    def _start_event_loop(self):
        """Thread target: run the event loop forever until stopped."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _ensure_loop(self):
        """Start the background event loop thread if not already running."""
        with self._loop_lock:
            if self._loop_thread is not None and self._loop_thread.is_alive():
                return

            self._loop = asyncio.new_event_loop()
            self._loop_thread = threading.Thread(
                target=self._start_event_loop,
                daemon=True,
                name="taskiq-event-loop",
            )
            self._loop_thread.start()

    def _run_async(self, coro, timeout: Optional[float] = None):
        """Schedule a coroutine on the background loop and block for its result."""
        self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    def _ensure_broker_started(self):
        """Start the broker if not already started (sync wrapper)."""
        if not self._broker_started:
            self._run_async(self._broker.startup())
            self._broker_started = True

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def initialize(self, consumer_concurrency: int = DEFAULT_CONSUMER_CONCURRENCY):
        """Initialize the taskiq broker, result backend, and background loop."""
        self._consumer_concurrency = consumer_concurrency
        adapter_config: TaskiqAdapterConfig = self._config.adapter_config

        # Create the broker using the factory function.
        self._broker = _create_broker(
            broker_type=adapter_config.broker_type,
            queue_name=self._config.queue_name,
            broker_kwargs=adapter_config.broker_kwargs,
        )

        # Create result backend only if explicitly configured.
        if adapter_config.result_backend_url:
            from taskiq_redis import RedisAsyncResultBackend

            self._result_backend = RedisAsyncResultBackend(
                redis_url=adapter_config.result_backend_url,
            )
            self._broker = self._broker.with_result_backend(self._result_backend)

        # Start the background event loop thread.
        self._ensure_loop()

        logger.info(
            f"Taskiq adapter initialized with broker_type={adapter_config.broker_type!r}, "
            f"queue: {self._config.queue_name}"
        )

    # ------------------------------------------------------------------
    # Task registration
    # ------------------------------------------------------------------

    def register_task_handle(self, func: Callable, name: Optional[str] = None):
        """Register a callable as a taskiq task.

        Taskiq natively supports both sync and async handlers.

        Bound methods are wrapped in a plain function because taskiq's
        decorator internals set ``__name__`` on the callable, which
        fails on bound method objects.
        """
        task_name = name or func.__name__

        # Wrap bound methods so taskiq can set __name__ on the wrapper.
        if hasattr(func, "__self__"):
            original = func

            def wrapper(*args, **kwargs):
                return original(*args, **kwargs)

            wrapper.__name__ = task_name
            wrapper.__qualname__ = task_name
            func = wrapper

        self._broker.register_task(func, task_name=task_name)
        logger.info(f"Registered task: {task_name!r}")

    # ------------------------------------------------------------------
    # Task enqueueing
    # ------------------------------------------------------------------

    def enqueue_task_sync(
        self, task_name: str, args=None, kwargs=None, **options
    ) -> TaskResult:
        """Enqueue a task by name. Returns immediately with PENDING status."""
        self._ensure_broker_started()
        return self._run_async(
            self._enqueue_task_async(task_name, args, kwargs, **options),
            timeout=self._config.adapter_config.operation_timeout,
        )

    async def _enqueue_task_async(
        self, task_name: str, args=None, kwargs=None, **options
    ) -> TaskResult:
        task_id = uuid.uuid4().hex
        message = TaskiqMessage(
            task_id=task_id,
            task_name=task_name,
            labels={},
            args=args or [],
            kwargs=kwargs or {},
        )
        broker_message = self._broker.formatter.dumps(message)
        await self._broker.kick(broker_message)

        return TaskResult(
            id=task_id,
            status="PENDING",
            created_at=time.time(),
            result=None,
        )

    # ------------------------------------------------------------------
    # Task status
    # ------------------------------------------------------------------

    def get_task_status_sync(self, task_id: str) -> TaskResult:
        """Retrieve the current status of a task from the result backend."""
        return self._run_async(
            self._get_task_status_async(task_id),
            timeout=self._config.adapter_config.operation_timeout,
        )

    async def _get_task_status_async(self, task_id: str) -> TaskResult:
        if self._result_backend is None:
            raise RuntimeError(
                "Cannot query task status: no result_backend_url configured "
                "in TaskiqAdapterConfig."
            )

        task_handle = AsyncTaskiqTask(
            task_id=task_id,
            result_backend=self._result_backend,
        )

        if await task_handle.is_ready():
            result = await task_handle.get_result()
            if result.is_err:
                return TaskResult(
                    id=task_id,
                    status="ERROR",
                    result=str(result.error),
                )
            return TaskResult(
                id=task_id,
                status="SUCCESS",
                result=result.return_value,
            )

        return TaskResult(id=task_id, status="PENDING", result=None)

    # ------------------------------------------------------------------
    # Consumer management
    # ------------------------------------------------------------------

    def start_consumer(self, **kwargs):
        """Start consuming tasks from the broker.

        Launches the taskiq ``Receiver`` on the background event loop.
        The receiver resolves task names via the broker's internal registry
        (populated by ``register_task_handle``).
        """
        if self._consumer_task is not None:
            logger.info("Taskiq consumer is already running.")
            return

        self._ensure_broker_started()

        # Python 3.10+ asyncio.Event is not loop-bound, so creating it here
        # on the main thread and using it on the background loop is safe.
        self._finish_event = asyncio.Event()
        self._receiver = Receiver(
            broker=self._broker,
            max_async_tasks=self._consumer_concurrency,
            run_startup=False,  # We already called broker.startup().
        )

        # Schedule listen() on the background loop. It runs until
        # finish_event is set (by stop_consumer).
        self._consumer_task = asyncio.run_coroutine_threadsafe(
            self._receiver.listen(self._finish_event),
            self._loop,
        )
        logger.info(
            f"Taskiq consumer started (concurrency={self._consumer_concurrency})"
        )

    def stop_consumer(self, timeout: float = 60.0):
        """Gracefully stop the consumer and shut down the broker."""
        if self._consumer_task is None:
            logger.info("Taskiq consumer is not running.")
            return

        # Signal the receiver to stop.
        self._loop.call_soon_threadsafe(self._finish_event.set)

        # Wait for the consumer task to finish.
        try:
            self._consumer_task.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            logger.warning(f"Taskiq consumer did not stop within {timeout}s.")
        except Exception as e:
            logger.warning(f"Taskiq consumer task exited with an exception: {e}")

        # Shut down the broker connections.
        if self._broker_started:
            try:
                self._run_async(self._broker.shutdown())
            except Exception as e:
                logger.warning(f"Failed to shut down taskiq broker: {e}")
            finally:
                self._broker_started = False

        self._consumer_task = None
        self._finish_event = None
        self._receiver = None
        logger.info("Taskiq consumer stopped.")

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    def cancel_task_sync(self, task_id: str):
        """Cancel a task. Taskiq does not provide built-in task cancellation."""
        raise NotImplementedError(
            "Task cancellation is not supported by Taskiq. "
            "See: https://github.com/taskiq-python/taskiq/issues/305"
        )

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def get_metrics_sync(self) -> Dict[str, Any]:
        raise NotImplementedError

    def health_check_sync(self) -> List[Dict]:
        raise NotImplementedError
