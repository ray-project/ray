import logging
from typing import Any, Callable, Dict, List, Optional

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


@PublicAPI(stability="beta")
class TaskiqTaskProcessorAdapter(TaskProcessorAdapter):
    """
    Taskiq-based task processor adapter for Ray Serve.

    Supports multiple brokers (Redis Streams, RabbitMQ, NATS, Kafka) via
    the ``broker_type`` field in ``TaskiqAdapterConfig``. Broker-specific
    options are passed through ``broker_kwargs``.
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

    def initialize(self, consumer_concurrency: int = DEFAULT_CONSUMER_CONCURRENCY):
        """Initialize the taskiq broker and result backend."""
        self._consumer_concurrency = consumer_concurrency
        adapter_config: TaskiqAdapterConfig = self._config.adapter_config

        # Create the broker using the factory function
        self._broker = _create_broker(
            broker_type=adapter_config.broker_type,
            queue_name=self._config.queue_name,
            broker_kwargs=adapter_config.broker_kwargs,
        )

        # Create result backend only if explicitly configured
        if adapter_config.result_backend_url:
            from taskiq_redis import RedisAsyncResultBackend

            self._result_backend = RedisAsyncResultBackend(
                redis_url=adapter_config.result_backend_url,
            )
            self._broker = self._broker.with_result_backend(self._result_backend)

        logger.info(
            f"Taskiq adapter initialized with broker_type={adapter_config.broker_type!r}, "
            f"queue: {self._config.queue_name}"
        )

    # ------------------------------------------------------------------
    # Abstract method stubs — full implementations in a follow-up PR.
    # ------------------------------------------------------------------

    def register_task_handle(self, func: Callable, name: Optional[str] = None):
        raise NotImplementedError

    def enqueue_task_sync(
        self, task_name: str, args=None, kwargs=None, **options
    ) -> TaskResult:
        raise NotImplementedError

    def get_task_status_sync(self, task_id: str) -> TaskResult:
        raise NotImplementedError

    def start_consumer(self, **kwargs):
        raise NotImplementedError

    def stop_consumer(self, timeout: float = 10.0):
        raise NotImplementedError

    def cancel_task_sync(self, task_id: str):
        raise NotImplementedError

    def get_metrics_sync(self) -> Dict[str, Any]:
        raise NotImplementedError

    def health_check_sync(self) -> List[Dict]:
        raise NotImplementedError
