import logging
from typing import Any, Dict

import pika
import redis

import ray
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Actor name prefix for QueueMonitor actors
QUEUE_MONITOR_ACTOR_PREFIX = "QUEUE_MONITOR::"


class QueueMonitorConfig:
    """Configuration for the QueueMonitor deployment."""

    def __init__(self, broker_url: str, queue_name: str):
        self.broker_url = broker_url
        self.queue_name = queue_name

    @property
    def broker_type(self) -> str:
        url_lower = self.broker_url.lower()
        if url_lower.startswith("redis"):
            return "redis"
        elif url_lower.startswith("amqp") or url_lower.startswith("pyamqp"):
            return "rabbitmq"
        elif "sqs" in url_lower:
            return "sqs"
        else:
            return "unknown"


class QueueMonitor:
    """
    Actor that monitors queue length by directly querying the broker.

    Uses native broker clients:
        - Redis: Uses redis-py library with LLEN command
        - RabbitMQ: Uses pika library with passive queue declaration
    """

    def __init__(self, config: QueueMonitorConfig):
        self._config = config
        self._client: Any = None
        self._last_queue_length: int = 0
        self._is_initialized: bool = False

    def initialize(self) -> None:
        """
        Initialize connection to the broker.

        Creates the appropriate client based on broker type and tests the connection.
        """
        if self._is_initialized:
            return

        broker_type = self._config.broker_type
        try:
            if broker_type == "redis":
                self._init_redis()
            elif broker_type == "rabbitmq":
                self._init_rabbitmq()
            else:
                raise ValueError(
                    f"Unsupported broker type: {broker_type}. Supported: redis, rabbitmq"
                )

            self._is_initialized = True
            logger.info(
                f"QueueMonitor initialized for queue '{self._config.queue_name}' (broker: {broker_type})"
            )

        except Exception as e:
            logger.error(f"Failed to initialize QueueMonitor: {e}")
            raise

    def _init_redis(self) -> None:
        """Initialize Redis client."""
        self._client = redis.from_url(self._config.broker_url)

        # Test connection
        self._client.ping()

    def _init_rabbitmq(self) -> None:
        """Initialize RabbitMQ connection parameters."""
        # Store connection parameters - we'll create connections as needed
        self._connection_params = pika.URLParameters(self._config.broker_url)

        # Test connection
        connection = pika.BlockingConnection(self._connection_params)
        connection.close()

    def _get_redis_queue_length(self) -> int:
        return self._client.llen(self._config.queue_name)

    def _get_rabbitmq_queue_length(self) -> int:
        connection = pika.BlockingConnection(self._connection_params)
        try:
            channel = connection.channel()

            # Passive declaration - doesn't create queue, just gets info
            result = channel.queue_declare(queue=self._config.queue_name, passive=True)

            return result.method.message_count
        finally:
            connection.close()

    def get_config(self) -> Dict[str, str]:
        """
        Get the QueueMonitor configuration as a serializable dict.

        Returns:
            Dict with 'broker_url' and 'queue_name' keys
        """
        return {
            "broker_url": self._config.broker_url,
            "queue_name": self._config.queue_name,
        }

    def get_queue_length(self) -> int:
        """
        Get the current queue length from the broker.

        Returns:
            Number of pending tasks in the queue
        """
        if not self._is_initialized:
            logger.warning(
                f"QueueMonitor not initialized for queue '{self._config.queue_name}', returning 0"
            )
            return 0

        try:
            broker_type = self._config.broker_type

            if broker_type == "redis":
                queue_length = self._get_redis_queue_length()
            elif broker_type == "rabbitmq":
                queue_length = self._get_rabbitmq_queue_length()
            else:
                raise ValueError(f"Unsupported broker type: {broker_type}")

            # Update cache
            self._last_queue_length = queue_length

            return queue_length

        except Exception as e:
            logger.warning(
                f"Failed to query queue length: {e}. Using last known value: {self._last_queue_length}"
            )
            return self._last_queue_length

    def shutdown(self) -> None:
        if self._client is not None:
            try:
                if hasattr(self._client, "close"):
                    self._client.close()
            except Exception as e:
                logger.warning(f"Error closing client: {e}")

            self._client = None
            self._is_initialized = False

    def __del__(self):
        self.shutdown()


@ray.remote(num_cpus=0)
class QueueMonitorActor(QueueMonitor):
    """
    Ray actor version of QueueMonitor for direct access from ServeController.

    This is used instead of a Serve deployment because the autoscaling policy
    runs inside the ServeController, and using serve.get_deployment_handle()
    from within the controller causes a deadlock.
    """

    def __init__(self, config: QueueMonitorConfig):
        super().__init__(config)
        self.initialize()


def create_queue_monitor_actor(
    deployment_name: str,
    config: QueueMonitorConfig,
    namespace: str = "serve",
) -> ray.actor.ActorHandle:
    """
    Create a named QueueMonitor Ray actor.

    Args:
        deployment_name: Name of the deployment
        config: QueueMonitorConfig with broker URL and queue name
        namespace: Ray namespace for the actor

    Returns:
        ActorHandle for the QueueMonitor actor
    """
    full_actor_name = f"{QUEUE_MONITOR_ACTOR_PREFIX}{deployment_name}"

    # Check if actor already exists
    try:
        existing = ray.get_actor(full_actor_name, namespace=namespace)
        logger.info(f"QueueMonitor actor '{full_actor_name}' already exists, reusing")

        return existing
    except ValueError:
        pass  # Actor doesn't exist, create it

    actor = QueueMonitorActor.options(
        name=full_actor_name,
        namespace=namespace,
        lifetime="detached",
    ).remote(config)

    logger.info(
        f"Created QueueMonitor actor '{full_actor_name}' in namespace '{namespace}'"
    )
    return actor


def get_queue_monitor_actor(
    deployment_name: str,
    namespace: str = "serve",
) -> ray.actor.ActorHandle:
    """
    Get an existing QueueMonitor actor by name.

    Args:
        deployment_name: Name of the deployment
        namespace: Ray namespace

    Returns:
        ActorHandle for the QueueMonitor actor

    Raises:
        ValueError: If actor doesn't exist
    """
    full_actor_name = f"{QUEUE_MONITOR_ACTOR_PREFIX}{deployment_name}"
    return ray.get_actor(full_actor_name, namespace=namespace)


def delete_queue_monitor_actor(
    deployment_name: str,
    namespace: str = "serve",
) -> bool:
    """
    Delete a QueueMonitor actor by name.

    Args:
        deployment_name: Name of the deployment
        namespace: Ray namespace

    Returns:
        True if actor was deleted, False if it didn't exist
    """
    full_actor_name = f"{QUEUE_MONITOR_ACTOR_PREFIX}{deployment_name}"
    try:
        actor = ray.get_actor(full_actor_name, namespace=namespace)
        ray.kill(actor)
        logger.info(f"Deleted QueueMonitor actor '{full_actor_name}'")
        return True
    except ValueError:
        # Actor doesn't exist
        return False
