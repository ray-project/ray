import logging
from typing import Any, Dict

import ray
from ray.serve._private.broker import Broker
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Actor name prefix for QueueMonitor actors
QUEUE_MONITOR_ACTOR_PREFIX = "QUEUE_MONITOR::"


class QueueMonitorConfig:
    """Configuration for the QueueMonitor deployment."""

    def __init__(
        self,
        broker_url: str,
        queue_name: str,
        rabbitmq_http_url: str = "http://guest:guest@localhost:15672/api/",
    ):
        self.broker_url = broker_url
        self.queue_name = queue_name
        self.rabbitmq_http_url = rabbitmq_http_url


@ray.remote(num_cpus=0)
class QueueMonitorActor:

    """
    Actor that monitors queue length by directly querying the broker.

    Returns pending tasks in the queue.

    Uses native broker clients:
        - Redis: Uses redis-py library with LLEN command
        - RabbitMQ: Uses HTTP management API
    """

    def __init__(self, config: QueueMonitorConfig):
        self._config = config
        self._last_queue_length: int = 0
        self._is_initialized: bool = False

        self._broker = Broker(
            self._config.broker_url, http_api=self._config.rabbitmq_http_url
        )
        self._is_initialized = True

    def get_config(self) -> Dict[str, Any]:
        """
        Get the QueueMonitor configuration as a serializable dict.

        Returns:
            Dict with 'broker_url' and 'queue_name' keys
        """
        return {
            "broker_url": self._config.broker_url,
            "queue_name": self._config.queue_name,
            "rabbitmq_http_url": self._config.rabbitmq_http_url,
        }

    async def get_queue_length(self) -> int:
        """
        Get the current queue length from the broker.

        Returns:
            Number of pending tasks in the queue.
        """
        if not self._is_initialized:
            logger.warning(
                f"QueueMonitor not initialized for queue '{self._config.queue_name}', returning 0"
            )
            return 0

        try:
            queues = await self._broker.queues([self._config.queue_name])
            print(f"queues: {queues}")
            if queues is not None:
                for q in queues:
                    if q.get("name") == self._config.queue_name:
                        queue_length = q.get("messages")
                        self._last_queue_length = queue_length
                        return queue_length

            print(f"last_queue_length: {self._last_queue_length}")

            if self._last_queue_length is not None:
                return self._last_queue_length
            else:
                logger.warning(
                    f"No queue length found for queue '{self._config.queue_name}', returning 0"
                )
                return 0

        except Exception as e:
            print(f"error 123123: {e}")
            logger.warning(
                f"Failed to query queue length: {e}. Using last known value: {self._last_queue_length}"
            )
            return self._last_queue_length

    def shutdown(self) -> None:
        if self._broker is not None:
            self._broker.close()
            self._broker = None
        self._is_initialized = False


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


def kill_queue_monitor_actor(
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
