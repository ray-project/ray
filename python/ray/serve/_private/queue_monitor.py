import logging
from typing import Any, Dict

import ray
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray.serve._private.broker import Broker
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Actor name prefix for QueueMonitor actors
QUEUE_MONITOR_ACTOR_PREFIX = "QUEUE_MONITOR::"


@ray.remote(num_cpus=0)
class QueueMonitorActor:
    """
    Actor that monitors queue length by directly querying the broker.

    Returns pending tasks in the queue.

    Uses native broker clients:
        - Redis: Uses redis-py library with LLEN command
        - RabbitMQ: Uses HTTP management API
    """

    def __init__(
        self,
        broker_url: str,
        queue_name: str,
        rabbitmq_http_url: str = "http://guest:guest@localhost:15672/api/",
    ):
        self._broker_url = broker_url
        self._queue_name = queue_name
        self._rabbitmq_http_url = rabbitmq_http_url

        self._broker = Broker(self._broker_url, http_api=self._rabbitmq_http_url)

    def __ray_shutdown__(self):
        if self._broker is not None:
            self._broker.close()
            self._broker = None

    def get_config(self) -> Dict[str, Any]:
        """
        Get the QueueMonitor configuration as a serializable dict.

        Returns:
            Dict with 'broker_url', 'queue_name', and 'rabbitmq_http_url' keys
        """
        return {
            "broker_url": self._broker_url,
            "queue_name": self._queue_name,
            "rabbitmq_http_url": self._rabbitmq_http_url,
        }

    async def get_queue_length(self) -> int:
        """
        Get the current queue length from the broker.

        Returns:
            Number of pending tasks in the queue.

        Raises:
            ValueError: If queue is not found in broker response or
                if queue data is missing the 'messages' field.
        """
        queues = await self._broker.queues([self._queue_name])
        if queues is not None:
            for q in queues:
                if q.get("name") == self._queue_name:
                    queue_length = q.get("messages")
                    if queue_length is None:
                        raise ValueError(
                            f"Queue '{self._queue_name}' is missing 'messages' field"
                        )
                    return queue_length

        raise ValueError(f"Queue '{self._queue_name}' not found in broker response")


def create_queue_monitor_actor(
    deployment_name: str,
    broker_url: str,
    queue_name: str,
    rabbitmq_http_url: str = "http://guest:guest@localhost:15672/api/",
    namespace: str = "serve",
) -> ray.actor.ActorHandle:
    """
    Create a named QueueMonitor Ray actor.

    Args:
        deployment_name: Name of the deployment
        broker_url: URL of the message broker
        queue_name: Name of the queue to monitor
        rabbitmq_http_url: HTTP API URL for RabbitMQ management (only for RabbitMQ)
        namespace: Ray namespace for the actor

    Returns:
        ActorHandle for the QueueMonitor actor
    """
    full_actor_name = f"{QUEUE_MONITOR_ACTOR_PREFIX}{deployment_name}"

    # Check if actor already exists
    try:
        existing = get_queue_monitor_actor(deployment_name, namespace=namespace)
        logger.info(f"QueueMonitor actor '{full_actor_name}' already exists, reusing")
        return existing
    except ValueError:
        actor = QueueMonitorActor.options(
            name=full_actor_name,
            namespace=namespace,
            max_restarts=-1,
            max_task_retries=-1,
            resources={HEAD_NODE_RESOURCE_NAME: 0.001},
        ).remote(broker_url, queue_name, rabbitmq_http_url)

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
) -> None:
    """
    Delete a QueueMonitor actor by name.

    Args:
        deployment_name: Name of the deployment
        namespace: Ray namespace

    Raises:
        ValueError: If actor doesn't exist
    """
    full_actor_name = f"{QUEUE_MONITOR_ACTOR_PREFIX}{deployment_name}"
    actor = get_queue_monitor_actor(deployment_name, namespace=namespace)

    ray.kill(actor, no_restart=True)
    logger.info(f"Deleted QueueMonitor actor '{full_actor_name}'")
