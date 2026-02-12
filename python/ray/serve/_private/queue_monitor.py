import logging
import time

import ray
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray.actor import ActorHandle
from ray.serve._private.broker import Broker
from ray.serve._private.common import (
    AsyncInferenceTaskQueueMetricReport,
    DeploymentID,
)
from ray.serve._private.constants import (
    RAY_SERVE_ASYNC_INFERENCE_TASK_QUEUE_METRIC_PUSH_INTERVAL_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.metrics_utils import MetricsPusher

logger = logging.getLogger(SERVE_LOGGER_NAME)

# Actor name prefix for QueueMonitor actors
QUEUE_MONITOR_ACTOR_PREFIX = "QUEUE_MONITOR::"


def get_queue_monitor_actor_name(deployment_id: DeploymentID) -> str:
    """Get the Ray actor name for a deployment's QueueMonitor.

    Args:
        deployment_id: ID of the deployment (contains app_name and name)

    Returns:
        The full actor name in format "QUEUE_MONITOR::<app_name>#<deployment_name>#"
    """
    return f"{QUEUE_MONITOR_ACTOR_PREFIX}{deployment_id.app_name}#{deployment_id.name}#"


@ray.remote(num_cpus=0)
class QueueMonitorActor:
    """
    Actor that monitors queue length by directly querying the broker.

    Returns pending tasks in the queue.

    Uses native broker clients:
        - Redis: Uses redis-py library with LLEN command
        - RabbitMQ: Uses HTTP management API

    Periodically pushes queue length metrics to the controller for autoscaling.
    """

    PUSH_METRICS_TO_CONTROLLER_TASK_NAME = "push_metrics_to_controller"

    async def __init__(
        self,
        broker_url: str,
        queue_name: str,
        deployment_id: DeploymentID,
        controller_handle: ActorHandle,
        rabbitmq_http_url: str = "http://guest:guest@localhost:15672/api/",
    ):
        self._broker_url = broker_url
        self._queue_name = queue_name
        self._deployment_id = deployment_id
        self._controller_handle = controller_handle
        self._rabbitmq_http_url = rabbitmq_http_url

        self._broker = Broker(self._broker_url, http_api=self._rabbitmq_http_url)

        self._metrics_pusher = MetricsPusher()
        self._start_metrics_pusher()

    def _start_metrics_pusher(self):
        """Start the metrics pusher to periodically push metrics to the controller."""
        self._metrics_pusher.register_or_update_task(
            self.PUSH_METRICS_TO_CONTROLLER_TASK_NAME,
            self._push_metrics_to_controller,
            RAY_SERVE_ASYNC_INFERENCE_TASK_QUEUE_METRIC_PUSH_INTERVAL_S,
        )
        self._metrics_pusher.start()

    def __ray_shutdown__(self):
        # Note: This must be synchronous (not async) because Ray's core code
        # in _raylet.pyx calls __ray_shutdown__() without awaiting.
        if self._metrics_pusher is not None:
            self._metrics_pusher.stop_tasks()
            self._metrics_pusher = None
        if self._broker is not None:
            self._broker.close()
            self._broker = None

    async def get_queue_length(self) -> int:
        """
        Fetch queue length from the broker.

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

    async def _push_metrics_to_controller(self) -> None:
        """Push queue length metrics to the controller for autoscaling."""
        try:
            queue_length = await self.get_queue_length()
        except Exception as e:
            logger.warning(
                f"[{self._deployment_id}] Failed to get queue length for metrics push: {e}"
            )
            raise e

        report = AsyncInferenceTaskQueueMetricReport(
            deployment_id=self._deployment_id,
            queue_length=queue_length,
            timestamp_s=time.time(),
        )
        # Fire-and-forget push to controller
        self._controller_handle.record_autoscaling_metrics_from_async_inference_task_queue.remote(
            report
        )


def create_queue_monitor_actor(
    deployment_id: DeploymentID,
    broker_url: str,
    queue_name: str,
    controller_handle: ActorHandle,
    rabbitmq_http_url: str = "http://guest:guest@localhost:15672/api/",
    namespace: str = "serve",
) -> ray.actor.ActorHandle:
    """
    Create a named QueueMonitor Ray actor.

    Args:
        deployment_id: ID of the deployment (contains name and app_name)
        broker_url: URL of the message broker
        queue_name: Name of the queue to monitor
        controller_handle: Handle to the Serve controller for pushing metrics
        rabbitmq_http_url: HTTP API URL for RabbitMQ management (only for RabbitMQ)
        namespace: Ray namespace for the actor

    Returns:
        ActorHandle for the QueueMonitor actor
    """
    try:
        existing = get_queue_monitor_actor(deployment_id, namespace=namespace)
        logger.info(
            f"QueueMonitor actor for deployment '{deployment_id}' already exists, reusing"
        )
        return existing
    except ValueError:
        actor_name = get_queue_monitor_actor_name(deployment_id)
        actor = QueueMonitorActor.options(
            name=actor_name,
            namespace=namespace,
            max_restarts=-1,
            max_task_retries=-1,
            resources={HEAD_NODE_RESOURCE_NAME: 0.001},
        ).remote(
            broker_url=broker_url,
            queue_name=queue_name,
            deployment_id=deployment_id,
            controller_handle=controller_handle,
            rabbitmq_http_url=rabbitmq_http_url,
        )

        logger.info(
            f"Created QueueMonitor actor '{actor_name}' in namespace '{namespace}'"
        )
        return actor


def get_queue_monitor_actor(
    deployment_id: DeploymentID,
    namespace: str = "serve",
) -> ray.actor.ActorHandle:
    """
    Get an existing QueueMonitor actor by name.

    Args:
        deployment_id: ID of the deployment (contains app_name and name)
        namespace: Ray namespace

    Returns:
        ActorHandle for the QueueMonitor actor

    Raises:
        ValueError: If actor doesn't exist
    """
    actor_name = get_queue_monitor_actor_name(deployment_id)
    return ray.get_actor(actor_name, namespace=namespace)


def kill_queue_monitor_actor(
    deployment_id: DeploymentID,
    namespace: str = "serve",
) -> None:
    """
    Delete a QueueMonitor actor by name.

    Args:
        deployment_id: ID of the deployment (contains app_name and name)
        namespace: Ray namespace

    Raises:
        ValueError: If actor doesn't exist
    """
    actor_name = get_queue_monitor_actor_name(deployment_id)
    actor = get_queue_monitor_actor(deployment_id, namespace=namespace)

    ray.kill(actor, no_restart=True)
    logger.info(f"Deleted QueueMonitor actor '{actor_name}'")
