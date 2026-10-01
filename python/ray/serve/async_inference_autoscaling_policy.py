import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple, Union

from ray.serve._private.broker import Broker
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.config import AutoscalingContext

logger = logging.getLogger(SERVE_LOGGER_NAME)

DEFAULT_ASYNC_INFERENCE_QUEUE_POLL_INTERVAL_S = 10.0


class AsyncInferenceAutoscalingPolicy:
    """Autoscaling policy that scales replicas based on message queue length.

    Polls a message broker (Redis or RabbitMQ) for queue length and combines
    it with HTTP request load to compute the desired number of replicas.

    Polling uses one-shot async tasks instead of an infinite background loop.
    An infinite ``while True`` coroutine holds a strong reference to ``self``
    through the coroutine, and the event loop keeps the task alive, so
    ``__del__`` would never fire after the framework drops the policy on
    redeploy/deregistration — leaking both the poller and the broker
    connection. Instead, each poll is a single one-shot task kicked off from
    ``__call__`` when the poll interval has elapsed. The task completes
    naturally after one poll, so there is at most one short-lived in-flight
    task at any time and no cleanup is needed when the policy is
    garbage-collected.

    This policy is intended for use with ``@task_consumer`` deployments.
    Pass it as a class-based policy via ``AutoscalingPolicy``:

    .. code-block:: python

        from ray.serve.config import AutoscalingConfig, AutoscalingPolicy

        @serve.deployment(
            autoscaling_config=AutoscalingConfig(
                min_replicas=1,
                max_replicas=10,
                policy=AutoscalingPolicy(
                    policy_function=AsyncInferenceAutoscalingPolicy,
                    policy_kwargs={
                        "broker_url": "redis://localhost:6379/0",
                        "queue_name": "my_queue",
                    },
                ),
            ),
        )
        @task_consumer(task_processor_config=config)
        class MyConsumer: ...

    Args:
        broker_url: URL of the message broker (e.g. ``redis://localhost:6379/0``
            or ``amqp://guest:guest@localhost:5672//``).
        queue_name: Name of the queue to monitor.
        rabbitmq_management_url: RabbitMQ HTTP management API URL. Only required
            for RabbitMQ brokers (e.g. ``http://guest:guest@localhost:15672/api/``).
        poll_interval_s: How often (in seconds) to poll the broker for queue
            length. Defaults to 10s. Lower values increase responsiveness
            but add broker load.
    """

    def __init__(
        self,
        broker_url: str,
        queue_name: str,
        rabbitmq_management_url: Optional[str] = None,
        poll_interval_s: float = DEFAULT_ASYNC_INFERENCE_QUEUE_POLL_INTERVAL_S,
    ):
        self._broker_url = broker_url
        self._queue_name = queue_name
        self._rabbitmq_management_url = rabbitmq_management_url
        self._poll_interval_s = poll_interval_s

        self._queue_length: int = 0
        self._broker: Optional[Broker] = None
        self._task: Optional[asyncio.Task] = None
        self._last_poll_time: float = 0.0

    def _ensure_broker(self) -> None:
        """Lazily initialize the broker connection."""
        if self._broker is not None:
            return

        if self._rabbitmq_management_url is not None:
            self._broker = Broker(
                self._broker_url, http_api=self._rabbitmq_management_url
            )
        else:
            self._broker = Broker(self._broker_url)

    async def _poll_once(self) -> None:
        """Single one-shot poll of the broker for queue length."""
        try:
            queues = await self._broker.queues([self._queue_name])
            if queues is not None:
                for q in queues:
                    if q.get("name") == self._queue_name:
                        queue_length = q.get("messages")
                        if queue_length is not None:
                            self._queue_length = queue_length
                        break
        except Exception as e:
            logger.warning(f"Failed to get queue length for '{self._queue_name}': {e}")

    def __call__(
        self, ctx: AutoscalingContext
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        self._ensure_broker()

        # Clear completed poll task so a new one can be started.
        if self._task is not None and self._task.done():
            self._task = None

        # Start a new poll if the interval has elapsed and no poll is in-flight.
        now = time.monotonic()
        if self._task is None and (now - self._last_poll_time) >= self._poll_interval_s:
            self._last_poll_time = now
            self._task = asyncio.get_running_loop().create_task(self._poll_once())

        num_running_replicas = ctx.current_num_replicas
        total_workload = ctx.total_num_requests + self._queue_length
        config = ctx.config

        if num_running_replicas == 0:
            return 1 if total_workload > 0 else 0, {"queue_length": self._queue_length}

        target_num_requests = (
            config.get_target_ongoing_requests() * num_running_replicas
        )
        error_ratio = total_workload / target_num_requests
        desired_num_replicas = num_running_replicas * error_ratio

        return desired_num_replicas, {"queue_length": self._queue_length}
