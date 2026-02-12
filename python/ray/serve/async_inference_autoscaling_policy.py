import asyncio
import logging
from typing import Any, Dict, Optional, Tuple, Union

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.config import AutoscalingContext

logger = logging.getLogger(SERVE_LOGGER_NAME)

DEFAULT_ASYNC_INFERENCE_QUEUE_POLL_INTERVAL_S = 10.0


class AsyncInferenceAutoscalingPolicy:
    """Autoscaling policy that scales replicas based on message queue length.

    Polls a message broker (Redis or RabbitMQ) for queue length and combines
    it with HTTP request load to compute the desired number of replicas.

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
        broker_url: str = None,
        queue_name: str = None,
        rabbitmq_management_url: Optional[str] = None,
        poll_interval_s: Optional[float] = None,
    ):
        if not broker_url or not queue_name:
            raise ValueError(
                "AsyncInferenceAutoscalingPolicy requires 'broker_url' and "
                "'queue_name' in policy_kwargs. Example:\n"
                "  AutoscalingPolicy(\n"
                "      policy_function=AsyncInferenceAutoscalingPolicy,\n"
                '      policy_kwargs={"broker_url": "redis://...", '
                '"queue_name": "my_queue"},\n'
                "  )"
            )
        self._broker_url = broker_url
        self._queue_name = queue_name
        self._rabbitmq_management_url = rabbitmq_management_url
        self._poll_interval_s = (
            poll_interval_s
            if poll_interval_s is not None
            else DEFAULT_ASYNC_INFERENCE_QUEUE_POLL_INTERVAL_S
        )

        self._queue_length: int = 0
        self._broker = None
        self._task: Optional[asyncio.Task] = None
        self._started: bool = False

    def _ensure_started(self) -> None:
        """Lazily start the background queue polling task."""
        if self._started:
            return
        self._started = True

        from ray.serve._private.broker import Broker

        if self._rabbitmq_management_url is not None:
            self._broker = Broker(
                self._broker_url, http_api=self._rabbitmq_management_url
            )
        else:
            self._broker = Broker(self._broker_url)

        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._poll_queue())

    def __del__(self):
        if self._task is not None:
            self._task.cancel()
        if self._broker is not None:
            self._broker.close()

    async def _poll_queue(self) -> None:
        """Background loop that periodically queries the broker for queue length."""
        while True:
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
                logger.warning(
                    f"Failed to get queue length for '{self._queue_name}': {e}"
                )
            await asyncio.sleep(self._poll_interval_s)

    def __call__(
        self, ctx: AutoscalingContext
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        self._ensure_started()

        num_running_replicas = ctx.current_num_replicas
        total_workload = ctx.total_num_requests + self._queue_length
        config = ctx.config

        if num_running_replicas == 0:
            raise ValueError("Number of replicas cannot be zero")

        target_num_requests = (
            config.get_target_ongoing_requests() * num_running_replicas
        )
        error_ratio = total_workload / target_num_requests
        desired_num_replicas = num_running_replicas * error_ratio

        return desired_num_replicas, {"queue_length": self._queue_length}
