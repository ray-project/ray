"""Shared task-processor config.

One Redis instance is both the broker and the result backend. The ingress uses
this to enqueue; the consumer uses it (via @task_consumer) to consume.
"""

from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig

from constants import (
    FAILED_QUEUE,
    INDEX_QUEUE,
    MAX_RETRIES,
    REDIS_BACKEND_URL,
    REDIS_BROKER_URL,
    UNPROCESSABLE_QUEUE,
    VISIBILITY_TIMEOUT_S,
)

PROCESSOR_CONFIG = TaskProcessorConfig(
    queue_name=INDEX_QUEUE,
    adapter_config=CeleryAdapterConfig(
        broker_url=REDIS_BROKER_URL,
        backend_url=REDIS_BACKEND_URL,
        # Raise the Celery producer pool (default 10) so concurrent publishes
        # don't FIFO-block, and make publishes fire-and-forget.
        app_custom_config={
            "broker_pool_limit": 128,
            "task_publish_retry": False,
            "task_send_sent_event": False,
            "worker_send_task_events": False,
        },
        broker_transport_options={
            "visibility_timeout": VISIBILITY_TIMEOUT_S,
            "socket_keepalive": True,
            "health_check_interval": 30,
        },
    ),
    max_retries=MAX_RETRIES,
    failed_task_queue_name=FAILED_QUEUE,
    unprocessable_task_queue_name=UNPROCESSABLE_QUEUE,
)
