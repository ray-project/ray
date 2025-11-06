import asyncio
import concurrent.futures
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from celery import Celery
from celery.signals import task_failure, task_unknown
from flower.utils.broker import Broker

from ray.serve import get_replica_context
from ray.serve._private.constants import (
    DEFAULT_CONSUMER_CONCURRENCY,
    SERVE_LOGGER_NAME,
)
from ray.serve.schema import (
    CeleryAdapterConfig,
    TaskProcessorAdapter,
    TaskProcessorConfig,
    TaskResult,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


CELERY_WORKER_POOL = "worker_pool"
CELERY_WORKER_CONCURRENCY = "worker_concurrency"
CELERY_TASK_IGNORE_RESULT = "task_ignore_result"
CELERY_TASK_ACKS_LATE = "task_acks_late"
CELERY_TASK_REJECT_ON_WORKER_LOST = "task_reject_on_worker_lost"

CELERY_DEFAULT_APP_CONFIG = [
    CELERY_WORKER_POOL,
    CELERY_WORKER_CONCURRENCY,
    CELERY_TASK_IGNORE_RESULT,
    CELERY_TASK_ACKS_LATE,
    CELERY_TASK_REJECT_ON_WORKER_LOST,
]


class FlowerQueueMonitor:
    """
    Thread-safe queue length monitor using Flower's broker utility.

    This class provides a broker-agnostic way to query queue lengths by:
    1. Running a background asyncio event loop in a dedicated thread
    2. Using Flower's Broker utility to query queue information
    3. Bridging async/sync with run_coroutine_threadsafe
    """

    def __init__(self, app: Celery):
        """Initialize the queue monitor with Celery app configuration."""
        self.app = app

        # Initialize Flower's Broker utility (broker-agnostic)
        # This utility handles Redis, RabbitMQ, SQS, and other brokers
        try:
            self.broker = Broker(
                app.connection().as_uri(include_password=True),
                broker_options=app.conf.broker_transport_options or {},
                broker_use_ssl=app.conf.broker_use_ssl or {},
            )
        except Exception as e:
            logger.error(f"Failed to initialize Flower Broker: {e}")
            raise

        # Event loop management
        self._loop = None
        self._loop_thread = None
        self._loop_ready = threading.Event()
        self._should_stop = threading.Event()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="FlowerQueueMonitor"
        )

    def start(self):
        """
        Start the background event loop in a dedicated thread.

        This creates a new thread that runs an asyncio event loop.
        The thread acts as the "main thread" for the event loop,
        avoiding signal handler issues.
        """
        if self._loop is not None:
            logger.warning("Queue monitor already started")
            return

        def _run_event_loop():
            """Run the event loop in the background thread."""
            try:
                # Create new event loop for this thread
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)

                # Signal that loop is ready
                self._loop_ready.set()

                # Run loop until stop is called
                while not self._should_stop.is_set():
                    self._loop.run_until_complete(asyncio.sleep(0.1))

            except Exception as e:
                logger.error(f"Error in event loop thread: {e}")
            finally:
                # Clean up loop
                if self._loop and not self._loop.is_closed():
                    self._loop.close()

        # Start event loop in background thread
        self._loop_thread = self._executor.submit(_run_event_loop)

        # Wait for loop to be ready (with timeout)
        if not self._loop_ready.wait(timeout=10):
            raise RuntimeError("Failed to start event loop thread within 10 seconds")

        logger.info("Queue monitor event loop started successfully")

    def stop(self):
        """Stop the background event loop and cleanup resources."""
        if self._loop is None:
            logger.info("Flower queue monitor not running, nothing to stop")
            return

        try:
            # Signal the loop to stop
            self._should_stop.set()

            # Wait for thread to finish (with timeout)
            if self._loop_thread:
                self._loop_thread.result(timeout=20)

            # Shutdown executor
            self._executor.shutdown(wait=True)

            logger.info("Queue monitor stopped successfully")

        except Exception as e:
            logger.error(f"Error stopping queue monitor: {e}")
        finally:
            self._loop = None
            self._loop_thread = None
            self._loop_ready.clear()
            self._should_stop.clear()

    def get_queue_lengths(
        self, queue_names: List[str], timeout: float = 5.0
    ) -> Dict[str, int]:
        """Get queue lengths synchronously (thread-safe)."""
        if self._loop is None or not self._loop_ready.is_set():
            logger.error("Event loop not initialized. Call start() first.")
            return {}

        try:
            # Schedule coroutine in background event loop thread
            future = asyncio.run_coroutine_threadsafe(
                self.broker.queues(queue_names), self._loop
            )

            # Wait for result with timeout
            queue_stats = future.result(timeout=timeout)

            # Convert to simple dict format
            queue_lengths = {}
            for queue_info in queue_stats:
                queue_name = queue_info.get("name")
                messages = queue_info.get("messages", 0)
                if queue_name:
                    queue_lengths[queue_name] = messages

            return queue_lengths

        except Exception as e:
            logger.error(f"Error getting queue lengths: {e}")
            return {}


@PublicAPI(stability="alpha")
class CeleryTaskProcessorAdapter(TaskProcessorAdapter):
    """
    Celery-based task processor adapter.
    This adapter does NOT support any async operations.
    All operations must be performed synchronously.
    """

    _app: Celery
    _config: TaskProcessorConfig
    _worker_thread: Optional[threading.Thread] = None
    _worker_hostname: Optional[str] = None
    _worker_concurrency: int = DEFAULT_CONSUMER_CONCURRENCY
    _queue_monitor: Optional[FlowerQueueMonitor] = None

    def __init__(self, config: TaskProcessorConfig, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not isinstance(config.adapter_config, CeleryAdapterConfig):
            raise TypeError(
                "TaskProcessorConfig.adapter_config must be an instance of CeleryAdapterConfig"
            )

        # Check if any app_custom_config keys conflict with default Celery app config
        if config.adapter_config.app_custom_config:
            conflicting_keys = set(
                config.adapter_config.app_custom_config.keys()
            ) & set(CELERY_DEFAULT_APP_CONFIG)
            if conflicting_keys:
                raise ValueError(
                    f"The following configuration keys cannot be changed via app_custom_config: {sorted(conflicting_keys)}. "
                    f"These are managed internally by the CeleryTaskProcessorAdapter."
                )

        self._config = config

        # Celery adapter does not support any async capabilities
        # self._async_capabilities is already an empty set from parent class

    def initialize(self, consumer_concurrency: int = DEFAULT_CONSUMER_CONCURRENCY):
        self._app = Celery(
            self._config.queue_name,
            backend=self._config.adapter_config.backend_url,
            broker=self._config.adapter_config.broker_url,
        )

        app_configuration = {
            CELERY_WORKER_POOL: "threads",
            CELERY_WORKER_CONCURRENCY: consumer_concurrency,
            CELERY_TASK_IGNORE_RESULT: False,  # Store task results so they can be retrieved after completion
            CELERY_TASK_ACKS_LATE: True,  # Acknowledge tasks only after completion (not when received) for better reliability
            CELERY_TASK_REJECT_ON_WORKER_LOST: True,  # Reject and requeue tasks when worker is lost to prevent data loss
        }

        if self._config.adapter_config.app_custom_config:
            app_configuration.update(self._config.adapter_config.app_custom_config)

        self._app.conf.update(app_configuration)

        queue_config = {
            self._config.queue_name: {
                "exchange": self._config.queue_name,
                "exchange_type": "direct",
                "routing_key": self._config.queue_name,
            },
        }

        if self._config.failed_task_queue_name:
            queue_config[self._config.failed_task_queue_name] = {
                "exchange": self._config.failed_task_queue_name,
                "exchange_type": "direct",
                "routing_key": self._config.failed_task_queue_name,
            }

        if self._config.unprocessable_task_queue_name:
            queue_config[self._config.unprocessable_task_queue_name] = {
                "exchange": self._config.unprocessable_task_queue_name,
                "exchange_type": "direct",
                "routing_key": self._config.unprocessable_task_queue_name,
            }

        self._app.conf.update(
            task_queues=queue_config,
            task_routes={
                # Default tasks go to main queue
                "*": {"queue": self._config.queue_name},
            },
        )

        if self._config.adapter_config.broker_transport_options is not None:
            self._app.conf.update(
                broker_transport_options=self._config.adapter_config.broker_transport_options,
            )

        if self._config.failed_task_queue_name:
            task_failure.connect(self._handle_task_failure)

        if self._config.unprocessable_task_queue_name:
            task_unknown.connect(self._handle_unknown_task)

        try:
            self._queue_monitor = FlowerQueueMonitor(self._app)
            logger.info("Queue monitor initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize flower queue monitor: {e}.")
            self._queue_monitor = None

    def register_task_handle(self, func, name=None):
        task_options = {
            "autoretry_for": (Exception,),
            "retry_kwargs": {"max_retries": self._config.max_retries},
            "retry_backoff": True,
            "retry_backoff_max": 60,  # Max backoff of 60 seconds
            "retry_jitter": False,  # Disable jitter for predictable testing
        }
        if self._config.adapter_config.task_custom_config:
            task_options.update(self._config.adapter_config.task_custom_config)

        if name:
            self._app.task(name=name, **task_options)(func)
        else:
            self._app.task(**task_options)(func)

    def enqueue_task_sync(
        self, task_name, args=None, kwargs=None, **options
    ) -> TaskResult:
        task_response = self._app.send_task(
            task_name,
            args=args,
            kwargs=kwargs,
            queue=self._config.queue_name,
            **options,
        )

        return TaskResult(
            id=task_response.id,
            status=task_response.status,
            created_at=time.time(),
            result=task_response.result,
        )

    def get_task_status_sync(self, task_id) -> TaskResult:
        task_details = self._app.AsyncResult(task_id)
        return TaskResult(
            id=task_details.id,
            result=task_details.result,
            status=task_details.status,
        )

    def start_consumer(self, **kwargs):
        """Starts the Celery worker thread and queue monitoring."""
        if self._worker_thread is not None and self._worker_thread.is_alive():
            logger.info("Celery worker thread is already running.")
            return

        unique_id = get_replica_context().replica_tag
        self._worker_hostname = f"{self._app.main}_{unique_id}"

        worker_args = [
            "worker",
            f"--hostname={self._worker_hostname}",
            "-Q",
            self._config.queue_name,
        ]

        self._worker_thread = threading.Thread(
            target=self._app.worker_main,
            args=(worker_args,),
        )
        self._worker_thread.start()

        logger.info(
            f"Celery worker thread started with hostname: {self._worker_hostname}"
        )

        # Start queue monitor
        if self._queue_monitor:
            try:
                self._queue_monitor.start()
                logger.info("Queue monitor started")
            except Exception as e:
                logger.error(f"Failed to start queue monitor: {e}")
                self._queue_monitor = None

    def stop_consumer(self, timeout: float = 10.0):
        """Signals the Celery worker to shut down and waits for it to terminate."""
        # Stop queue monitor first
        if self._queue_monitor:
            try:
                self._queue_monitor.stop()
                logger.info("Queue monitor stopped")
            except Exception as e:
                logger.warning(f"Error stopping queue monitor: {e}")
            finally:
                self._queue_monitor = None

        if self._worker_thread is None or not self._worker_thread.is_alive():
            logger.info("Celery worker thread is not running.")
            return

        logger.info("Sending shutdown signal to Celery worker...")

        # Use the worker's hostname for targeted shutdown
        self._app.control.broadcast(
            "shutdown", destination=[f"celery@{self._worker_hostname}"]
        )
        self._worker_thread.join(timeout=timeout)

        if self._worker_thread.is_alive():
            logger.warning(f"Worker thread did not terminate after {timeout} seconds.")
        else:
            logger.info("Celery worker thread has stopped.")

        self._worker_thread = None

    def shutdown(self):
        logger.info("Shutting down Celery worker...")
        self._app.control.shutdown()
        logger.info("Celery worker shutdown complete...")

    def cancel_task_sync(self, task_id):
        """
        Cancels a task synchronously. Only supported for Redis and RabbitMQ brokers by Celery.
        More details can be found here: https://docs.celeryq.dev/en/stable/userguide/workers.html#revoke-revoking-tasks
        """
        self._app.control.revoke(task_id)

    def get_metrics_sync(self) -> Dict[str, Any]:
        """
        Returns the metrics of the Celery worker synchronously.
        More details can be found here: https://docs.celeryq.dev/en/stable/reference/celery.app.control.html#celery.app.control.Inspect.stats
        """
        return self._app.control.inspect().stats()

    def health_check_sync(self) -> List[Dict]:
        """
        Checks the health of the Celery worker synchronously.
        Returns a list of dictionaries, each containing the worker name and a dictionary with the health status.
        Example: [{'celery@192.168.1.100': {'ok': 'pong'}}]
        More details can be found here: https://docs.celeryq.dev/en/stable/reference/celery.app.control.html#celery.app.control.Control.ping
        """
        return self._app.control.ping()

    def get_queue_lengths_sync(self) -> Dict[str, int]:
        """
        Get the lengths of all queues by querying broker directly.

        Returns:
            Dict[str, int]: A dictionary mapping queue names to their lengths.
                Returns empty dict if monitoring is unavailable.
        """
        if self._queue_monitor is None:
            logger.warning(
                "Flower queue monitor not initialized. Cannot retrieve queue lengths."
            )
            return {}

        # Collect all queue names to check
        queue_names = [self._config.queue_name]
        if self._config.failed_task_queue_name:
            queue_names.append(self._config.failed_task_queue_name)
        if self._config.unprocessable_task_queue_name:
            queue_names.append(self._config.unprocessable_task_queue_name)

        try:
            queue_lengths = self._queue_monitor.get_queue_lengths(queue_names)
            return queue_lengths
        except Exception as e:
            logger.error(f"Flower failed to retrieve queue lengths: {e}")
            return {}

    def _handle_task_failure(
        self,
        sender: Any = None,
        task_id: str = None,
        args: Any = None,
        kwargs: Any = None,
        einfo: Any = None,
        **kw,
    ):
        """Handle task failures and route them to appropriate dead letter queues.

        This method is called when a task fails after all retry attempts have been
        exhausted. It logs the failure and moves the task to failed_task_queue

        Args:
            sender: The task object that failed
            task_id: Unique identifier of the failed task
            args: Positional arguments passed to the task
            kwargs: Keyword arguments passed to the task
            einfo: Exception info object containing exception details and traceback
            **kw: Additional keyword arguments passed by Celery
        """
        logger.info(
            f"Task failure detected for task_id: {task_id}, einfo: {str(einfo)}"
        )

        dlq_args = [
            task_id,
            str(einfo.exception),
            str(args),
            str(kwargs),
            str(einfo),
        ]

        if self._config.failed_task_queue_name:
            self._move_task_to_queue(
                self._config.failed_task_queue_name,
                sender.name,
                dlq_args,
            )

            logger.error(
                f"Task {task_id} failed after max retries. Exception: {einfo}. Moved it to the {self._config.failed_task_queue_name} queue."
            )

    def _handle_unknown_task(
        self,
        sender: Any = None,
        name: str = None,
        id: str = None,
        message: Any = None,
        exc: Any = None,
        **kwargs,
    ):
        """Handle unknown or unregistered tasks received by Celery.

        This method is called when Celery receives a task that it doesn't recognize
        (i.e., a task that hasn't been registered with the Celery app). These tasks
        are moved to the unprocessable task queue if configured.

        Args:
            sender: The Celery app or worker that detected the unknown task
            name: Name of the unknown task
            id: Task ID of the unknown task
            message: The raw message received for the unknown task
            exc: The exception raised when trying to process the unknown task
            **kwargs: Additional context information from Celery
        """
        logger.info(
            f"Unknown task detected by Celery. Name: {name}, ID: {id}, Exc: {str(exc)}"
        )

        if self._config.unprocessable_task_queue_name:
            self._move_task_to_queue(
                self._config.unprocessable_task_queue_name,
                name,
                [
                    name,
                    id,
                    str(message),
                    str(exc),
                    str(kwargs),
                ],
            )

    def _move_task_to_queue(self, queue_name: str, task_name: str, args: list):
        """Helper function to move a task to a specified queue."""
        try:
            logger.info(
                f"Moving task: {task_name} to queue: {queue_name}, args: {args}"
            )
            self._app.send_task(
                name=task_name,
                queue=queue_name,
                args=args,
            )
        except Exception as e:
            logger.error(
                f"Failed to move task: {task_name} to queue: {queue_name}, error: {e}"
            )
            raise e
