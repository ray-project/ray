import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from celery import Celery

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.schema import CeleryTaskProcessorConfig, TaskProcessorConfig, TaskResult

logger = logging.getLogger(SERVE_LOGGER_NAME)


class TaskProcessorAdapter(ABC):
    @abstractmethod
    def initialize(self, config: TaskProcessorConfig):
        pass

    @abstractmethod
    def register_task_handle(self, func, name=None):
        pass

    @abstractmethod
    def enqueue_task(self, task_name, args=None, kwargs=None, **options) -> TaskResult:
        pass

    @abstractmethod
    def get_task_status(self, task_id) -> Dict[str, Any]:
        pass

    @abstractmethod
    def cancel_task(self, task_id) -> bool:
        pass

    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def start_consumer(self, **kwargs):
        pass

    @abstractmethod
    def stop_consumer(self, timeout: float = 10.0):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    @abstractmethod
    def health_check(self):
        pass


class CeleryTaskProcessorAdapter(TaskProcessorAdapter):
    _app: Celery
    _config: TaskProcessorConfig
    _worker_thread: Optional[threading.Thread] = None

    def __init__(self, config: TaskProcessorConfig):
        if not isinstance(config.adapter_config, CeleryTaskProcessorConfig):
            raise TypeError(
                "TaskProcessorConfig.adapter_config must be an instance of CeleryTaskProcessorConfig"
            )

        self._config = config

    def initialize(self, config: TaskProcessorConfig):
        self._app = Celery(
            config.queue_name,
            backend=config.adapter_config.backend_url,
            broker=config.adapter_config.broker_url,
        )

        self._app.conf.update(
            loglevel="info",
            worker_pool="threads",
            worker_concurrency=10,
            task_max_retries=config.max_retry,
            task_default_queue=config.queue_name,
            task_ignore_result=False,
            task_acks_late=True,
            reject_on_worker_lost=True,
        )

        if config.adapter_config.broker_transport_options is not None:
            self._app.conf.update(
                broker_transport_options=config.adapter_config.broker_transport_options,
            )

        ### TODO(harshit|2025-07-22): add the failed_task_queue_name and unprocessable_task_queue_name business logic here

    def register_task_handle(self, func, name=None):
        if name:
            self._app.task(name=name)(func)
        else:
            self._app.task(func)

    def enqueue_task(self, task_name, args=None, kwargs=None, **options) -> TaskResult:
        task_response = self._app.send_task(
            task_name, args=args, queue=self._config.queue_name
        )

        return TaskResult(
            id=task_response.id,
            status=task_response.status,
            created_at=time.time(),
            result=task_response.result,
        )

    def get_task_status(self, task_id) -> Dict[str, Any]:
        task_details = self._app.AsyncResult(task_id)
        return {
            "id": task_details.id,
            "result": task_details.result,
            "status": task_details.status,
        }

    def cancel_task(self, task_id) -> bool:
        return self._app.AsyncResult(task_id).cancel()

    def get_metrics(self) -> Dict[str, Any]:
        return self._app.control.inspect().stats()

    def start_consumer(self, **kwargs):
        """Starts the Celery worker thread."""
        if self._worker_thread is not None and self._worker_thread.is_alive():
            logger.info("Celery worker thread is already running.")
            return

        self._worker_thread = threading.Thread(
            target=self._app.worker_main,
            args=(("worker", f"--hostname={self._app.main}"),),
        )
        self._worker_thread.start()

        logger.info(f"Celery worker thread started with hostname: {self._app.main}")

    def stop_consumer(self, timeout: float = 10.0):
        """Signals the Celery worker to shut down and waits for it to terminate."""
        if self._worker_thread is None or not self._worker_thread.is_alive():
            logger.info("Celery worker thread is not running.")
            return

        logger.info("Sending shutdown signal to Celery worker...")

        # Use the worker's hostname for targeted shutdown
        self._app.control.broadcast(
            "shutdown", destination=[f"celery@{self._app.main}"]
        )
        self._worker_thread.join(timeout=timeout)

        if self._worker_thread.is_alive():
            logger.warning(f"Worker thread did not terminate after {timeout} seconds.")
        else:
            logger.info("Celery worker thread has stopped.")

        self._worker_thread = None

    def shutdown(self):
        self._app.control.shutdown()

    def health_check(self):
        return self._app.control.ping()


def get_task_adapter(config: TaskProcessorConfig) -> TaskProcessorAdapter:
    """
    Factory function to instantiate the appropriate TaskProcessorAdapter
    based on the provided config.

    Currently only Celery is supported.

    Args:
        config: The configuration object for the task processor.

    Returns:
        An instance of a TaskProcessorAdapter subclass.

    Raises:
        ValueError: If the adapter_config type is not recognized.
    """

    if isinstance(config.adapter_config, CeleryTaskProcessorConfig):
        adapter = CeleryTaskProcessorAdapter(config=config)
        adapter.initialize(config=config)

        return adapter
    else:
        raise ValueError(f"Unknown adapter_config type: {type(config.adapter_config)}")
