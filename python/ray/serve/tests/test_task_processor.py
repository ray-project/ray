import sys
import tempfile
from pathlib import Path

import httpx
import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve.schema import CeleryConfig, TaskProcessorConfig
from ray.serve.task_consumer import (
    get_task_adapter,
    task_consumer,
    task_handler,
)


@pytest.fixture(scope="function")
def temp_queue_directory():
    """
    Creates a temporary directory with 'queue', 'results', and 'control' subdirectories for task consumer tests.
    This fixture uses 'yield' to ensure the directory persists for the duration of the test.

    Yields:
        dict: Contains 'queue_path', 'results_path', and 'control_path' for easy access
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        data_folder_queue = tmpdir_path / "queue"
        data_folder_queue.mkdir()

        results_path = tmpdir_path / "results"
        results_path.mkdir()

        control_path = tmpdir_path / "control"
        control_path.mkdir()

        yield {
            "queue_path": data_folder_queue,
            "results_path": results_path,
            "control_path": control_path,
        }


class TestTaskConsumerWithRayServe:
    """Test task consumer integration with Ray Serve."""

    DEFAULT_QUEUE_NAME = "my_default_app_queue"
    DEFAULT_BROKER_URL = "filesystem://"

    @classmethod
    def _create_transport_options(cls, queue_path, control_path):
        """Create standard transport options for filesystem broker."""
        return {
            # Incoming message queue - where new task messages are written when sent to broker
            "data_folder_in": queue_path,
            # Outgoing message storage - where task results and responses are written after completion
            "data_folder_out": queue_path,
            # Processed message archive - where messages are moved after successful processing
            "data_folder_processed": queue_path,
            # Control message storage - where Celery management and control commands are stored
            "control_folder": control_path,
        }

    @classmethod
    def _create_processor_config(cls, temp_queue_directory, **kwargs):
        """Create a TaskProcessorConfig with common defaults."""
        queue_path = temp_queue_directory["queue_path"]
        control_path = temp_queue_directory["control_path"]
        results_path = temp_queue_directory["results_path"]

        transport_options = cls._create_transport_options(queue_path, control_path)

        config_params = {
            "queue_name": cls.DEFAULT_QUEUE_NAME,
            "adapter_config": CeleryConfig(
                broker_url=cls.DEFAULT_BROKER_URL,
                backend_url=f"file://{results_path}",
                broker_transport_options=transport_options,
            ),
        }
        config_params.update(kwargs)

        return TaskProcessorConfig(**config_params)

    @staticmethod
    def _create_send_request_to_queue_remote(processor_config):
        """Create a Ray remote function for sending requests to the queue."""

        @ray.remote
        def send_request_to_queue(data):
            celery_adapter = get_task_adapter(config=processor_config)
            result = celery_adapter.enqueue_task_sync("process_request", args=[data])
            assert result.id is not None
            return result.id

        return send_request_to_queue

    def test_task_consumer_as_serve_deployment(
        self, temp_queue_directory, serve_instance
    ):
        """Test that task consumers can be used as Ray Serve deployments."""
        processor_config = self._create_processor_config(temp_queue_directory)
        send_request_to_queue = self._create_send_request_to_queue_remote(
            processor_config
        )

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            def __init__(self):
                self.data_received = None
                self.task_received = False

            @task_handler(name="process_request")
            def process_request(self, data):
                self.task_received = True
                self.data_received = data

            def assert_task_received(self):
                assert self.task_received is True
                assert self.data_received is not None
                assert self.data_received == "test_data_1"

            async def __call__(self, request: Request):
                try:
                    self.assert_task_received()
                    return JSONResponse({"status": "success"}, status_code=200)
                except Exception as e:
                    return JSONResponse(
                        {"status": "error", "detail": str(e)}, status_code=500
                    )

        # Deploy the consumer as a Serve deployment
        serve.run(ServeTaskConsumer.bind())
        send_request_to_queue.remote("test_data_1")

        def assert_result():
            response = httpx.get("http://localhost:8000/assert-result")
            if response.status_code == 200:
                return True
            else:
                return False

        wait_for_condition(assert_result)

    def test_task_consumer_as_serve_deployment_with_failed_task(
        self, temp_queue_directory, serve_instance
    ):
        """Test that task consumers can be used as Ray Serve deployments."""
        processor_config = self._create_processor_config(
            temp_queue_directory, failed_task_queue_name="my_failed_task_queue"
        )

        send_request_to_queue = self._create_send_request_to_queue_remote(
            processor_config
        )

        @ray.remote
        def get_task_status(task_id):
            celery_adapter = get_task_adapter(config=processor_config)
            return celery_adapter.get_task_status_sync(task_id)

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            @task_handler(name="process_request")
            def process_request(self, data):
                raise ValueError("Task failed as expected")

        serve.run(ServeTaskConsumer.bind())
        task_id_ref = send_request_to_queue.remote("test_data_1")
        task_id = ray.get(task_id_ref)

        def assert_result():
            status_ref = get_task_status.remote(task_id)
            result = ray.get(status_ref)

            if (
                result.status == "FAILURE"
                and result.result is not None
                and isinstance(result.result, ValueError)
                and str(result.result) == "Task failed as expected"
            ):
                return True
            else:
                return False

        wait_for_condition(assert_result, timeout=5)

    def test_task_consumer_as_serve_deployment_with_async_task_handler(
        self, temp_queue_directory, serve_instance
    ):
        """Test that task consumers properly raise NotImplementedError for async task handlers."""
        processor_config = self._create_processor_config(temp_queue_directory)

        # Test that async task handlers raise NotImplementedError during decoration
        with pytest.raises(
            NotImplementedError,
            match="Async task handlers are not supported yet",
        ):

            @serve.deployment
            @task_consumer(task_processor_config=processor_config)
            class ServeTaskConsumer:
                def __init__(self):
                    self.data_received = None
                    self.task_received = False

                # This async task handler should raise NotImplementedError during decoration
                @task_handler(name="process_request")
                async def process_request(self, data):
                    self.task_received = True
                    self.data_received = data


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
