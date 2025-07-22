import sys
import tempfile
import time
from pathlib import Path

import httpx
import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve.schema import CeleryTaskProcessorConfig, TaskProcessorConfig
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

    def test_task_consumer_as_serve_deployment(
        self, temp_queue_directory, serve_instance
    ):
        """Test that task consumers can be used as Ray Serve deployments."""

        backend_url = "rpc://"
        broker_url = "filesystem://"
        queue_name = "my_default_app_queue"
        data_folder_queue = temp_queue_directory["queue_path"]
        control_path = temp_queue_directory["control_path"]

        transport_options = {
            "data_folder_in": data_folder_queue,
            "data_folder_out": data_folder_queue,
            "data_folder_processed": data_folder_queue,
            "control_folder": control_path,
        }

        processor_config = TaskProcessorConfig(
            queue_name=queue_name,
            adapter_config=CeleryTaskProcessorConfig(
                broker_url=broker_url,
                backend_url=backend_url,
                broker_transport_options=transport_options,
            ),
        )

        @ray.remote(num_cpus=2)
        def send_request_to_queue(data):
            celery_adapter = get_task_adapter(config=processor_config)
            result = celery_adapter.enqueue_task("process_request", args=[data])

            assert result.id is not None
            assert result.status in ("PENDING", "FAILED", "SUCCESS")

        @serve.deployment
        @task_consumer(taskProcessorConfig=processor_config)
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
                if request.url.path == "/assert-result":
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

        time.sleep(5)  # wait for the task to be processed

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

        broker_url = "filesystem://"
        queue_name = "my_default_app_queue"
        data_folder_queue = temp_queue_directory["queue_path"]
        control_path = temp_queue_directory["control_path"]

        results_path = temp_queue_directory["results_path"]
        backend_url = f"file://{results_path}"

        transport_options = {
            "data_folder_in": data_folder_queue,
            "data_folder_out": data_folder_queue,
            "data_folder_processed": data_folder_queue,
            "control_folder": control_path,
        }

        processor_config = TaskProcessorConfig(
            queue_name=queue_name,
            adapter_config=CeleryTaskProcessorConfig(
                broker_url=broker_url,
                backend_url=backend_url,
                broker_transport_options=transport_options,
            ),
            max_retry=3,
            failed_task_queue_name="my_failed_task_queue",
        )

        @ray.remote(num_cpus=2)
        def send_request_to_queue(data):
            celery_adapter = get_task_adapter(config=processor_config)
            result = celery_adapter.enqueue_task("process_request", args=[data])

            assert result.id is not None
            return result.id

        @ray.remote
        def get_task_status(task_id):
            celery_adapter = get_task_adapter(config=processor_config)
            return celery_adapter.get_task_status(task_id)

        @serve.deployment
        @task_consumer(taskProcessorConfig=processor_config)
        class ServeTaskConsumer:
            def __init__(self):
                self.data_received = None
                self.task_received = False

            @task_handler(name="process_request")
            def process_request(self, data):
                self.task_received = True
                self.data_received = data
                raise ValueError("Task failed as expected")

        serve.run(ServeTaskConsumer.bind())
        task_id_ref = send_request_to_queue.remote("test_data_1")
        task_id = ray.get(task_id_ref)

        def assert_result():
            status_ref = get_task_status.remote(task_id)
            status = ray.get(status_ref)

            if (
                status["status"] == "FAILURE"
                and status["result"] is not None
                and isinstance(status["result"], ValueError)
                and str(status["result"]) == "Task failed as expected"
            ):
                return True
            else:
                return False

        wait_for_condition(assert_result, timeout=5)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
