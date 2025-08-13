import sys
import tempfile
from pathlib import Path

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig
from ray.serve.task_consumer import (
    instantiate_adapter_from_config,
    task_consumer,
    task_handler,
)


@ray.remote
def send_request_to_queue(processor_config: TaskProcessorConfig, data):
    adapter_instance = instantiate_adapter_from_config(
        task_processor_config=processor_config
    )
    result = adapter_instance.enqueue_task_sync("process_request", args=[data])
    assert result.id is not None
    return result.id


@pytest.fixture(scope="function")
def temp_queue_directory():
    """Creates a temporary directory with 'queue', 'results', and 'control' subdirectories for task consumer tests."""

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


@pytest.fixture(scope="function")
def transport_options(temp_queue_directory):
    """Create standard transport options for filesystem broker."""

    queue_path = temp_queue_directory["queue_path"]
    control_path = temp_queue_directory["control_path"]

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


@pytest.fixture(scope="function")
def create_processor_config(temp_queue_directory, transport_options):
    """Create a TaskProcessorConfig with common defaults."""

    def _create(**kwargs):
        results_path = temp_queue_directory["results_path"]

        config_params = {
            "queue_name": "my_default_app_queue",
            "adapter_config": CeleryAdapterConfig(
                broker_url="filesystem://",
                backend_url=f"file://{results_path}",
                broker_transport_options=transport_options,
            ),
        }
        config_params.update(kwargs)

        return TaskProcessorConfig(**config_params)

    return _create


class TestTaskConsumerWithRayServe:
    """Test task consumer integration with Ray Serve."""

    def test_task_consumer_as_serve_deployment(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that task consumers can be used as Ray Serve deployments."""
        processor_config = create_processor_config()

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

        # Deploy the consumer as a Serve deployment
        handle = serve.run(ServeTaskConsumer.bind())
        send_request_to_queue.remote(processor_config, "test_data_1")

        def assert_result():
            try:
                # `assert_task_received` will throw AssertionError if the task was not received or data is not as expected
                handle.assert_task_received.remote().result()
                return True
            except Exception:
                return False

        wait_for_condition(assert_result)

    def test_task_consumer_as_serve_deployment_with_failed_task(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that task consumers can be used as Ray Serve deployments."""
        processor_config = create_processor_config(
            failed_task_queue_name="my_failed_task_queue"
        )

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            def __init__(self):
                self.num_calls = 0

            @task_handler(name="process_request")
            def process_request(self, data):
                self.num_calls += 1
                raise ValueError("Task failed as expected")

            def get_num_calls(self):
                return self.num_calls

        handle = serve.run(ServeTaskConsumer.bind())
        task_id_ref = send_request_to_queue.remote(processor_config, "test_data_1")
        task_id = ray.get(task_id_ref)

        adapter_instance = instantiate_adapter_from_config(
            task_processor_config=processor_config
        )

        def assert_result():
            result = adapter_instance.get_task_status_sync(task_id)

            if (
                result.status == "FAILURE"
                and result.result is not None
                and isinstance(result.result, ValueError)
                and str(result.result) == "Task failed as expected"
                and handle.get_num_calls.remote().result()
                == 1 + processor_config.max_retries
            ):
                return True
            else:
                return False

        wait_for_condition(assert_result, timeout=10)

    def test_task_consumer_as_serve_deployment_with_async_task_handler(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that task consumers properly raise NotImplementedError for async task handlers."""
        processor_config = create_processor_config()

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
