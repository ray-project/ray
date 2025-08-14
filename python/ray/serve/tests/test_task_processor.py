import json
import sys
import tempfile
from collections import defaultdict
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
def send_request_to_queue(
    processor_config: TaskProcessorConfig, data, task_name="process_request"
):
    adapter_instance = instantiate_adapter_from_config(
        task_processor_config=processor_config
    )
    result = adapter_instance.enqueue_task_sync(task_name, args=[data])
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


def _get_task_counts_by_routing_key(queue_path):
    """Counts tasks in a queue directory by reading the routing key from each message."""
    counts = defaultdict(int)
    if not queue_path.exists():
        return counts

    for msg_file in queue_path.iterdir():
        if msg_file.is_file():
            try:
                with open(msg_file, "r") as f:
                    data = json.load(f)
                    routing_key = (
                        data.get("properties", {})
                        .get("delivery_info", {})
                        .get("routing_key")
                    )
                    if routing_key:
                        counts[routing_key] += 1
            except (json.JSONDecodeError, IOError):
                # Ignore files that aren't valid JSON or are otherwise unreadable
                continue
    return counts


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
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

    def test_task_consumer_as_serve_deployment_with_unknown_task(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that task consumers can be used as Ray Serve deployments."""
        processor_config = create_processor_config()
        processor_config.unprocessable_task_queue_name = "unprocessable_task_queue"

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            @task_handler(name="process_request")
            def process_request(self, data):
                pass

        serve.run(ServeTaskConsumer.bind())
        send_request_to_queue.remote(
            processor_config, "test_data_1", task_name="unregistered_task"
        )

        def assert_queue_task_counts():
            queue_path = Path(temp_queue_directory["queue_path"])
            counts = _get_task_counts_by_routing_key(queue_path)

            main_queue_tasks = counts.get(processor_config.queue_name, 0)
            unprocessable_tasks = counts.get(
                processor_config.unprocessable_task_queue_name, 0
            )
            # The initial unknown task should be gone from the main queue and one new
            # task should be in the unprocessable queue.
            return main_queue_tasks == 0 and unprocessable_tasks == 1

        wait_for_condition(assert_queue_task_counts, timeout=10)

    def test_task_consumer_as_serve_deployment_with_failed_task_and_dead_letter_queue(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that task consumers can be used as Ray Serve deployments."""
        processor_config = create_processor_config()
        processor_config.failed_task_queue_name = "failed_task_queue"

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            @task_handler(name="process_request")
            def process_request(self, data):
                raise ValueError("Task failed as expected")

        serve.run(ServeTaskConsumer.bind())
        send_request_to_queue.remote(processor_config, "test_data_1")

        def assert_queue_task_counts():
            queue_path = Path(temp_queue_directory["queue_path"])
            counts = _get_task_counts_by_routing_key(queue_path)

            main_queue_tasks = counts.get(processor_config.queue_name, 0)
            failed_tasks = counts.get(processor_config.failed_task_queue_name, 0)
            # The initial unknown task should be gone from the main queue and one new
            # task should be in the failed queue.
            return main_queue_tasks == 0 and failed_tasks == 1

        wait_for_condition(assert_queue_task_counts, timeout=15)

    def test_task_consumer_with_mismatched_arguments(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that tasks with mismatched arguments are sent to the failed task queue."""
        processor_config = create_processor_config()
        processor_config.unprocessable_task_queue_name = "unprocessable_task_queue"
        processor_config.failed_task_queue_name = "failed_task_queue"

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            @task_handler(name="process_request")
            def process_request(self, arg1, arg2):  # Expects two arguments
                pass

        serve.run(ServeTaskConsumer.bind())

        # Send a task with only one argument, which should cause a TypeError.
        send_request_to_queue.remote(processor_config, ["test_data_1"])

        def assert_queue_task_counts():
            queue_path = Path(temp_queue_directory["queue_path"])
            counts = _get_task_counts_by_routing_key(queue_path)
            main_queue_tasks = counts.get(processor_config.queue_name, 0)
            unprocessable_tasks = counts.get(
                processor_config.unprocessable_task_queue_name, 0
            )

            return main_queue_tasks == 0 and unprocessable_tasks == 1

        wait_for_condition(assert_queue_task_counts, timeout=15)

    def test_task_consumer_with_argument_type_mismatch(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that tasks with argument type mismatches are sent to the failed task queue."""
        processor_config = create_processor_config()
        processor_config.unprocessable_task_queue_name = "unprocessable_task_queue"
        processor_config.failed_task_queue_name = "failed_task_queue"

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            @task_handler(name="process_request")
            def process_request(self, data: str):
                return len(data)  # This will fail if data is not a sequence

        serve.run(ServeTaskConsumer.bind())
        # Send an integer, for which len() is undefined, causing a TypeError.
        send_request_to_queue.remote(processor_config, 12345)

        def assert_queue_task_counts():
            queue_path = Path(temp_queue_directory["queue_path"])
            counts = _get_task_counts_by_routing_key(queue_path)
            main_queue_tasks = counts.get(processor_config.queue_name, 0)
            unprocessable_tasks = counts.get(
                processor_config.unprocessable_task_queue_name, 0
            )

            return main_queue_tasks == 0 and unprocessable_tasks == 1

        wait_for_condition(assert_queue_task_counts, timeout=15)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
