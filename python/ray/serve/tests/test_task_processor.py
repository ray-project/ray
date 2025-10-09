import json
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig
from ray.serve.task_consumer import (
    instantiate_adapter_from_config,
    task_consumer,
    task_handler,
)
from ray.tests.conftest import external_redis  # noqa: F401


@ray.remote
class ProcessedTasksTracker:
    def __init__(self):
        self.processed_tasks = set()

    def add_task(self, task_data):
        self.processed_tasks.add(task_data)

    def get_processed_tasks(self):
        return self.processed_tasks

    def get_count(self):
        return len(self.processed_tasks)


@ray.remote
def send_request_to_queue(
    processor_config: TaskProcessorConfig, data, task_name="process_request"
):
    adapter_instance_global = instantiate_adapter_from_config(
        task_processor_config=processor_config
    )
    result = adapter_instance_global.enqueue_task_sync(task_name, args=[data])
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
        "data_folder_in": str(queue_path),
        # Outgoing message storage - where task results and responses are written after completion
        "data_folder_out": str(queue_path),
        # Processed message archive - where messages are moved after successful processing
        "data_folder_processed": str(queue_path),
        # Control message storage - where Celery management and control commands are stored
        "control_folder": str(control_path),
    }


@pytest.fixture(scope="function")
def create_processor_config(temp_queue_directory, transport_options):
    """Create a TaskProcessorConfig with common defaults."""

    def _create(
        failed_task_queue_name=None, unprocessable_task_queue_name=None, **kwargs
    ):
        results_path = temp_queue_directory["results_path"]

        config_params = {
            "queue_name": "my_default_app_queue",
            "adapter_config": CeleryAdapterConfig(
                broker_url="filesystem://",
                backend_url=f"file://{results_path}",
                broker_transport_options=transport_options,
                worker_concurrency=1,
            ),
        }

        # Add dead letter queue names if provided
        if failed_task_queue_name is not None:
            config_params["failed_task_queue_name"] = failed_task_queue_name
        if unprocessable_task_queue_name is not None:
            config_params[
                "unprocessable_task_queue_name"
            ] = unprocessable_task_queue_name

        config_params.update(kwargs)

        return TaskProcessorConfig(**config_params)

    return _create


def _get_task_counts_by_routing_key(queue_path):
    """Counts tasks in a queue directory by reading the routing key from each message."""
    counts = defaultdict(int)
    if not queue_path.exists():
        return counts

    # Celery doesn't provide a way to get the queue size.
    # so we've to levarage the broker's API to get the queue size.
    # Since we are using the filesystem broker in tests, we can read the files in the queue directory to get the queue size.
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

        wait_for_condition(assert_result, timeout=20)

    def test_task_consumer_persistence_across_restarts(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that tasks persist in queue and get executed after deployment restart."""
        # Setup
        config = create_processor_config()
        tracker = ProcessedTasksTracker.remote()
        signal1 = SignalActor.remote()

        @serve.deployment(num_replicas=1, graceful_shutdown_timeout_s=60)
        @task_consumer(task_processor_config=config)
        class TaskConsumer:
            def __init__(self, tracker_ref, signal_ref):
                self.tracker, self.signal = tracker_ref, signal_ref
                self.local_processed = []

            @task_handler(name="process_request")
            def process_request(self, data):
                ray.get(self.signal.wait.remote())  # Block until signal
                self.local_processed.append(data)
                ray.get(self.tracker.add_task.remote(data))
                return f"Processed: {data}"

            def get_local_processed(self):
                return self.local_processed

        # Deploy first version and send tasks
        serve.run(TaskConsumer.bind(tracker, signal1), name="app_v1")

        num_tasks = 20
        for i in range(num_tasks):
            ray.get(send_request_to_queue.remote(config, f"task_{i}"))

        # Process exactly 1 task, then restart deployment
        wait_for_condition(
            lambda: ray.get(signal1.cur_num_waiters.remote()) == 1, timeout=10
        )
        ray.get(signal1.send.remote(clear=True))  # Allow 1 task to complete
        wait_for_condition(lambda: ray.get(tracker.get_count.remote()) == 1, timeout=10)

        # Shutdown first deployment
        serve.delete("app_v1", _blocking=False)
        ray.get(signal1.send.remote())  # Release any stuck tasks
        wait_for_condition(
            lambda: "app_v1" not in serve.status().applications, timeout=100
        )

        tasks_before_restart = ray.get(tracker.get_count.remote())
        assert (
            tasks_before_restart >= 2 and tasks_before_restart < num_tasks
        ), f"Expected at least 2 tasks processed and atleast one less than num_tasks, got {tasks_before_restart}"

        # Deploy second version and process remaining tasks
        signal2 = SignalActor.remote()
        handle = serve.run(TaskConsumer.bind(tracker, signal2), name="app_v2")

        wait_for_condition(
            lambda: ray.get(signal2.cur_num_waiters.remote()) == 1, timeout=10
        )
        ray.get(signal2.send.remote())  # Process all remaining tasks
        wait_for_condition(
            lambda: ray.get(tracker.get_count.remote()) == num_tasks, timeout=100
        )

        # Verify all tasks were processed and distributed correctly
        expected_tasks = {f"task_{i}" for i in range(num_tasks)}
        final_tasks = ray.get(tracker.get_processed_tasks.remote())
        second_deployment_tasks = handle.get_local_processed.remote().result()

        assert (
            final_tasks == expected_tasks
        ), f"Missing tasks: {expected_tasks - final_tasks}"
        assert (
            len(second_deployment_tasks) == num_tasks - tasks_before_restart
        ), f"Second deployment processed {len(second_deployment_tasks)} tasks, expected {num_tasks - tasks_before_restart}"

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

    def test_task_consumer_metrics(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that task processor metrics are collected and exposed correctly."""
        processor_config = create_processor_config()

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            def __init__(self):
                self.task_received = False

            @task_handler(name="process_request")
            def process_request(self, data):
                self.task_received = True

            def get_task_received(self) -> bool:
                return self.task_received

        handle = serve.run(ServeTaskConsumer.bind())
        send_request_to_queue.remote(processor_config, "test_data_1")

        def assert_task_received():
            return handle.get_task_received.remote().result()

        wait_for_condition(assert_task_received, timeout=20)

        adapter_instance = instantiate_adapter_from_config(
            task_processor_config=processor_config
        )
        metrics = adapter_instance.get_metrics_sync()

        assert len(metrics) == 1
        worker_name = next(iter(metrics))
        worker_stats = metrics[worker_name]

        # Check that the total number of processed tasks is correct.
        assert worker_stats["pool"]["threads"] == 1
        assert worker_stats["pool"]["max-concurrency"] == 1
        assert worker_stats["total"]["process_request"] == 1
        assert worker_stats["broker"]["transport"] == "filesystem"

    def test_task_consumer_health_check(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that the health check for the task processor works correctly."""
        processor_config = create_processor_config()

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            pass

        serve.run(ServeTaskConsumer.bind())

        adapter_instance = instantiate_adapter_from_config(
            task_processor_config=processor_config
        )

        def check_health():
            health_status = adapter_instance.health_check_sync()
            return len(health_status) > 0

        # Wait for the worker to be ready
        wait_for_condition(check_health, timeout=20)

        health_status = adapter_instance.health_check_sync()
        assert len(health_status) == 1

        worker_reply = health_status[0]
        assert len(worker_reply) == 1
        worker_name = next(iter(worker_reply))
        assert worker_reply[worker_name] == {"ok": "pong"}

    def test_task_processor_with_cancel_tasks(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test the cancel task functionality with celery broker."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")

        processor_config = TaskProcessorConfig(
            queue_name="my_app_queue",
            adapter_config=CeleryAdapterConfig(
                broker_url=f"redis://{redis_address}/0",
                backend_url=f"redis://{redis_address}/1",
                worker_concurrency=1,
            ),
        )

        signal = SignalActor.remote()

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class MyTaskConsumer:
            def __init__(self, signal_actor):
                self._signal = signal_actor
                self.message_received = []

            @task_handler(name="process")
            def process(self, data):
                ray.get(self._signal.wait.remote())
                self.message_received.append(data)

            def get_message_received(self):
                return self.message_received

        handle = serve.run(MyTaskConsumer.bind(signal), name="app_v1")

        task_ids = []
        for i in range(2):
            task_id_ref = send_request_to_queue.remote(
                processor_config, f"test_data_{i}", task_name="process"
            )
            task_ids.append(ray.get(task_id_ref))

        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) == 1, timeout=10
        )

        adapter_instance = instantiate_adapter_from_config(
            task_processor_config=processor_config
        )
        adapter_instance.cancel_task_sync(task_ids[1])

        ray.get(signal.send.remote())

        def check_revoked():
            status = adapter_instance.get_task_status_sync(task_ids[1])
            return status.status == "REVOKED"

        wait_for_condition(check_revoked, timeout=20)

        assert "test_data_0" in handle.get_message_received.remote().result()
        assert "test_data_1" not in handle.get_message_received.remote().result()

        serve.delete("app_v1")


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
class TestTaskConsumerWithDLQsConfiguration:
    """Test task consumer with dead letter queues."""

    def _assert_queue_counts(
        self,
        temp_queue_directory,
        processor_config,
        expected_main=0,
        expected_unprocessable=0,
        expected_failed=0,
        timeout=15,
    ):
        """Helper to assert expected task counts in different queues."""

        def check_counts():
            queue_path = Path(temp_queue_directory["queue_path"])
            counts = _get_task_counts_by_routing_key(queue_path)

            main_count = counts.get(processor_config.queue_name, 0)
            unprocessable_count = counts.get(
                getattr(processor_config, "unprocessable_task_queue_name", ""), 0
            )
            failed_count = counts.get(
                getattr(processor_config, "failed_task_queue_name", ""), 0
            )

            return (
                main_count == expected_main
                and unprocessable_count == expected_unprocessable
                and failed_count == expected_failed
            )

        wait_for_condition(check_counts, timeout=timeout)

    def test_task_consumer_as_serve_deployment_with_unknown_task(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that unknown tasks are sent to the unprocessable task queue."""
        processor_config = create_processor_config(
            unprocessable_task_queue_name="unprocessable_task_queue"
        )

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            @task_handler(name="process_request")
            def process_request(self, data):
                pass

        serve.run(ServeTaskConsumer.bind())

        # Send a task with an unknown name
        send_request_to_queue.remote(
            processor_config, "test_data_1", task_name="unregistered_task"
        )

        self._assert_queue_counts(
            temp_queue_directory,
            processor_config,
            expected_main=0,
            expected_unprocessable=1,
            timeout=10,
        )

    def test_task_consumer_as_serve_deployment_with_failed_task_and_dead_letter_queue(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that failed tasks are sent to the failed task queue."""
        processor_config = create_processor_config(
            failed_task_queue_name="failed_task_queue"
        )

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            @task_handler(name="process_request")
            def process_request(self, data):
                raise ValueError("Task failed as expected")

        serve.run(ServeTaskConsumer.bind())
        send_request_to_queue.remote(processor_config, "test_data_1")

        self._assert_queue_counts(
            temp_queue_directory, processor_config, expected_main=0, expected_failed=1
        )

    def test_task_consumer_with_mismatched_arguments(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that tasks with mismatched arguments are sent to the unprocessable task queue."""
        processor_config = create_processor_config(
            unprocessable_task_queue_name="unprocessable_task_queue",
            failed_task_queue_name="failed_task_queue",
        )

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            @task_handler(name="process_request")
            def process_request(self, arg1, arg2):  # Expects two arguments
                pass

        serve.run(ServeTaskConsumer.bind())

        # Send a task with only one argument, which should cause a TypeError
        send_request_to_queue.remote(processor_config, ["test_data_1"])

        self._assert_queue_counts(
            temp_queue_directory,
            processor_config,
            expected_main=0,
            expected_failed=1,
        )

    def test_task_consumer_with_argument_type_mismatch(
        self, temp_queue_directory, serve_instance, create_processor_config
    ):
        """Test that tasks with argument type mismatches are sent to the unprocessable task queue."""
        processor_config = create_processor_config(
            unprocessable_task_queue_name="unprocessable_task_queue",
            failed_task_queue_name="failed_task_queue",
        )

        @serve.deployment
        @task_consumer(task_processor_config=processor_config)
        class ServeTaskConsumer:
            @task_handler(name="process_request")
            def process_request(self, data: str):
                return len(data)  # This will fail if data is not a sequence

        serve.run(ServeTaskConsumer.bind())

        # Send an integer, for which len() is undefined, causing a TypeError
        send_request_to_queue.remote(processor_config, 12345)

        self._assert_queue_counts(
            temp_queue_directory,
            processor_config,
            expected_main=0,
            expected_failed=1,
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
