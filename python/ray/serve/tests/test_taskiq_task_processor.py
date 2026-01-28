import os
import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve.schema import TaskProcessorConfig
from ray.serve.task_consumer import (
    instantiate_adapter_from_config,
    task_consumer,
    task_handler,
)
from ray.serve.taskiq_task_processor import TaskiqAdapterConfig
from ray.serve.tests.test_task_processor import ProcessedTasksTracker
from ray.tests.conftest import external_redis  # noqa: F401


def create_taskiq_config(
    redis_address: str,
    queue_name: str = "test_queue",
    failed_task_queue_name: str = None,
    max_retries: int = 3,
    idle_timeout_ms: int = 600000,
    consumer_group_name: str = "taskiq",
) -> TaskProcessorConfig:
    """Helper to create TaskProcessorConfig for taskiq tests."""
    return TaskProcessorConfig(
        queue_name=queue_name,
        adapter="ray.serve.taskiq_task_processor.TaskiqTaskProcessorAdapter",
        adapter_config=TaskiqAdapterConfig(
            broker_url=f"redis://{redis_address}",
            result_backend_url=f"redis://{redis_address}",
            idle_timeout_ms=idle_timeout_ms,
            consumer_group_name=consumer_group_name,
        ),
        failed_task_queue_name=failed_task_queue_name,
        max_retries=max_retries,
    )


@ray.remote
def send_taskiq_request_to_queue(
    processor_config: TaskProcessorConfig, data, task_name="process_request"
):
    """Send a task to the taskiq queue."""
    adapter_instance = instantiate_adapter_from_config(
        task_processor_config=processor_config
    )
    result = adapter_instance.enqueue_task_sync(task_name, args=[data])
    assert result.id is not None
    return result.id


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
class TestTaskiqTaskConsumerWithRayServe:
    """End-to-end tests for Taskiq task consumer with Ray Serve."""

    def test_taskiq_task_consumer_as_serve_deployment(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test that taskiq task consumers work as Ray Serve deployments."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        processor_config = create_taskiq_config(
            redis_address, queue_name="taskiq_serve_test_queue"
        )

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=processor_config)
        class TaskiqServeConsumer:
            def __init__(self):
                self.data_received = None
                self.task_received = False

            @task_handler(name="process_request")
            async def process_request(self, data):
                self.task_received = True
                self.data_received = data

            def assert_task_received(self):
                assert self.task_received is True
                assert self.data_received is not None
                assert self.data_received == "test_data_taskiq"

        handle = serve.run(TaskiqServeConsumer.bind())
        send_taskiq_request_to_queue.remote(processor_config, "test_data_taskiq")

        def assert_result():
            try:
                handle.assert_task_received.remote().result()
                return True
            except Exception:
                return False

        wait_for_condition(assert_result, timeout=30)

    def test_taskiq_task_consumer_with_failed_task(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test taskiq task consumer handles failed tasks correctly."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        processor_config = create_taskiq_config(
            redis_address,
            queue_name="taskiq_failed_task_queue",
            max_retries=4,
        )

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=processor_config)
        class TaskiqFailingConsumer:
            def __init__(self):
                self.num_calls = 0

            @task_handler(name="process_request")
            def process_request(self, data):
                self.num_calls += 1
                raise ValueError("Taskiq task failed as expected")

            def get_num_calls(self):
                return self.num_calls

        handle = serve.run(TaskiqFailingConsumer.bind())
        task_id_ref = send_taskiq_request_to_queue.remote(
            processor_config, "test_data_fail"
        )
        task_id = ray.get(task_id_ref)

        adapter_instance = instantiate_adapter_from_config(
            task_processor_config=processor_config
        )

        def assert_failure():
            result = adapter_instance.get_task_status_sync(task_id)
            num_calls = handle.get_num_calls.remote().result()
            return (
                result.status == "FAILURE" and num_calls == 5
            )  # 4 retries + 1 initial attempt

        wait_for_condition(assert_failure, timeout=30)

    def test_taskiq_task_consumer_multiple_tasks(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test taskiq task consumer processes multiple tasks correctly."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        processor_config = create_taskiq_config(
            redis_address, queue_name="taskiq_multi_task_queue"
        )

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=processor_config)
        class TaskiqMultiConsumer:
            def __init__(self):
                self.messages_received = []

            @task_handler(name="process_request")
            async def process_request(self, data):
                self.messages_received.append(data)

            def get_messages_received(self):
                return self.messages_received

        handle = serve.run(TaskiqMultiConsumer.bind())

        num_tasks = 5
        for i in range(num_tasks):
            send_taskiq_request_to_queue.remote(processor_config, f"task_{i}")

        def check_all_processed():
            messages = handle.get_messages_received.remote().result()
            return len(messages) == num_tasks

        wait_for_condition(check_all_processed, timeout=60)

        messages = handle.get_messages_received.remote().result()
        expected = {f"task_{i}" for i in range(num_tasks)}
        assert set(messages) == expected

    def test_taskiq_task_consumer_with_multiple_handlers(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test taskiq consumer with multiple task handlers in same queue."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        processor_config = create_taskiq_config(
            redis_address, queue_name="taskiq_multi_handler_queue"
        )

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=processor_config)
        class TaskiqMultiHandlerConsumer:
            def __init__(self):
                self.handler1_messages = []
                self.handler2_messages = []

            @task_handler(name="handler_one")
            async def handler_one(self, data):
                self.handler1_messages.append(data)

            @task_handler(name="handler_two")
            async def handler_two(self, data):
                self.handler2_messages.append(data)

            def get_handler1_messages(self):
                return self.handler1_messages

            def get_handler2_messages(self):
                return self.handler2_messages

        handle = serve.run(TaskiqMultiHandlerConsumer.bind())

        send_taskiq_request_to_queue.remote(
            processor_config, "data_for_handler1", task_name="handler_one"
        )
        send_taskiq_request_to_queue.remote(
            processor_config, "data_for_handler2", task_name="handler_two"
        )
        send_taskiq_request_to_queue.remote(
            processor_config, "more_data_handler1", task_name="handler_one"
        )

        def check_messages():
            h1 = handle.get_handler1_messages.remote().result()
            h2 = handle.get_handler2_messages.remote().result()
            return len(h1) == 2 and len(h2) == 1

        wait_for_condition(check_messages, timeout=30)

        h1_messages = handle.get_handler1_messages.remote().result()
        h2_messages = handle.get_handler2_messages.remote().result()

        assert set(h1_messages) == {"data_for_handler1", "more_data_handler1"}
        assert h2_messages == ["data_for_handler2"]

    def test_taskiq_cancel_task_raises_not_implemented(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test that cancel_task raises NotImplementedError for taskiq."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        processor_config = create_taskiq_config(
            redis_address, queue_name="taskiq_cancel_test_queue"
        )

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=processor_config)
        class TaskiqCancelConsumer:
            @task_handler(name="process_request")
            async def process_request(self, data):
                pass

        serve.run(TaskiqCancelConsumer.bind())

        adapter_instance = instantiate_adapter_from_config(
            task_processor_config=processor_config
        )

        with pytest.raises(NotImplementedError, match="not supported by Taskiq"):
            adapter_instance.cancel_task_sync("some_task_id")

    def test_taskiq_task_consumer_persistence_across_restarts(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test that tasks persist in queue and get executed after deployment restart."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        # Use short idle_timeout_ms (5 seconds) so unacked tasks from dead consumer
        # get reclaimed quickly by the new consumer
        processor_config = create_taskiq_config(
            redis_address, queue_name="taskiq_persistence_queue", idle_timeout_ms=5000
        )
        tracker = ProcessedTasksTracker.remote()
        signal1 = SignalActor.remote()

        @serve.deployment(
            num_replicas=1, graceful_shutdown_timeout_s=60, max_ongoing_requests=1
        )
        @task_consumer(task_processor_config=processor_config)
        class TaskiqPersistenceConsumer:
            def __init__(self, tracker_ref, signal_ref):
                self.tracker, self.signal = tracker_ref, signal_ref
                self.local_processed = []

            @task_handler(name="process_request")
            def process_request(self, data):
                ray.get(self.signal.wait.remote())
                self.local_processed.append(data)
                ray.get(self.tracker.add_task.remote(data))
                return f"Processed: {data}"

            def get_local_processed(self):
                return self.local_processed

        # Deploy first version and send tasks
        serve.run(
            TaskiqPersistenceConsumer.bind(tracker, signal1), name="taskiq_app_v1"
        )

        num_tasks = 10
        for i in range(num_tasks):
            ray.get(send_taskiq_request_to_queue.remote(processor_config, f"task_{i}"))

        # Process exactly 1 task, then restart deployment
        wait_for_condition(
            lambda: ray.get(signal1.cur_num_waiters.remote()) == 1, timeout=15
        )
        ray.get(signal1.send.remote(clear=True))  # Allow 1 task to complete
        wait_for_condition(lambda: ray.get(tracker.get_count.remote()) == 1, timeout=15)

        # Shutdown first deployment
        serve.delete("taskiq_app_v1", _blocking=False)
        ray.get(signal1.send.remote(clear=True))  # Release any stuck tasks
        wait_for_condition(
            lambda: "taskiq_app_v1" not in serve.status().applications, timeout=100
        )

        tasks_before_restart = ray.get(tracker.get_count.remote())
        # At least 1 task should have been processed and at least one less than num_tasks
        assert tasks_before_restart >= 1 and tasks_before_restart < num_tasks

        # Deploy second version to process any remaining tasks
        signal2 = SignalActor.remote()
        handle = serve.run(
            TaskiqPersistenceConsumer.bind(tracker, signal2), name="taskiq_app_v2"
        )

        wait_for_condition(
            lambda: ray.get(signal2.cur_num_waiters.remote()) == 1, timeout=15
        )

        ray.get(signal2.send.remote())  # Process all remaining tasks

        def xyz():
            x = ray.get(tracker.get_count.remote())
            print(f"Tasks processed: {x}")
            return x == num_tasks

        wait_for_condition(xyz, timeout=100)
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

    def test_taskiq_failed_task_sent_to_dlq(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test that tasks failing max_retries times are sent to the dead letter queue."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        dlq_name = "taskiq_dlq"

        # Config for the failing consumer with DLQ enabled
        failing_processor_config = create_taskiq_config(
            redis_address,
            queue_name="taskiq_failing_queue",
            failed_task_queue_name=dlq_name,
            max_retries=2,  # Will attempt 3 times total (1 initial + 2 retries)
        )

        # Config for the DLQ consumer
        dlq_processor_config = create_taskiq_config(
            redis_address,
            queue_name=dlq_name,
        )

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=failing_processor_config)
        class FailingTaskConsumer:
            def __init__(self):
                self.attempt_count = 0

            @task_handler(name="process_request")
            def process_request(self, data):
                self.attempt_count += 1
                raise ValueError(f"Task failed intentionally: {data}")

            def get_attempt_count(self):
                return self.attempt_count

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=dlq_processor_config)
        class DLQConsumer:
            def __init__(self):
                self.received_task_id = None
                self.received_task_name = None
                self.received_args = None
                self.received_kwargs = None
                self.received_exception = None
                self.received_traceback = None

            @task_handler(name=dlq_name)
            def handle_failed_task(
                self, task_id, task_name, args, kwargs, exception_msg, traceback_str
            ):
                self.received_task_id = task_id
                self.received_task_name = task_name
                self.received_args = args
                self.received_kwargs = kwargs
                self.received_exception = exception_msg
                self.received_traceback = traceback_str

            def get_received_data(self):
                return {
                    "task_id": self.received_task_id,
                    "task_name": self.received_task_name,
                    "args": self.received_args,
                    "kwargs": self.received_kwargs,
                    "exception": self.received_exception,
                    "traceback": self.received_traceback,
                }

        # Deploy both consumers
        failing_handle = serve.run(
            FailingTaskConsumer.bind(),
            name="failing_consumer",
            route_prefix="/failing",
        )
        dlq_handle = serve.run(
            DLQConsumer.bind(),
            name="dlq_consumer",
            route_prefix="/dlq",
        )

        # Send a task that will fail
        test_data = "dlq_test_data"
        task_id = ray.get(
            send_taskiq_request_to_queue.remote(failing_processor_config, test_data)
        )

        # Wait for all retry attempts to complete (1 initial + 2 retries = 3 attempts)
        # Retries use exponential backoff: 1s, 2s delays, so total ~5s minimum
        def all_retries_exhausted():
            count = failing_handle.get_attempt_count.remote().result()
            return count == 3  # max_retries=2 means 3 total attempts

        wait_for_condition(all_retries_exhausted, timeout=60)

        # Wait for DLQ consumer to receive the failed task
        def dlq_received_task():
            data = dlq_handle.get_received_data.remote().result()
            return data["task_id"] is not None

        wait_for_condition(dlq_received_task, timeout=30)

        # Verify DLQ received correct data
        dlq_data = dlq_handle.get_received_data.remote().result()
        assert (
            dlq_data["task_id"] == task_id
        ), f"Expected task_id {task_id}, got {dlq_data['task_id']}"
        assert (
            dlq_data["task_name"] == "process_request"
        ), f"Expected task_name 'process_request', got {dlq_data['task_name']}"
        assert dlq_data["args"] == [
            test_data
        ], f"Expected args [{test_data}], got {dlq_data['args']}"
        assert (
            dlq_data["kwargs"] == {}
        ), f"Expected empty kwargs, got {dlq_data['kwargs']}"
        assert (
            "Task failed intentionally" in dlq_data["exception"]
        ), f"Expected error message in exception, got {dlq_data['exception']}"
        assert (
            "ValueError" in dlq_data["traceback"]
        ), f"Expected ValueError in traceback, got {dlq_data['traceback']}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
