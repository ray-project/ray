import os
import sys
import uuid

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve.schema import TaskProcessorConfig
from ray.serve.task_consumer import (
    instantiate_adapter_from_config,
    task_consumer,
    task_handler,
)
from ray.serve.taskiq_task_processor import TaskiqAdapterConfig
from ray.tests.conftest import external_redis  # noqa: F401


@ray.remote
def send_taskiq_request(
    processor_config: TaskProcessorConfig, data, task_name="process_request"
):
    """Enqueue a task from a separate Ray process (mimics a client)."""
    adapter = instantiate_adapter_from_config(task_processor_config=processor_config)
    result = adapter.enqueue_task_sync(task_name, args=[data])
    assert result.id is not None
    return result.id


@pytest.fixture
def taskiq_processor_config(external_redis):  # noqa: F811
    """Create a TaskProcessorConfig backed by a real Redis stream broker."""
    redis_url = f"redis://{os.environ['RAY_REDIS_ADDRESS']}"
    return TaskProcessorConfig(
        queue_name=f"test-queue-{uuid.uuid4().hex[:8]}",
        adapter="ray.serve.taskiq_task_processor.TaskiqTaskProcessorAdapter",
        adapter_config=TaskiqAdapterConfig(
            broker_type="redis_stream",
            broker_kwargs={"url": redis_url},
            result_backend_url=redis_url,
        ),
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
class TestTaskiqTaskProcessor:
    """E2E tests for the Taskiq task processor adapter (no Ray Serve)."""

    def test_full_task_lifecycle(self, taskiq_processor_config):
        """Enqueue a task, consume it, verify execution."""
        adapter = instantiate_adapter_from_config(taskiq_processor_config)

        results = []

        def process_request(data):
            results.append(data)

        adapter.register_task_handle(process_request, name="process_request")
        adapter.start_consumer()

        adapter.enqueue_task_sync("process_request", args=["hello"])

        wait_for_condition(lambda: len(results) == 1, timeout=10)
        assert results[0] == "hello"

        adapter.stop_consumer()

    def test_task_status_with_result_backend(self, taskiq_processor_config):
        """Verify task status transitions from PENDING to SUCCESS."""
        adapter = instantiate_adapter_from_config(taskiq_processor_config)

        def add(a, b):
            return a + b

        adapter.register_task_handle(add, name="add")
        adapter.start_consumer()

        result = adapter.enqueue_task_sync("add", args=[2, 3])
        assert result.status == "PENDING"

        def check_success():
            status = adapter.get_task_status_sync(result.id)
            return status.status == "SUCCESS" and status.result == 5

        wait_for_condition(check_success, timeout=10)

        adapter.stop_consumer()

    def test_failed_task_returns_error_status(self, taskiq_processor_config):
        """Verify a failing task transitions to ERROR status."""
        adapter = instantiate_adapter_from_config(taskiq_processor_config)

        def failing_task():
            raise ValueError("intentional failure")

        adapter.register_task_handle(failing_task, name="failing_task")
        adapter.start_consumer()

        result = adapter.enqueue_task_sync("failing_task")

        def check_error():
            status = adapter.get_task_status_sync(result.id)
            return status.status == "ERROR" and "intentional failure" in status.result

        wait_for_condition(check_error, timeout=10)

        adapter.stop_consumer()


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
class TestTaskiqWithRayServe:
    """E2E tests for the Taskiq adapter deployed as a Ray Serve task consumer."""

    def test_task_consumer_as_serve_deployment(
        self, serve_instance, taskiq_processor_config
    ):
        """Test that a taskiq-backed task consumer works as a Serve deployment."""
        processor_config = taskiq_processor_config

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=processor_config)
        class TaskiqConsumer:
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

        handle = serve.run(TaskiqConsumer.bind())
        send_taskiq_request.remote(processor_config, "test_data_1")

        def assert_result():
            try:
                handle.assert_task_received.remote().result()
                return True
            except Exception:
                return False

        wait_for_condition(assert_result, timeout=30)

    def test_multiple_task_handlers(self, serve_instance, taskiq_processor_config):
        """Test multiple @task_handler methods in a single deployment."""
        processor_config = taskiq_processor_config

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=processor_config)
        class TaskiqConsumer:
            def __init__(self):
                self.greetings = []
                self.sums = []

            @task_handler(name="greet")
            def greet(self, name):
                self.greetings.append(name)

            @task_handler(name="double")
            def double(self, n):
                self.sums.append(n * 2)

            def get_greetings(self):
                return self.greetings

            def get_sums(self):
                return self.sums

        handle = serve.run(TaskiqConsumer.bind())
        send_taskiq_request.remote(processor_config, "alice", task_name="greet")
        send_taskiq_request.remote(processor_config, "bob", task_name="greet")
        send_taskiq_request.remote(processor_config, 5, task_name="double")

        def assert_result():
            try:
                greetings = handle.get_greetings.remote().result()
                sums = handle.get_sums.remote().result()
                return set(greetings) == {"alice", "bob"} and sums == [10]
            except Exception:
                return False

        wait_for_condition(assert_result, timeout=30)

    def test_async_task_handler(self, serve_instance, taskiq_processor_config):
        """Test that 30 async tasks are all consumed by the deployment."""
        processor_config = taskiq_processor_config
        num_tasks = 30

        @serve.deployment(max_ongoing_requests=1)
        @task_consumer(task_processor_config=processor_config)
        class TaskiqConsumer:
            def __init__(self):
                self.received = []

            @task_handler(name="async_process")
            async def async_process(self, data):
                self.received.append(data)

            def get_received(self):
                return self.received

        handle = serve.run(TaskiqConsumer.bind())

        # Enqueue all tasks from a single adapter to avoid spawning 30 Ray
        # worker processes (each send_taskiq_request.remote creates one).
        enqueue_adapter = instantiate_adapter_from_config(processor_config)
        for i in range(num_tasks):
            enqueue_adapter.enqueue_task_sync("async_process", args=[f"msg-{i}"])

        def assert_result():
            try:
                received = handle.get_received.remote().result()
                return len(received) == num_tasks
            except Exception:
                return False

        wait_for_condition(assert_result, timeout=30)

        received = handle.get_received.remote().result()
        assert set(received) == {f"msg-{i}" for i in range(num_tasks)}


if __name__ == "__main__":
    pytest.main(["-v", __file__])
