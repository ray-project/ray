import sys
import uuid
from typing import Any, Dict, List
from unittest.mock import MagicMock, call

import pytest

from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig, TaskResult
from ray.serve.task_consumer import task_consumer, task_handler
from ray.serve.task_processor import TaskProcessorAdapter


class MockTaskProcessorAdapter(TaskProcessorAdapter):
    """Mock adapter for testing task processor functionality."""

    _start_consumer_received: bool = False
    _stop_consumer_received: bool = False
    _shutdown_received: bool = False

    def __init__(self, config: TaskProcessorConfig):
        self._config = config
        self.register_task_handle_mock = MagicMock()

    def initialize(self):
        pass

    def register_task_handle(self, func, name=None):
        self.register_task_handle_mock(func, name=name)

    def enqueue_task_sync(
        self, task_name, args=None, kwargs=None, **options
    ) -> TaskResult:
        pass

    def get_task_status_sync(self, task_id) -> TaskResult:
        pass

    def start_consumer(self, **kwargs):
        self._start_consumer_received = True

    def stop_consumer(self, timeout: float = 10.0):
        self._stop_consumer_received = True

    def shutdown(self):
        self._shutdown_received = True

    def cancel_task_sync(self, task_id) -> bool:
        pass

    def get_metrics_sync(self) -> Dict[str, Any]:
        pass

    def health_check_sync(self) -> List[Dict]:
        pass


@pytest.fixture
def config():
    """Provides a mock TaskProcessorConfig."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    return TaskProcessorConfig(
        queue_name=queue_name,
        adapter_config=CeleryAdapterConfig(
            broker_url="fake://",
            backend_url="fake://",
        ),
        adapter=MockTaskProcessorAdapter,
    )


class TestTaskHandlerDecorator:
    """Test the task_handler decorator."""

    def _create_and_test_handler(self, decorator_args=None, expected_name=None):
        """Helper to create and test a task handler."""
        mock = MagicMock()

        if decorator_args is None:

            @task_handler
            def test_handler():
                mock()

        else:

            @task_handler(**decorator_args)
            def test_handler():
                mock()

        test_handler()

        assert mock.call_count == 1
        assert test_handler._task_name == expected_name

    def test_task_handler_decorator_with_name(self):
        self._create_and_test_handler(
            decorator_args={"name": "my_task"}, expected_name="my_task"
        )

    def test_task_handler_decorator_without_name(self):
        self._create_and_test_handler(expected_name="test_handler")

    @pytest.mark.parametrize("invalid_name", ["", "   ", 123])
    def test_task_handler_decorator_invalid_name(self, invalid_name):
        """Test various invalid task names."""
        with pytest.raises(
            ValueError,
            match=f"Task name must be a non-empty string, got {invalid_name}",
        ):

            @task_handler(name=invalid_name)
            def my_task_handler():
                pass

    def test_task_handler_on_callable_object_without_name_attr(self):
        """Test that AttributeError is raised for callables with no __name__."""

        class MyCallable:
            """A simple callable class without a __name__ attribute on instances."""

            def __call__(self):
                pass

        with pytest.raises(AttributeError):
            task_handler(MyCallable())


class TestTaskConsumerDecorator:
    """Test the task_consumer decorator."""

    def _verify_and_cleanup(self, instance, expected_calls=None):
        """Verify consumer and cleanup instance."""
        adapter = instance._adapter
        assert adapter._start_consumer_received

        if expected_calls is not None:
            if expected_calls:
                calls = [call(method, name=name) for method, name in expected_calls]
                adapter.register_task_handle_mock.assert_has_calls(
                    calls, any_order=False
                )
                assert adapter.register_task_handle_mock.call_count == len(
                    expected_calls
                )
            else:
                adapter.register_task_handle_mock.assert_not_called()

        del instance

    def _run_consumer_test(
        self, config, consumer_class_factory, expected_calls_factory=None
    ):
        """Run a consumer test with factory functions."""
        consumer_class = consumer_class_factory(config)
        instance = consumer_class()

        expected_calls = (
            expected_calls_factory(instance) if expected_calls_factory else None
        )

        self._verify_and_cleanup(instance, expected_calls)

    def test_task_consumer_basic(self, config):
        """Test basic functionality of the task_consumer decorator."""

        def make_consumer(cfg):
            @task_consumer(task_processor_config=cfg)
            class MyConsumer:
                @task_handler
                def my_task(self):
                    pass

            return MyConsumer

        self._run_consumer_test(
            config, make_consumer, lambda inst: [(inst.my_task, "my_task")]
        )

    def test_task_consumer_multiple_handlers(self, config):
        """Test with multiple task handlers."""

        def make_consumer(cfg):
            @task_consumer(task_processor_config=cfg)
            class MyConsumer:
                @task_handler
                def task1(self):
                    pass

                @task_handler
                def task2(self):
                    pass

            return MyConsumer

        self._run_consumer_test(
            config,
            make_consumer,
            lambda inst: [(inst.task1, "task1"), (inst.task2, "task2")],
        )

    def test_task_consumer_custom_names(self, config):
        """Test task handlers with and without custom names."""

        def make_consumer(cfg):
            @task_consumer(task_processor_config=cfg)
            class MyConsumer:
                @task_handler(name="custom_task")
                def task1(self):
                    pass

                @task_handler
                def task2(self):
                    pass

            return MyConsumer

        self._run_consumer_test(
            config,
            make_consumer,
            lambda inst: [(inst.task1, "custom_task"), (inst.task2, "task2")],
        )

    def test_task_consumer_init_args(self, config):
        """Test that __init__ arguments are passed correctly."""

        @task_consumer(task_processor_config=config)
        class MyConsumer:
            def __init__(self, value):
                self.value = value

        instance = MyConsumer(value=42)
        assert instance.value == 42
        self._verify_and_cleanup(instance)

    def test_task_consumer_no_handlers(self, config):
        """Test with a class that has no task handlers."""

        def make_consumer(cfg):
            @task_consumer(task_processor_config=cfg)
            class MyConsumer:
                def some_method(self):
                    pass

            return MyConsumer

        self._run_consumer_test(config, make_consumer, lambda inst: [])

    def test_task_consumer_inheritance(self, config):
        """Test that inherited task handlers are registered."""

        def make_consumer(cfg):
            class BaseConsumer:
                @task_handler
                def base_task(self):
                    pass

            @task_consumer(task_processor_config=cfg)
            class DerivedConsumer(BaseConsumer):
                @task_handler
                def derived_task(self):
                    pass

            return DerivedConsumer

        self._run_consumer_test(
            config,
            make_consumer,
            lambda inst: [
                (inst.base_task, "base_task"),
                (inst.derived_task, "derived_task"),
            ],
        )

    def test_task_consumer_no_args_decorator(self):
        """Test using @task_consumer without arguments raises TypeError."""
        with pytest.raises(TypeError):

            @task_consumer
            class MyConsumer:
                pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
