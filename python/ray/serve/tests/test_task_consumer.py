import sys
import uuid
from unittest.mock import MagicMock, call

import pytest

from ray.serve.schema import MockTaskProcessorConfig, TaskProcessorConfig
from ray.serve.task_consumer import task_consumer, task_handler


@pytest.fixture
def config():
    """Provides a mock TaskProcessorConfig."""
    queue_name = f"test_queue_{uuid.uuid4().hex}"
    return TaskProcessorConfig(
        queue_name=queue_name, adapter_config=MockTaskProcessorConfig()
    )


class TestTaskHandlerDecorator:
    """Test the task_handler decorator."""

    def test_task_handler_decorator_with_name():
        mock = MagicMock()

        @task_handler(name="my_task")
        def my_task_handler():
            mock()

        my_task_handler()

        assert mock.call_count == 1
        assert my_task_handler._task_name == "my_task"

    def test_task_handler_decorator_without_name():
        mock = MagicMock()

        @task_handler
        def my_task_handler():
            mock()

        my_task_handler()

        assert mock.call_count == 1
        assert my_task_handler._task_name == "my_task_handler"

    def test_task_handler_decorator_invalid_name():
        with pytest.raises(
            ValueError, match="Task name must be a non-empty string when provided"
        ):

            @task_handler(name="")
            def my_task_handler():
                pass

        with pytest.raises(
            ValueError, match="Task name must be a non-empty string when provided"
        ):

            @task_handler(name="   ")
            def my_task_handler_with_spaces():
                pass

        with pytest.raises(
            ValueError, match="Task name must be a non-empty string when provided"
        ):

            @task_handler(name=123)
            def my_task_handler_with_invalid_type():
                pass

    def test_task_handler_on_callable_object_without_name_attr():
        """Test that AttributeError is raised for callables with no __name__."""

        class MyCallable:
            """A simple callable class without a __name__ attribute on instances."""

            def __call__(self):
                pass

        with pytest.raises(AttributeError):
            # When task_handler is used without a name on a callable object
            # that doesn't have a __name__, it should raise an AttributeError.
            task_handler(MyCallable())


class TestTaskConsumerDecorator:
    """Test the task_consumer decorator."""

    def test_task_consumer_basic(config):
        """Test basic functionality of the task_consumer decorator."""

        @task_consumer(task_processor_config=config)
        class MyConsumer:
            @task_handler
            def my_task(self):
                pass

        consumer_instance = MyConsumer()
        adapter = consumer_instance._adapter

        assert adapter._start_consumer_received
        adapter.register_task_handle_mock.assert_called_once_with(
            consumer_instance.my_task, name="my_task"
        )

        del consumer_instance

    def test_task_consumer_multiple_handlers(config):
        """Test with multiple task handlers."""

        @task_consumer(task_processor_config=config)
        class MyConsumer:
            @task_handler
            def task1(self):
                pass

            @task_handler
            def task2(self):
                pass

        consumer_instance = MyConsumer()
        adapter = consumer_instance._adapter

        assert adapter._start_consumer_received
        adapter.register_task_handle_mock.assert_has_calls(
            [
                call(consumer_instance.task1, name="task1"),
                call(consumer_instance.task2, name="task2"),
            ],
            any_order=True,
        )
        assert adapter.register_task_handle_mock.call_count == 2

        del consumer_instance

    def test_task_consumer_custom_names(config):
        """Test task handlers with and without custom names."""

        @task_consumer(task_processor_config=config)
        class MyConsumer:
            @task_handler(name="custom_task")
            def task1(self):
                pass

            @task_handler
            def task2(self):
                pass

        consumer_instance = MyConsumer()
        adapter = consumer_instance._adapter

        assert adapter._start_consumer_received
        adapter.register_task_handle_mock.assert_has_calls(
            [
                call(consumer_instance.task1, name="custom_task"),
                call(consumer_instance.task2, name="task2"),
            ],
            any_order=True,
        )
        assert adapter.register_task_handle_mock.call_count == 2

        del consumer_instance

    def test_task_consumer_init_args(config):
        """Test that __init__ arguments are passed correctly."""

        @task_consumer(task_processor_config=config)
        class MyConsumer:
            def __init__(self, value):
                self.value = value

        consumer_instance = MyConsumer(value=42)
        assert consumer_instance.value == 42
        assert consumer_instance._adapter is not None

        del consumer_instance

    def test_task_consumer_no_handlers(config):
        """Test with a class that has no task handlers."""

        @task_consumer(task_processor_config=config)
        class MyConsumer:
            def some_method(self):
                pass

        consumer_instance = MyConsumer()
        adapter = consumer_instance._adapter

        assert adapter._start_consumer_received
        adapter.register_task_handle_mock.assert_not_called()

        del consumer_instance

    def test_task_consumer_inheritance(config):
        """Test that inherited task handlers are registered."""

        class BaseConsumer:
            @task_handler
            def base_task(self):
                pass

        @task_consumer(task_processor_config=config)
        class DerivedConsumer(BaseConsumer):
            @task_handler
            def derived_task(self):
                pass

        consumer_instance = DerivedConsumer()
        adapter = consumer_instance._adapter

        assert adapter._start_consumer_received
        adapter.register_task_handle_mock.assert_has_calls(
            [
                call(consumer_instance.base_task, name="base_task"),
                call(consumer_instance.derived_task, name="derived_task"),
            ],
            any_order=True,
        )
        assert adapter.register_task_handle_mock.call_count == 2

        del consumer_instance

    def test_task_consumer_no_args_decorator():
        """Test using @task_consumer without arguments raises TypeError."""
        with pytest.raises(TypeError):

            @task_consumer
            class MyConsumer:
                pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
