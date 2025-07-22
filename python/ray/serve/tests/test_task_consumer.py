import sys

import pytest

from ray.serve.task_consumer import task_handler


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
