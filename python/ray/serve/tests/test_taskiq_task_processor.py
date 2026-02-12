import sys

import pytest

from ray.serve.taskiq_task_processor import (
    _create_broker,
    _import_broker_class,
)


class TestBrokerFactory:
    """Tests for broker creation helpers."""

    def test_create_redis_broker(self):
        broker = _create_broker(
            broker_type="redis_stream",
            queue_name="test_queue",
            broker_kwargs={"url": "redis://localhost:6379"},
        )
        assert broker is not None

    def test_unsupported_broker_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported broker_type"):
            _import_broker_class("unsupported_broker")

    def test_missing_required_kwargs_raises(self):
        with pytest.raises(ValueError, match="requires the following keys"):
            _create_broker(
                broker_type="redis_stream",
                queue_name="test_queue",
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
