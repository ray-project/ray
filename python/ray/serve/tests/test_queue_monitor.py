import sys
from types import SimpleNamespace

import pytest

import ray
from ray.serve._private.queue_monitor import (
    QueueMonitorActor,
    QueueMonitorConfig,
)


@pytest.fixture(autouse=True)
def ray_local_mode():
    """Run these tests in Ray local mode so driver-side mocks apply to actors."""
    if ray.is_initialized():
        ray.shutdown()
    ray.init(local_mode=True, num_cpus=2, include_dashboard=False)
    yield
    ray.shutdown()


class StubBroker:
    """Picklable stub for flower.Broker used by QueueMonitorActor."""

    init_calls = []
    queues_responses = []
    queues_side_effects = []
    close_calls = 0

    def __init__(self, broker_url: str, rabbitmq_http_url: str):
        type(self).init_calls.append((broker_url, rabbitmq_http_url))

    def queues(self, names):
        if type(self).queues_side_effects:
            exc = type(self).queues_side_effects.pop(0)
            if exc is not None:
                raise exc
        if type(self).queues_responses:
            return type(self).queues_responses.pop(0)
        return None

    def close(self):
        type(self).close_calls += 1


@pytest.fixture(autouse=True)
def stub_flower(monkeypatch):
    """Patch queue_monitor.flower with a picklable stub (avoids cloudpickle/MagicMock issues)."""
    import ray.serve._private.queue_monitor as qm

    StubBroker.init_calls = []
    StubBroker.queues_responses = []
    StubBroker.queues_side_effects = []
    StubBroker.close_calls = 0

    monkeypatch.setattr(qm, "flower", SimpleNamespace(Broker=StubBroker))
    yield StubBroker


class TestQueueMonitorConfig:
    """Tests for QueueMonitorConfig class."""

    def test_config_stores_values(self):
        """Test config stores broker_url, queue_name, and rabbitmq_http_url."""
        config = QueueMonitorConfig(
            broker_url="redis://localhost:6379/0",
            queue_name="test_queue",
        )

        assert config.broker_url == "redis://localhost:6379/0"
        assert config.queue_name == "test_queue"
        assert config.rabbitmq_http_url == "http://guest:guest@localhost:15672/api/"

    def test_config_custom_rabbitmq_http_url(self):
        config = QueueMonitorConfig(
            broker_url="amqp://guest:guest@localhost:5672//",
            queue_name="my_queue",
            rabbitmq_http_url="http://user:pass@localhost:15672/api/",
        )

        assert config.broker_url == "amqp://guest:guest@localhost:5672//"
        assert config.queue_name == "my_queue"
        assert config.rabbitmq_http_url == "http://user:pass@localhost:15672/api/"


class TestQueueMonitor:
    """Tests for QueueMonitor class."""

    @pytest.fixture
    def redis_config(self):
        return QueueMonitorConfig(
            broker_url="redis://localhost:6379/0",
            queue_name="test_queue",
        )

    def test_get_queue_length(self, stub_flower, redis_config):
        """Test queue length returns number of messages from Flower broker."""
        stub_flower.queues_responses = [[{"name": "test_queue", "messages": 30}]]

        monitor = QueueMonitorActor.remote(redis_config)
        length = ray.get(monitor.get_queue_length.remote())

        assert length == 30

    def test_get_queue_length_returns_cached_on_error(self, stub_flower, redis_config):
        """Test get_queue_length returns cached value on error."""
        stub_flower.queues_side_effects = [None, Exception("Connection lost")]

        stub_flower.queues_responses = [[{"name": "test_queue", "messages": 50}]]

        monitor = QueueMonitorActor.remote(redis_config)

        # First successful query
        length = ray.get(monitor.get_queue_length.remote())
        assert length == 50

        # Should return cached value
        length = ray.get(monitor.get_queue_length.remote())
        assert length == 50

    def test_shutdown_marks_uninitialized(self, stub_flower, redis_config):
        monitor = QueueMonitorActor.remote(redis_config)
        ray.get(monitor.shutdown.remote())

        assert ray.get(monitor.get_queue_length.remote()) == 0
        assert stub_flower.close_calls == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
