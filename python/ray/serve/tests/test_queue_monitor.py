"""Integration tests for QueueMonitorActor using real Redis."""
import os
import sys

import pytest
import redis

import ray
from ray.serve._private.queue_monitor import (
    create_queue_monitor_actor,
)
from ray.tests.conftest import external_redis  # noqa: F401


@pytest.fixture
def redis_client(external_redis):  # noqa: F811
    """Create a Redis client connected to the external Redis."""
    redis_address = os.environ.get("RAY_REDIS_ADDRESS")
    host, port = redis_address.split(":")
    client = redis.Redis(host=host, port=int(port), db=0)
    yield client
    # Cleanup: delete test queue after each test
    client.delete("test_queue")
    client.close()


@pytest.fixture
def redis_broker_url(external_redis):  # noqa: F811
    """Get the Redis broker URL for the external Redis."""
    redis_address = os.environ.get("RAY_REDIS_ADDRESS")
    return f"redis://{redis_address}/0"


class TestQueueMonitorActor:
    """Integration tests for QueueMonitorActor with real Redis."""

    def test_get_queue_length(self, ray_instance, redis_client, redis_broker_url):
        """Test queue length returns number of messages from broker."""
        # Push some messages to the queue
        for i in range(30):
            redis_client.lpush("test_queue", f"message_{i}")

        monitor = create_queue_monitor_actor(
            "test_deployment", redis_broker_url, "test_queue"
        )
        length = ray.get(monitor.get_queue_length.remote())

        assert length == 30

    def test_get_queue_length_empty_queue(
        self, ray_instance, redis_client, redis_broker_url
    ):
        """Test queue length returns 0 for empty queue."""
        monitor = create_queue_monitor_actor(
            "test_deployment", redis_broker_url, "test_queue"
        )
        length = ray.get(monitor.get_queue_length.remote())

        assert length == 0

    def test_get_config(self, ray_instance, redis_broker_url):
        """Test get_config returns the configuration as a dict."""
        monitor = create_queue_monitor_actor(
            "test_deployment", redis_broker_url, "test_queue"
        )
        config = ray.get(monitor.get_config.remote())

        assert config["broker_url"] == redis_broker_url
        assert config["queue_name"] == "test_queue"
        assert config["rabbitmq_http_url"] == "http://guest:guest@localhost:15672/api/"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
