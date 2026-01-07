"""Integration tests for QueueMonitorActor using real Redis."""
import os
import sys

import pytest
import redis

import ray
from ray.serve._private.queue_monitor import (
    QueueMonitorActor,
    create_queue_monitor_actor,
    get_queue_monitor_actor,
    kill_queue_monitor_actor,
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

        monitor = QueueMonitorActor.remote(redis_broker_url, "test_queue")
        length = ray.get(monitor.get_queue_length.remote())

        assert length == 30

    def test_get_queue_length_empty_queue(
        self, ray_instance, redis_client, redis_broker_url
    ):
        """Test queue length returns 0 for empty queue."""
        monitor = QueueMonitorActor.remote(redis_broker_url, "test_queue")
        length = ray.get(monitor.get_queue_length.remote())

        assert length == 0

    def test_get_config(self, ray_instance, redis_broker_url):
        """Test get_config returns the configuration as a dict."""
        monitor = QueueMonitorActor.remote(redis_broker_url, "test_queue")
        config = ray.get(monitor.get_config.remote())

        assert config["broker_url"] == redis_broker_url
        assert config["queue_name"] == "test_queue"
        assert config["rabbitmq_http_url"] == "http://guest:guest@localhost:15672/api/"


class TestQueueMonitorHelpers:
    """Integration tests for queue monitor helper functions."""

    def test_create_queue_monitor_actor(self, ray_instance, redis_broker_url):
        """Test creating a named queue monitor actor."""
        actor = create_queue_monitor_actor(
            "test_deployment", redis_broker_url, "test_queue"
        )

        # Verify the actor works
        config = ray.get(actor.get_config.remote())
        assert config["queue_name"] == "test_queue"

        # Clean up
        kill_queue_monitor_actor("test_deployment")

    def test_create_queue_monitor_actor_reuses_existing(
        self, ray_instance, redis_broker_url
    ):
        """Test that creating an actor with the same name reuses the existing one."""
        actor1 = create_queue_monitor_actor(
            "test_deployment", redis_broker_url, "test_queue"
        )
        actor2 = create_queue_monitor_actor(
            "test_deployment", redis_broker_url, "test_queue"
        )

        # Both should reference the same actor
        assert actor1 == actor2

        # Clean up
        kill_queue_monitor_actor("test_deployment")

    def test_get_queue_monitor_actor(self, ray_instance, redis_broker_url):
        """Test retrieving an existing queue monitor actor."""
        actor = create_queue_monitor_actor(
            "test_deployment", redis_broker_url, "test_queue"
        )
        # Ensure actor is ready before looking it up by name
        ray.get(actor.get_config.remote())

        retrieved_actor = get_queue_monitor_actor("test_deployment")
        config = ray.get(retrieved_actor.get_config.remote())
        assert config["queue_name"] == "test_queue"

        # Clean up
        kill_queue_monitor_actor("test_deployment")

    def test_get_queue_monitor_actor_not_found(self, ray_instance):
        """Test retrieving a non-existent actor raises ValueError."""
        with pytest.raises(ValueError):
            get_queue_monitor_actor("nonexistent_deployment")

    def test_kill_queue_monitor_actor(self, ray_instance, redis_broker_url):
        """Test killing a queue monitor actor."""
        actor = create_queue_monitor_actor(
            "test_deployment", redis_broker_url, "test_queue"
        )
        # Ensure actor is ready before trying to kill it
        ray.get(actor.get_config.remote())

        kill_queue_monitor_actor("test_deployment")

        # Should not be able to get the actor anymore
        with pytest.raises(ValueError):
            get_queue_monitor_actor("test_deployment")

    def test_kill_queue_monitor_actor_not_found(self, ray_instance):
        """Test killing a non-existent actor raises ValueError."""
        with pytest.raises(ValueError):
            kill_queue_monitor_actor("nonexistent_deployment")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
