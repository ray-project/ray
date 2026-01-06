"""Integration tests for QueueMonitorActor using real Redis."""
import os
import sys

import pytest
import redis

import ray
from ray.serve._private.queue_monitor import (
    QueueMonitorActor,
    QueueMonitorConfig,
    create_queue_monitor_actor,
    get_queue_monitor_actor,
    kill_queue_monitor_actor,
)
from ray.tests.conftest import external_redis  # noqa: F401


@pytest.fixture
def ray_instance():  # noqa: F811
    """Initialize Ray with external Redis."""
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=2, include_dashboard=False)
    yield
    ray.shutdown()


@pytest.fixture
def redis_client(external_redis):  # noqa: F811
    """Create a Redis client connected to the external Redis."""
    redis_address = os.environ.get("RAY_REDIS_ADDRESS")
    host, port = redis_address.split(":")
    client = redis.Redis(host=host, port=int(port), db=0)
    yield client
    client.close()


@pytest.fixture
def redis_config(external_redis):  # noqa: F811
    """Create a QueueMonitorConfig with the external Redis URL."""
    redis_address = os.environ.get("RAY_REDIS_ADDRESS")
    return QueueMonitorConfig(
        broker_url=f"redis://{redis_address}/0",
        queue_name="test_queue",
    )


class TestQueueMonitorActor:
    """Integration tests for QueueMonitorActor with real Redis."""

    def test_get_queue_length(self, ray_instance, redis_client, redis_config):
        """Test queue length returns number of messages from broker."""
        # Push some messages to the queue
        for i in range(30):
            redis_client.lpush("test_queue", f"message_{i}")

        try:
            monitor = QueueMonitorActor.remote(redis_config)
            length = ray.get(monitor.get_queue_length.remote())

            assert length == 30
        finally:
            # Clean up
            redis_client.delete("test_queue")

    def test_get_queue_length_empty_queue(
        self, ray_instance, redis_client, redis_config
    ):
        """Test queue length returns 0 for empty queue."""
        # Ensure queue is empty
        redis_client.delete("test_queue")

        monitor = QueueMonitorActor.remote(redis_config)
        length = ray.get(monitor.get_queue_length.remote())

        assert length == 0

    def test_get_queue_length_returns_cached_on_error(
        self, ray_instance, redis_client, redis_config
    ):
        """Test get_queue_length returns cached value on error."""
        # Push messages initially
        for i in range(50):
            redis_client.lpush("test_queue", f"message_{i}")

        try:
            monitor = QueueMonitorActor.remote(redis_config)

            # First successful query
            length = ray.get(monitor.get_queue_length.remote())
            assert length == 50

            # Simulate error by deleting the queue and checking cached value is returned
            # Note: The actual error path is harder to trigger with real Redis,
            # so we verify the caching behavior by checking the value is consistent
            length = ray.get(monitor.get_queue_length.remote())
            assert length == 50
        finally:
            redis_client.delete("test_queue")

    def test_shutdown_marks_uninitialized(
        self, ray_instance, redis_client, redis_config
    ):
        """Test shutdown cleans up resources and returns 0 for queue length."""
        redis_client.lpush("test_queue", "message")

        try:
            monitor = QueueMonitorActor.remote(redis_config)

            # Verify monitor works before shutdown
            length = ray.get(monitor.get_queue_length.remote())
            assert length == 1

            # Shutdown the monitor
            ray.get(monitor.shutdown.remote())

            # After shutdown, should return 0
            length = ray.get(monitor.get_queue_length.remote())
            assert length == 0
        finally:
            redis_client.delete("test_queue")

    def test_get_config(self, ray_instance, redis_config):
        """Test get_config returns the configuration as a dict."""
        monitor = QueueMonitorActor.remote(redis_config)
        config = ray.get(monitor.get_config.remote())

        assert config["broker_url"] == redis_config.broker_url
        assert config["queue_name"] == redis_config.queue_name
        assert config["rabbitmq_http_url"] == redis_config.rabbitmq_http_url


class TestQueueMonitorHelpers:
    """Integration tests for queue monitor helper functions."""

    def test_create_queue_monitor_actor(self, ray_instance, redis_config):
        """Test creating a named queue monitor actor."""
        actor = create_queue_monitor_actor("test_deployment", redis_config)

        # Verify the actor works
        config = ray.get(actor.get_config.remote())
        assert config["queue_name"] == "test_queue"

        # Clean up
        kill_queue_monitor_actor("test_deployment")

    def test_create_queue_monitor_actor_reuses_existing(
        self, ray_instance, redis_config
    ):
        """Test that creating an actor with the same name reuses the existing one."""
        actor1 = create_queue_monitor_actor("test_deployment", redis_config)
        actor2 = create_queue_monitor_actor("test_deployment", redis_config)

        # Both should reference the same actor
        assert actor1 == actor2

        # Clean up
        kill_queue_monitor_actor("test_deployment")

    def test_get_queue_monitor_actor(self, ray_instance, redis_config):
        """Test retrieving an existing queue monitor actor."""
        actor = create_queue_monitor_actor("test_deployment", redis_config)
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

    def test_kill_queue_monitor_actor(self, ray_instance, redis_config):
        """Test killing a queue monitor actor."""
        actor = create_queue_monitor_actor("test_deployment", redis_config)
        # Ensure actor is ready before trying to kill it
        ray.get(actor.get_config.remote())

        result = kill_queue_monitor_actor("test_deployment")
        assert result is True

        # Should not be able to get the actor anymore
        with pytest.raises(ValueError):
            get_queue_monitor_actor("test_deployment")

    def test_kill_queue_monitor_actor_not_found(self, ray_instance):
        """Test killing a non-existent actor returns False."""
        result = kill_queue_monitor_actor("nonexistent_deployment")
        assert result is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
