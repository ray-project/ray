"""Integration tests for QueueMonitorActor using real Redis."""
import os
import sys
import time

import pytest
import redis

import ray
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID
from ray.serve._private.queue_monitor import (
    create_queue_monitor_actor,
    kill_queue_monitor_actor,
)
from ray.tests.conftest import external_redis  # noqa: F401

# Use short push interval for tests (0.5 seconds)
os.environ["RAY_SERVE_ASYNC_INFERENCE_TASK_QUEUE_METRIC_PUSH_INTERVAL_S"] = "0.5"


@ray.remote
class MockController:
    """Mock controller that accepts metrics push calls."""

    def record_autoscaling_metrics_from_async_inference_task_queue(self, report):
        pass


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


@pytest.fixture
def mock_controller(ray_instance):
    """Create a mock controller for testing."""
    return MockController.remote()


class TestQueueMonitorActor:
    """Integration tests for QueueMonitorActor with real Redis."""

    def test_queue_length_refresh(
        self, ray_instance, redis_client, redis_broker_url, mock_controller
    ):
        """Test QueueMonitor correctly fetches and caches queue length from broker."""
        # Push messages to the queue
        for i in range(30):
            redis_client.lpush("test_queue", f"message_{i}")

        deployment_id = DeploymentID("test_deployment", "test_app")
        monitor = create_queue_monitor_actor(
            deployment_id,
            redis_broker_url,
            "test_queue",
            controller_handle=mock_controller,
        )

        # Wait for metrics pusher to refresh the cache
        def check_cached_length():
            length = ray.get(monitor.get_cached_queue_length.remote())
            return length == 30

        wait_for_condition(check_cached_length, timeout=5)

        # Cleanup
        kill_queue_monitor_actor(deployment_id.name)

    def test_queue_length_empty_queue(
        self, ray_instance, redis_client, redis_broker_url, mock_controller
    ):
        """Test QueueMonitor returns 0 for empty queue."""
        deployment_id = DeploymentID("test_deployment", "test_app")
        monitor = create_queue_monitor_actor(
            deployment_id,
            redis_broker_url,
            "test_queue",
            controller_handle=mock_controller,
        )

        # Wait for metrics pusher to refresh (queue is empty)
        time.sleep(1.0)

        length = ray.get(monitor.get_cached_queue_length.remote())
        assert length == 0

        # Cleanup
        kill_queue_monitor_actor(deployment_id.name)

    def test_queue_length_updates_on_change(
        self, ray_instance, redis_client, redis_broker_url, mock_controller
    ):
        """Test QueueMonitor updates cached length when queue changes."""
        deployment_id = DeploymentID("test_deployment", "test_app")

        # Start with 10 messages
        for i in range(10):
            redis_client.lpush("test_queue", f"message_{i}")

        monitor = create_queue_monitor_actor(
            deployment_id,
            redis_broker_url,
            "test_queue",
            controller_handle=mock_controller,
        )

        # Wait for initial cache update
        def check_initial_length():
            length = ray.get(monitor.get_cached_queue_length.remote())
            return length == 10

        wait_for_condition(check_initial_length, timeout=5)

        # Add more messages
        for i in range(10, 25):
            redis_client.lpush("test_queue", f"message_{i}")

        # Wait for cache to update
        def check_updated_length():
            length = ray.get(monitor.get_cached_queue_length.remote())
            return length == 25

        wait_for_condition(check_updated_length, timeout=5)

        # Cleanup
        kill_queue_monitor_actor(deployment_id.name)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
