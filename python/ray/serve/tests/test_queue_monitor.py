import os
import sys

import pytest
import redis

import ray
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_CONTROLLER_NAME, SERVE_NAMESPACE
from ray.serve._private.queue_monitor import (
    create_queue_monitor_actor,
    kill_queue_monitor_actor,
)
from ray.tests.conftest import external_redis  # noqa: F401

TEST_DEPLOYMENT_ID = DeploymentID("test_deployment", "test_app")
TEST_QUEUE_NAME = "test_queue"


@pytest.fixture
def redis_client(external_redis):  # noqa: F811
    """Create a Redis client connected to the external Redis."""
    redis_address = os.environ.get("RAY_REDIS_ADDRESS")
    host, port = redis_address.split(":")
    client = redis.Redis(host=host, port=int(port), db=0)
    yield client
    client.delete(TEST_QUEUE_NAME)
    client.close()


@pytest.fixture
def redis_broker_url(external_redis):  # noqa: F811
    """Get the Redis broker URL for the external Redis."""
    redis_address = os.environ.get("RAY_REDIS_ADDRESS")
    return f"redis://{redis_address}/0"


@pytest.fixture
def queue_monitor(serve_instance, redis_broker_url):  # noqa: F811
    """Create a QueueMonitor with the real Serve controller."""
    controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
    monitor = create_queue_monitor_actor(
        deployment_id=TEST_DEPLOYMENT_ID,
        broker_url=redis_broker_url,
        queue_name=TEST_QUEUE_NAME,
        controller_handle=controller,
        namespace=SERVE_NAMESPACE,
    )
    yield monitor
    kill_queue_monitor_actor(TEST_DEPLOYMENT_ID, namespace=SERVE_NAMESPACE)


class TestQueueMonitorActor:
    """Integration tests for QueueMonitorActor with real Redis and Serve controller."""

    def test_queue_length_fetch(self, redis_client, queue_monitor):
        """Test QueueMonitor correctly fetches queue length from broker."""
        for i in range(30):
            redis_client.lpush(TEST_QUEUE_NAME, f"message_{i}")

        def check_length():
            return ray.get(queue_monitor.get_queue_length.remote()) == 30

        wait_for_condition(check_length, timeout=30)

    def test_queue_length_updates_on_change(self, redis_client, queue_monitor):
        """Test QueueMonitor returns updated length when queue changes."""
        for i in range(10):
            redis_client.lpush(TEST_QUEUE_NAME, f"message_{i}")

        def check_initial_length():
            return ray.get(queue_monitor.get_queue_length.remote()) == 10

        wait_for_condition(check_initial_length, timeout=30)

        for i in range(10, 25):
            redis_client.lpush(TEST_QUEUE_NAME, f"message_{i}")

        def check_updated_length():
            return ray.get(queue_monitor.get_queue_length.remote()) == 25

        wait_for_condition(check_updated_length, timeout=30)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
