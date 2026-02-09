import os
import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.queue_monitor import get_queue_monitor_actor
from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig
from ray.serve.task_consumer import (
    instantiate_adapter_from_config,
    task_consumer,
    task_handler,
)
from ray.tests.conftest import external_redis  # noqa: F401


@ray.remote
def enqueue_task(processor_config: TaskProcessorConfig, data, task_name="process"):
    adapter = instantiate_adapter_from_config(task_processor_config=processor_config)
    result = adapter.enqueue_task_sync(task_name, args=[data])
    assert result.id is not None
    return result.id


def get_num_running_replicas(controller, deployment_name, app_name):
    """Get the number of running replicas for a deployment."""
    deployment_id = DeploymentID(name=deployment_name, app_name=app_name)
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment_id)
    )
    return len(replicas.get([ReplicaState.RUNNING]))


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
class TestTaskConsumerQueueAutoscaling:
    """Test queue-based autoscaling for TaskConsumer deployments."""

    def test_task_consumer_queue_autoscaling(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test that TaskConsumer deployments autoscale based on queue length.

        Verifies the full e2e flow:
        1. QueueMonitor actor is automatically created
        2. Replicas scale up when messages pile up in the queue
        3. Replicas scale down when the queue drains
        """
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        app_name = "autoscaling_app"
        deployment_name = "AutoscalingConsumer"

        processor_config = TaskProcessorConfig(
            queue_name="autoscaling_test_queue",
            adapter_config=CeleryAdapterConfig(
                broker_url=f"redis://{redis_address}/0",
                backend_url=f"redis://{redis_address}/1",
            ),
        )

        signal = SignalActor.remote()

        @serve.deployment(
            name=deployment_name,
            max_ongoing_requests=1,
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 5,
                "target_ongoing_requests": 1,
                "upscale_delay_s": 0,
                "downscale_delay_s": 0,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 0.5,
            },
        )
        @task_consumer(task_processor_config=processor_config)
        class AutoscalingConsumer:
            def __init__(self, signal_actor):
                self._signal = signal_actor

            @task_handler(name="process")
            def process(self, data):
                ray.get(self._signal.wait.remote())

        _ = serve.run(
            AutoscalingConsumer.bind(signal),
            name=app_name,
            route_prefix="/autoscaling",
        )

        controller = serve_instance._controller

        # Wait for initial replica to be running
        wait_for_condition(
            lambda: get_num_running_replicas(controller, deployment_name, app_name)
            >= 1,
            timeout=30,
        )

        # Verify QueueMonitor actor was created
        deployment_id = DeploymentID(name=deployment_name, app_name=app_name)
        queue_monitor = get_queue_monitor_actor(
            deployment_id, namespace=SERVE_NAMESPACE
        )
        assert queue_monitor is not None

        # Enqueue tasks to build up the queue (signal blocks processing)
        num_tasks = 10
        for i in range(num_tasks):
            enqueue_task.remote(processor_config, f"data_{i}")

        # Wait for replicas to scale up to max_replicas
        wait_for_condition(
            lambda: get_num_running_replicas(controller, deployment_name, app_name)
            == 5,
            timeout=60,
        )

        # Release the signal to let all tasks drain
        ray.get(signal.send.remote())

        # Wait for replicas to scale back down to min_replicas
        wait_for_condition(
            lambda: get_num_running_replicas(controller, deployment_name, app_name)
            == 1,
            timeout=60,
        )

        serve.delete(app_name)

        # Verify QueueMonitor actor was cleaned up after deletion
        with pytest.raises(ValueError):
            get_queue_monitor_actor(deployment_id, namespace=SERVE_NAMESPACE)

    def test_no_queue_monitor_without_autoscaling(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test that no QueueMonitor is created for TaskConsumer without autoscaling."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        app_name = "no_autoscaling_app"
        deployment_name = "FixedReplicaConsumer"

        processor_config = TaskProcessorConfig(
            queue_name="no_autoscaling_queue",
            adapter_config=CeleryAdapterConfig(
                broker_url=f"redis://{redis_address}/0",
                backend_url=f"redis://{redis_address}/1",
            ),
        )

        @serve.deployment(name=deployment_name, num_replicas=2)
        @task_consumer(task_processor_config=processor_config)
        class FixedReplicaConsumer:
            @task_handler(name="process")
            def process(self, data):
                return data

        serve.run(
            FixedReplicaConsumer.bind(),
            name=app_name,
            route_prefix="/no_autoscaling",
        )

        controller = serve_instance._controller

        # Wait for replicas to be running
        wait_for_condition(
            lambda: get_num_running_replicas(controller, deployment_name, app_name)
            == 2,
            timeout=30,
        )

        # Verify no QueueMonitor actor was created
        deployment_id = DeploymentID(name=deployment_name, app_name=app_name)
        with pytest.raises(ValueError):
            get_queue_monitor_actor(deployment_id, namespace=SERVE_NAMESPACE)

        serve.delete(app_name)

    def test_app_deletion_cleans_up_queue_monitors(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test that deleting an app cleans up all QueueMonitor actors."""
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        app_name = "cleanup_app"

        processor_config_a = TaskProcessorConfig(
            queue_name="cleanup_queue_a",
            adapter_config=CeleryAdapterConfig(
                broker_url=f"redis://{redis_address}/0",
                backend_url=f"redis://{redis_address}/1",
            ),
        )
        processor_config_b = TaskProcessorConfig(
            queue_name="cleanup_queue_b",
            adapter_config=CeleryAdapterConfig(
                broker_url=f"redis://{redis_address}/0",
                backend_url=f"redis://{redis_address}/1",
            ),
        )

        @serve.deployment(
            name="ConsumerA",
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 2,
                "target_ongoing_requests": 1,
            },
        )
        @task_consumer(task_processor_config=processor_config_a)
        class ConsumerA:
            @task_handler(name="process")
            def process(self, data):
                return data

        @serve.deployment(
            name="ConsumerB",
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 2,
                "target_ongoing_requests": 1,
            },
        )
        @task_consumer(task_processor_config=processor_config_b)
        class ConsumerB:
            @task_handler(name="process")
            def process(self, data):
                return data

        # Deploy both as an app using ingress pattern
        @serve.deployment(name="Ingress")
        class Ingress:
            def __init__(self, a, b):
                pass

        serve.run(
            Ingress.bind(ConsumerA.bind(), ConsumerB.bind()),
            name=app_name,
            route_prefix="/cleanup",
        )

        controller = serve_instance._controller
        id_a = DeploymentID(name="ConsumerA", app_name=app_name)
        id_b = DeploymentID(name="ConsumerB", app_name=app_name)

        # Wait for both deployments to be running
        wait_for_condition(
            lambda: get_num_running_replicas(controller, "ConsumerA", app_name) >= 1
            and get_num_running_replicas(controller, "ConsumerB", app_name) >= 1,
            timeout=30,
        )

        # Verify both QueueMonitor actors exist
        assert get_queue_monitor_actor(id_a, namespace=SERVE_NAMESPACE) is not None
        assert get_queue_monitor_actor(id_b, namespace=SERVE_NAMESPACE) is not None

        # Delete the app
        serve.delete(app_name)

        # Verify both QueueMonitor actors were cleaned up
        with pytest.raises(ValueError):
            get_queue_monitor_actor(id_a, namespace=SERVE_NAMESPACE)
        with pytest.raises(ValueError):
            get_queue_monitor_actor(id_b, namespace=SERVE_NAMESPACE)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
