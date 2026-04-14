import os
import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import DeploymentID, ReplicaState
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy
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
        1. Replicas scale up when messages pile up in the queue
        2. Replicas scale down when the queue drains
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
            autoscaling_config=AutoscalingConfig(
                min_replicas=1,
                max_replicas=5,
                target_ongoing_requests=1,
                upscale_delay_s=0,
                downscale_delay_s=0,
                metrics_interval_s=0.1,
                look_back_period_s=0.5,
                policy=AutoscalingPolicy(
                    policy_function="ray.serve.async_inference_autoscaling_policy:AsyncInferenceAutoscalingPolicy",
                    policy_kwargs={
                        "broker_url": f"redis://{redis_address}/0",
                        "queue_name": "autoscaling_test_queue",
                    },
                ),
            ),
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
            == 1,
            timeout=30,
        )

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

    def test_task_consumer_scale_from_and_to_zero(
        self, external_redis, serve_instance  # noqa: F811
    ):
        """Test that TaskConsumer deployments can scale down to zero.

        Verifies:
        1. Replicas scale up when messages pile up in the queue
        2. Replicas scale down to 0 when the queue drains
        """
        redis_address = os.environ.get("RAY_REDIS_ADDRESS")
        app_name = "scale_to_zero_app"
        deployment_name = "ScaleToZeroConsumer"

        processor_config = TaskProcessorConfig(
            queue_name="scale_to_zero_queue",
            adapter_config=CeleryAdapterConfig(
                broker_url=f"redis://{redis_address}/0",
                backend_url=f"redis://{redis_address}/1",
            ),
        )

        signal = SignalActor.remote()

        @serve.deployment(
            name=deployment_name,
            max_ongoing_requests=1,
            autoscaling_config=AutoscalingConfig(
                min_replicas=0,
                max_replicas=5,
                target_ongoing_requests=1,
                upscale_delay_s=0,
                downscale_delay_s=0,
                downscale_to_zero_delay_s=5,
                metrics_interval_s=0.1,
                look_back_period_s=0.5,
                policy=AutoscalingPolicy(
                    policy_function="ray.serve.async_inference_autoscaling_policy:AsyncInferenceAutoscalingPolicy",
                    policy_kwargs={
                        "broker_url": f"redis://{redis_address}/0",
                        "queue_name": "scale_to_zero_queue",
                        "poll_interval_s": 1,
                    },
                ),
            ),
        )
        @task_consumer(task_processor_config=processor_config)
        class ScaleToZeroConsumer:
            def __init__(self, signal_actor):
                self._signal = signal_actor

            @task_handler(name="process")
            def process(self, data):
                ray.get(self._signal.wait.remote())

        _ = serve.run(
            ScaleToZeroConsumer.bind(signal),
            name=app_name,
            route_prefix="/scale_to_zero",
        )

        controller = serve_instance._controller

        wait_for_condition(
            lambda: get_num_running_replicas(controller, deployment_name, app_name)
            == 0,
            timeout=60,
        )

        enqueue_task.remote(processor_config, "data_0")

        wait_for_condition(
            lambda: get_num_running_replicas(controller, deployment_name, app_name)
            == 1,
            timeout=60,
        )

        # Release the signal to let all tasks drain
        ray.get(signal.send.remote())

        # Wait for replicas to scale down to 0
        wait_for_condition(
            lambda: get_num_running_replicas(controller, deployment_name, app_name)
            == 0,
            timeout=60,
        )

        serve.delete(app_name)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
