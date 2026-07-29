# __basic_example_begin__
from ray import serve
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy
from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig
from ray.serve.task_consumer import task_consumer, task_handler

processor_config = TaskProcessorConfig(
    queue_name="my_queue",
    adapter_config=CeleryAdapterConfig(
        broker_url="redis://localhost:6379/0",
        backend_url="redis://localhost:6379/1",
    ),
)


@serve.deployment(
    max_ongoing_requests=5,
    autoscaling_config=AutoscalingConfig(
        min_replicas=1,
        max_replicas=10,
        target_ongoing_requests=2,
        policy=AutoscalingPolicy(
            policy_function="ray.serve.async_inference_autoscaling_policy:AsyncInferenceAutoscalingPolicy",
            policy_kwargs={
                "broker_url": "redis://localhost:6379/0",
                "queue_name": "my_queue",
            },
        ),
    ),
)
@task_consumer(task_processor_config=processor_config)
class MyConsumer:
    @task_handler(name="process")
    def process(self, data):
        return f"processed: {data}"


app = MyConsumer.bind()
serve.run(app)
# __basic_example_end__
