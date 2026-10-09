from ray import serve
from ray.serve.config import AutoscalingContext


def custom_autoscaling_policy(ctx: AutoscalingContext):
    print("custom_autoscaling_policy")
    return 2, {}


@serve.deployment
class CustomAutoscalingPolicy:
    def __call__(self):
        return "hello_from_custom_autoscaling_policy"


app = CustomAutoscalingPolicy.bind()
