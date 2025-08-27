from typing import Any, Dict

from ray import serve


def test_deployment_custom_metric(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    @serve.deployment
    class DummyMetricIncrementer:
        def __init__(self):
            self.counter = 0

        async def __call__(self) -> str:
            self.counter += 1
            return "Hello, world"

        async def record_autoscaling_stats(self) -> Dict[str, Any]:
            # Increments each time the deployment has been called
            return {"counter": self.counter}

    assert True


def test_deployment_custom_metric_custom_method(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    @serve.deployment
    class CustomMetricMethodOverride:
        def __init__(self):
            self.counter = 0

        async def __call__(self) -> str:
            self.counter += 1
            return "Hello, world"

        async def my_custom_metric_method(self) -> Dict[str, Any]:
            # Increments each time the deployment has been called
            return {"counter": 42}

    assert True
