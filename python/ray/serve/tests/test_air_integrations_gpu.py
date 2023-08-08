import numpy as np
import pytest
import requests

import ray
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.serve.http_adapters import json_to_ndarray
from ray.train import Checkpoint
from ray.train.predictor import Predictor


def test_automatic_enable_gpu(serve_instance):
    """Test automatic setting of underlying Predictor use_gpu if deployment has GPUs."""

    @ray.remote
    def send_request(**requests_kargs):
        return requests.post("http://localhost:8000/GPU/", **requests_kargs).json()

    class DummyGPUPredictor(Predictor):
        def __init__(self, use_gpu):
            self.use_gpu = use_gpu

        @classmethod
        def from_checkpoint(cls, checkpoint: Checkpoint, use_gpu: bool = False):
            return cls(use_gpu)

        def predict(self, data: np.ndarray, **kwargs):
            if not self.use_gpu:
                raise ValueError("GPU not enabled")
            return [{"value": val} for val in data.tolist()]

    @serve.deployment(ray_actor_options={"num_gpus": 1})
    class DummyGPUDeployment:
        def __init__(self, checkpoint):
            self.predictor = DummyGPUPredictor.from_checkpoint(checkpoint, use_gpu=True)

        async def __call__(self, data):
            return self.predictor.predict(data)

    serve.run(
        DAGDriver.bind(
            DummyGPUDeployment.options(name="GPU").bind(Checkpoint.from_dict({"x": 1})),
            http_adapter=json_to_ndarray,
        )
    )

    assert ray.get(send_request.remote(json={"array": [40], "dtype": "int64"})) == [
        {"value": 40}
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
