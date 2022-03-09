import json
import tempfile
import numpy as np
import requests

from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import DataBatchType, Predictor
from ray.serve.model_wrappers import ModelWrapper
import ray
from ray import serve


class AdderPredictor(Predictor):
    def __init__(self, increment: int) -> None:
        self.increment = increment

    @classmethod
    def from_checkpoint(cls, checkpoint: "AdderCheckpoint") -> "Predictor":
        return cls(checkpoint.increment)

    def predict(self, data: DataBatchType) -> DataBatchType:
        return [
            {"value": val, "batch_size": len(data)}
            for val in (np.array(data) + self.increment).tolist()
        ]


class AdderCheckpoint(Checkpoint):
    def __init__(self, increment: int):
        self.increment = increment

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        return cls(data["increment"])

    @classmethod
    def from_uri(cls, uri: str) -> "Checkpoint":
        with open(uri) as f:
            return cls(json.load(f))


def adder_schema(query_param_arg: int) -> DataBatchType:
    return np.array([query_param_arg])


@ray.remote
def send_request():
    return requests.post(
        "http://localhost:8000/Adder/predict", json={"array": [40]}
    ).json()


def test_simple_adder(serve_instance):
    serve.deployment(name="Adder")(ModelWrapper).deploy(
        predictor_cls=AdderPredictor,
        checkpoint=AdderCheckpoint.from_dict({"increment": 2}),
    )
    resp = ray.get(send_request.remote())
    assert resp == {"value": [42], "batch_size": 1}


def test_batching(serve_instance):
    serve.deployment(name="Adder")(ModelWrapper).deploy(
        predictor_cls=AdderPredictor,
        checkpoint=AdderCheckpoint.from_dict({"increment": 2}),
        batching_params=dict(max_batch_size=2, batch_wait_timeout_s=1000),
    )

    refs = [send_request.remote() for _ in range(2)]
    for resp in ray.get(refs):
        assert resp == {"value": [42], "batch_size": 2}


def test_yaml_compatibility(serve_instance):
    _, path = tempfile.mkstemp()
    with open(path, "w") as f:
        json.dump(2, f)

    client = ServeSubmissionClient()
    client.deploy_application(
        [
            {
                "name": "Adder",
                "import_path": "ray.serve.model_wrappers.ModelWrapper",
                "init_kwargs": {
                    "predictor_cls": "ray.serve.tests.test_model_wrappers.AdderPredictor",
                    "checkpoint": {
                        "checkpoint_cls": "ray.serve.tests.test_model_wrappers.AdderCheckpoint",
                        "uri": path,
                    },
                    "input_schema": "ray.serve.tests.test_model_wrappers.adder_schema",
                    "batching_params": {"max_batch_size": 1},
                },
            }
        ]
    )

    resp = ray.get(send_request.remote())
    assert resp == {"value": [42], "batch_size": 1}
