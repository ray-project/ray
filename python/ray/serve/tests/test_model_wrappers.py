import numpy as np
import requests

from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import DataBatchType, Predictor
from ray.serve.model_wrappers import ModelWrapper
from ray import serve


class AdderPredictor(Predictor):
    def __init__(self, increment: int) -> None:
        self.increment = increment

    @classmethod
    def from_checkpoint(cls, checkpoint: "AdderCheckpoint") -> "Predictor":
        return cls(checkpoint.increment)

    def predict(self, data: DataBatchType) -> DataBatchType:
        return (np.array(data) + self.increment).tolist()


class AdderCheckpoint(Checkpoint):
    def __init__(self, increment: int):
        self.increment = increment

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        return cls(data["increment"])


# TODO(simon): Add REST API and CLI integration test
def test_simple_adder(serve_instance):
    serve.deployment(name="Adder")(ModelWrapper).deploy(
        predictor_cls=AdderPredictor,
        checkpoint=AdderCheckpoint.from_dict({"increment": 2}),
    )
    resp = requests.post(
        "http://localhost:8000/Adder/predict", json={"array": [40]}
    ).json()
    assert resp == [42]
