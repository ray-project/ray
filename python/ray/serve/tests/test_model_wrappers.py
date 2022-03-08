from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
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


# TODO(simon): Add REST API and CLI integration test
def test_simple_adder(serve_instance):
    serve.deployment(name="Adder")(ModelWrapper).deploy(
        predictor_cls=AdderPredictor,
        checkpoint=AdderCheckpoint.from_dict({"increment": 2}),
    )
    resp = requests.post(
        "http://localhost:8000/Adder/predict", json={"array": [40]}
    ).json()
    assert resp == {"value": [42], "batch_size": 1}


def test_batching(serve_instance):
    serve.deployment(name="Adder")(ModelWrapper).deploy(
        predictor_cls=AdderPredictor,
        checkpoint=AdderCheckpoint.from_dict({"increment": 2}),
        batching_params=dict(max_batch_size=2, batch_wait_timeout_s=1000),
    )

    with ThreadPoolExecutor() as pool:
        futs = [
            pool.submit(
                requests.post,
                "http://localhost:8000/Adder/predict",
                json={"array": [40]},
            )
            for _ in range(2)
        ]
        for fut in concurrent.futures.as_completed(futs):
            resp = fut.result().json()
    assert resp == {"value": [42], "batch_size": 2}
