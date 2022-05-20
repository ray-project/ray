import tempfile

from fastapi import Depends, FastAPI
import pandas as pd
import numpy as np
import requests
import pytest

from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import DataBatchType, Predictor
from ray.serve.model_wrappers import (
    ModelWrapperDeployment,
    collate_array,
    collate_dataframe,
)
from ray.serve.pipeline.api import build
from ray.serve.dag import InputNode
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.http_adapters import json_to_ndarray
import ray
from ray import serve


class TestCollationFunctions:
    def test_array(self):
        list_of_arr = [np.array([i]) for i in range(4)]
        batched_arr = np.array([[i] for i in range(4)])

        batched, unpack = collate_array(list_of_arr)
        assert np.array_equal(batched, batched_arr)
        for i, j in zip(unpack(batched), list_of_arr):
            assert np.array_equal(i, j)

    def test_array_error(self):
        list_of_arr = [np.array([i]) for i in range(4)]
        _, unpack = collate_array(list_of_arr)
        with pytest.raises(AssertionError, match="output array should have shape of"):
            unpack(np.arange(2))
        with pytest.raises(AssertionError, match="output should be np.ndarray but"):
            unpack("string")

    def test_dataframe(self):
        list_of_dfs = [pd.DataFrame({"a": [i, i], "b": [i, i]}) for i in range(4)]
        batched_df = pd.DataFrame(
            {
                "a": sum(([i, i] for i in range(4)), []),
                "b": sum(([i, i] for i in range(4)), []),
            }
        )
        batched, unpack = collate_dataframe(list_of_dfs)
        assert batched.equals(batched_df)
        assert len(unpack(batched)) == len(list_of_dfs)
        for i, j in zip(unpack(batched), list_of_dfs):
            assert i.equals(j)


class AdderPredictor(Predictor):
    def __init__(self, increment: int) -> None:
        self.increment = increment

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint) -> "AdderPredictor":
        return cls(checkpoint.to_dict()["increment"])

    def predict(self, data: np.ndarray) -> DataBatchType:
        return [
            {"value": val, "batch_size": len(data)}
            for val in (data + self.increment).tolist()
        ]


def adder_adapter(query_param_arg: int) -> DataBatchType:
    return np.array([query_param_arg])


@ray.remote
def send_request(**requests_kargs):
    return requests.post("http://localhost:8000/Adder/", **requests_kargs).json()


def test_simple_adder(serve_instance):
    ModelWrapperDeployment.options(name="Adder").deploy(
        predictor_cls=AdderPredictor,
        checkpoint=Checkpoint.from_dict({"increment": 2}),
    )
    resp = ray.get(send_request.remote(json={"array": [40]}))
    assert resp == {"value": [42], "batch_size": 1}


def test_batching(serve_instance):
    ModelWrapperDeployment.options(name="Adder").deploy(
        predictor_cls=AdderPredictor,
        checkpoint=Checkpoint.from_dict({"increment": 2}),
        batching_params=dict(max_batch_size=2, batch_wait_timeout_s=1000),
    )

    refs = [send_request.remote(json={"array": [40]}) for _ in range(2)]
    for resp in ray.get(refs):
        assert resp == {"value": [42], "batch_size": 2}


app = FastAPI()


@serve.deployment(route_prefix="/ingress")
@serve.ingress(app)
class Ingress:
    def __init__(self, dag: RayServeDAGHandle) -> None:
        self.dag = dag

    @app.post("/")
    async def predict(self, data=Depends(json_to_ndarray)):
        return await self.dag.remote(data)


def test_model_wrappers_in_pipeline(serve_instance):
    path = tempfile.mkdtemp()
    uri = f"file://{path}/test_uri"
    Checkpoint.from_dict({"increment": 2}).to_uri(uri)

    predictor_cls = "ray.serve.tests.test_model_wrappers.AdderPredictor"

    with InputNode() as dag_input:
        m1 = ModelWrapperDeployment.bind(
            predictor_cls=predictor_cls,
            checkpoint=uri,
        )
        dag = m1.predict.bind(dag_input)
    deployments = build(Ingress.bind(dag))
    for d in deployments:
        d.deploy()

    resp = requests.post("http://127.0.0.1:8000/ingress", json={"array": [40]})
    print(resp.text)
    resp.raise_for_status()
    return resp.json() == {"value": [42], "batch_size": 1}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
