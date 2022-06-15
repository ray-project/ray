import tempfile
from typing import Optional

from fastapi import Depends, FastAPI
import pandas as pd
import numpy as np
import requests
import pytest

from ray.serve.model_wrappers import (
    ModelWrapperDeployment,
    collate_array,
    collate_dataframe,
    collate_dict_array,
)
from ray.air.checkpoint import Checkpoint
from ray.air.predictor import DataBatchType, Predictor
from ray.serve.deployment_graph_build import build
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
        with pytest.raises(ValueError, match="output array should have shape of"):
            unpack(np.arange(2))
        with pytest.raises(TypeError, match="output should be np.ndarray but"):
            unpack("string")

    def test_dict_array(self):
        list_of_dicts = [
            {"a": np.array([1, 2]), "b": np.array(3)},
            {"a": np.array([3, 4]), "b": np.array(4)},
        ]
        batched_dict = {"a": np.array([[1, 2], [3, 4]]), "b": np.array([3, 4])}

        batched, unpack = collate_dict_array(list_of_dicts)
        assert batched.keys() == batched_dict.keys()
        for key in batched.keys():
            assert np.array_equal(batched[key], batched_dict[key])

        for original, unpacked in zip(list_of_dicts, unpack(batched)):
            assert original.keys() == unpacked.keys()
            for key in original.keys():
                assert np.array_equal(original[key], unpacked[key])

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
    def __init__(self, increment: int, do_double: bool) -> None:
        self.increment = increment
        self.do_double = do_double

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, do_double: bool = False
    ) -> "AdderPredictor":
        return cls(checkpoint.to_dict()["increment"], do_double)

    def predict(
        self, data: np.ndarray, override_increment: Optional[int] = None
    ) -> DataBatchType:
        increment = override_increment or self.increment
        multiplier = 2 if self.do_double else 1
        return [
            {"value": val, "batch_size": len(data)}
            for val in ((data + increment) * multiplier).tolist()
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


def test_predictor_kwargs(serve_instance):
    ModelWrapperDeployment.options(name="Adder").deploy(
        predictor_cls=AdderPredictor,
        checkpoint=Checkpoint.from_dict({"increment": 2}),
        predict_kwargs={"override_increment": 100},
    )
    resp = ray.get(send_request.remote(json={"array": [40]}))
    assert resp == {"value": [140], "batch_size": 1}


def test_predictor_from_checkpoint_kwargs(serve_instance):
    ModelWrapperDeployment.options(name="Adder").deploy(
        predictor_cls=AdderPredictor,
        checkpoint=Checkpoint.from_dict({"increment": 2}),
        do_double=True,
    )
    resp = ray.get(send_request.remote(json={"array": [40]}))
    assert resp == {"value": [84], "batch_size": 1}


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
