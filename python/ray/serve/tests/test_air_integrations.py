import os
import tempfile
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pytest
import requests
from fastapi import Depends, FastAPI
from starlette.requests import Request

import ray
import ray.cloudpickle as ray_pickle
from ray import serve
from ray.data.extensions import TensorArray
from ray.serve._private.deployment_graph_build import build
from ray.serve.air_integrations import _BatchingManager
from ray.serve.dag import InputNode
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.http_adapters import json_to_ndarray
from ray.train import Checkpoint
from ray.train.predictor import DataBatchType, Predictor


class TestBatchingFunctionFunctions:
    def test_array(self):
        list_of_arr = [np.array([i]) for i in range(4)]
        batched_arr = np.array([[i] for i in range(4)])
        batch_size = 4

        batched = _BatchingManager.batch_array(list_of_arr)
        assert np.array_equal(batched, batched_arr)

        for i, j in zip(_BatchingManager.split_array(batched, batch_size), list_of_arr):
            assert np.array_equal(i, j)

    def test_array_error(self):
        with pytest.raises(ValueError, match="output array should have shape of"):
            _BatchingManager.split_array(np.arange(2), 10)
        with pytest.raises(TypeError, match="output should be np.ndarray but"):
            _BatchingManager.split_array("string", 6)

    def test_dict_array(self):
        list_of_dicts = [
            {"a": np.array([1, 2]), "b": np.array(3)},
            {"a": np.array([3, 4]), "b": np.array(4)},
        ]
        batched_dict = {"a": np.array([[1, 2], [3, 4]]), "b": np.array([3, 4])}
        batch_size = 2

        batched = _BatchingManager.batch_dict_array(list_of_dicts)
        assert batched.keys() == batched_dict.keys()
        for key in batched.keys():
            assert np.array_equal(batched[key], batched_dict[key])

        unpacked_list = _BatchingManager.split_dict_array(batched, batch_size)
        for original, unpacked in zip(list_of_dicts, unpacked_list):
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
        batch_size = 4

        batched = _BatchingManager.batch_dataframe(list_of_dfs)
        assert batched.equals(batched_df)

        unpacked_list = _BatchingManager.split_dataframe(batched, batch_size)
        assert len(unpacked_list) == len(list_of_dfs)
        for i, j in zip(unpacked_list, list_of_dfs):
            assert i.equals(j)

    @pytest.mark.parametrize(
        "batched_df,expected",
        [
            (
                pd.DataFrame(
                    {
                        "a": TensorArray(np.arange(12).reshape((3, 2, 2))),
                        "b": TensorArray(np.arange(12, 24).reshape((3, 2, 2))),
                    }
                ),
                pd.DataFrame(
                    {
                        "a": [[[0, 1], [2, 3]], [[4, 5], [6, 7]], [[8, 9], [10, 11]]],
                        "b": [
                            [[12, 13], [14, 15]],
                            [[16, 17], [18, 19]],
                            [[20, 21], [22, 23]],
                        ],
                    }
                ),
            ),
            (
                pd.DataFrame(
                    {
                        "a": [np.ones((2, 2)), np.ones((3, 3))],
                        "b": [np.zeros((2, 2)), np.zeros((3, 3))],
                    }
                ),
                pd.DataFrame(
                    {
                        "a": [[[1, 1], [1, 1]], [[1, 1, 1], [1, 1, 1], [1, 1, 1]]],
                        "b": [[[0, 0], [0, 0]], [[0, 0, 0], [0, 0, 0], [0, 0, 0]]],
                    }
                ),
            ),
        ],
    )
    def test_unpack_dataframe(self, batched_df, expected):
        """Test _unpack_dataframe_to_serializable with TensorArray and
        list of ndarrays.
        """
        unpacked_list = _BatchingManager.split_dataframe(batched_df, 1)
        assert len(unpacked_list) == 1
        # On windows, conversion dtype is not preserved.
        check_dtype = not os.name == "nt"
        pd.testing.assert_frame_equal(
            unpacked_list[0].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_dtype=check_dtype,
        )


def create_dict_checkpoint(
    data: Dict[str, Any], directory: Optional[str] = None
) -> Checkpoint:
    if not directory:
        directory = tempfile.mkdtemp()
    with open(os.path.join(directory, "data.pkl"), "wb") as f:
        ray_pickle.dump(data, f)
    return Checkpoint.from_directory(directory)


def load_dict_checkpoint(checkpoint: Checkpoint) -> Dict[str, Any]:
    with checkpoint.as_directory() as checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "data.pkl"), "rb") as f:
            return ray_pickle.load(f)


class AdderPredictor(Predictor):
    def __init__(self, increment: int, do_double: bool) -> None:
        self.increment = increment
        self.do_double = do_double

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, do_double: bool = False
    ) -> "AdderPredictor":
        return cls(load_dict_checkpoint(checkpoint)["increment"], do_double)

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
    @serve.deployment
    class AdderDeployment:
        def __init__(self, checkpoint: Checkpoint):
            self.predictor = AdderPredictor.from_checkpoint(checkpoint)

        async def __call__(self, request: Request):
            data = await request.json()
            return self.predictor.predict(np.array(data["array"]))

    AdderDeployment.options(name="Adder").deploy(
        checkpoint=create_dict_checkpoint({"increment": 2}),
    )
    resp = ray.get(send_request.remote(json={"array": [40]}))
    assert resp == [{"value": 42, "batch_size": 1}]


def test_predictor_kwargs(serve_instance):
    @serve.deployment
    class AdderDeployment:
        def __init__(self, checkpoint: Checkpoint):
            self.predictor = AdderPredictor.from_checkpoint(checkpoint)

        async def __call__(self, request: Request):
            data = await request.json()
            return self.predictor.predict(
                np.array(data["array"]), override_increment=100
            )

    AdderDeployment.options(name="Adder").deploy(
        checkpoint=create_dict_checkpoint({"increment": 2}),
    )

    resp = ray.get(send_request.remote(json={"array": [40]}))
    assert resp == [{"value": 140, "batch_size": 1}]


def test_predictor_from_checkpoint_kwargs(serve_instance):
    @serve.deployment
    class AdderDeployment:
        def __init__(self, checkpoint: Checkpoint):
            self.predictor = AdderPredictor.from_checkpoint(checkpoint, do_double=True)

        async def __call__(self, request: Request):
            data = await request.json()
            return self.predictor.predict(np.array(data["array"]))

    AdderDeployment.options(name="Adder").deploy(
        checkpoint=create_dict_checkpoint({"increment": 2}),
    )
    resp = ray.get(send_request.remote(json={"array": [40]}))
    assert resp == [{"value": 84, "batch_size": 1}]


def test_batching(serve_instance):
    @serve.deployment
    class AdderDeployment:
        def __init__(self, checkpoint: Checkpoint):
            self.predictor = AdderPredictor.from_checkpoint(checkpoint)

        @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
        async def __call__(self, requests: List[Request]):
            items = [await request.json() for request in requests]
            batch = np.concatenate([np.array(item["array"]) for item in items])
            return self.predictor.predict(batch)

    AdderDeployment.options(name="Adder").deploy(
        checkpoint=create_dict_checkpoint({"increment": 2}),
    )

    refs = [send_request.remote(json={"array": [40]}) for _ in range(2)]
    for resp in ray.get(refs):
        assert resp == {"value": 42, "batch_size": 2}


app = FastAPI()


@serve.deployment(route_prefix="/ingress")
@serve.ingress(app)
class Ingress:
    def __init__(self, dag: RayServeDAGHandle) -> None:
        self.dag = dag

    @app.post("/")
    async def predict(self, data=Depends(json_to_ndarray)):
        return await (await self.dag.remote(data))


def test_air_integrations_in_pipeline(serve_instance):
    path = tempfile.mkdtemp()
    create_dict_checkpoint({"increment": 2}, path)

    @serve.deployment
    class AdderDeployment:
        def __init__(self, checkpoint: Checkpoint):
            self.predictor = AdderPredictor.from_checkpoint(checkpoint)

        async def __call__(self, data):
            return self.predictor.predict(data)

    with InputNode() as dag_input:
        m1 = AdderDeployment.bind(
            checkpoint=Checkpoint.from_directory(path),
        )
        dag = m1.__call__.bind(dag_input)
    deployments = build(Ingress.bind(dag), "")
    for d in deployments:
        d.deploy()

    resp = requests.post("http://127.0.0.1:8000/ingress", json={"array": [40]})
    print(resp.text)
    resp.raise_for_status()
    return resp.json() == {"value": [42], "batch_size": 1}


def test_air_integrations_reconfigure(serve_instance):
    path = tempfile.mkdtemp()
    create_dict_checkpoint({"increment": 2}, path)

    @serve.deployment
    class AdderDeployment:
        def __init__(self, checkpoint: Checkpoint):
            self.predictor = AdderPredictor.from_checkpoint(checkpoint)

        def reconfigure(self, config):
            self.predictor = AdderPredictor.from_checkpoint(
                create_dict_checkpoint(config["checkpoint"])
            )

        async def __call__(self, data):
            return self.predictor.predict(data)

    additional_config = {
        "checkpoint": {"increment": 5},
    }

    with InputNode() as dag_input:
        m1 = AdderDeployment.options(user_config=additional_config).bind(
            checkpoint=Checkpoint.from_directory(path),
        )
        dag = m1.__call__.bind(dag_input)
    deployments = build(Ingress.bind(dag), "")
    for d in deployments:
        d.deploy()

    resp = requests.post("http://127.0.0.1:8000/ingress", json={"array": [40]})
    print(resp.text)
    resp.raise_for_status()
    return resp.json() == {"value": [45], "batch_size": 1}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
