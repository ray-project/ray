import json
import tempfile
from fastapi import Depends, FastAPI

import numpy as np
import requests
from requests.adapters import HTTPAdapter, Retry

from ray._private.test_utils import wait_for_condition
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import DataBatchType, Predictor
from ray.serve.model_wrappers import ModelWrapperDeployment
from ray.serve.pipeline.api import build
from ray.serve.dag import InputNode
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.http_adapters import json_to_ndarray
import ray
from ray import serve


class AdderPredictor(Predictor):
    def __init__(self, increment: int) -> None:
        self.increment = increment

    @classmethod
    def from_checkpoint(cls, checkpoint: "AdderCheckpoint") -> "Predictor":
        if checkpoint._data_dict:
            return cls(checkpoint._data_dict["increment"])
        elif checkpoint._local_path:  # uri case
            with open(checkpoint._local_path) as f:
                return cls(json.load(f))
        raise Exception("Unreachable")

    def predict(self, data: DataBatchType) -> DataBatchType:
        return [
            {"value": val, "batch_size": len(data)}
            for val in (np.array(data) + self.increment).tolist()
        ]


class AdderCheckpoint(Checkpoint):
    pass


def adder_schema(query_param_arg: int) -> DataBatchType:
    return np.array([query_param_arg])


@ray.remote
def send_request(**requests_kargs):
    return requests.post("http://localhost:8000/Adder/", **requests_kargs).json()


def test_simple_adder(serve_instance):
    ModelWrapperDeployment.options(name="Adder").deploy(
        predictor_cls=AdderPredictor,
        checkpoint=AdderCheckpoint.from_dict({"increment": 2}),
    )
    resp = ray.get(send_request.remote(json={"array": [40]}))
    assert resp == {"value": [42], "batch_size": 1}


def test_batching(serve_instance):
    ModelWrapperDeployment.options(name="Adder").deploy(
        predictor_cls=AdderPredictor,
        checkpoint=AdderCheckpoint.from_dict({"increment": 2}),
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
    _, path = tempfile.mkstemp()
    with open(path, "w") as f:
        json.dump(2, f)

    predictor_cls = "ray.serve.tests.test_model_wrappers.AdderPredictor"
    checkpoint_cls = "ray.serve.tests.test_model_wrappers.AdderCheckpoint"

    with InputNode() as dag_input:
        m1 = ModelWrapperDeployment.bind(
            predictor_cls=predictor_cls,  # TODO: can't be the raw class right now?
            checkpoint={  # TODO: can't be the raw object right now?
                "checkpoint_cls": checkpoint_cls,
                "uri": path,
            },
        )
        dag = m1.predict.bind(dag_input)
    deployments = build(Ingress.bind(dag))
    for d in deployments:
        d.deploy()

    resp = requests.post("http://127.0.0.1:8000/ingress", json={"array": [40]})
    print(resp.text)
    resp.raise_for_status()
    return resp.json() == {"value": [42], "batch_size": 1}


# NOTE(simon): Make sure this is the last test because the REST API will start
# controller and http proxy in another namespace.
def test_yaml_compatibility(serve_instance):
    _, path = tempfile.mkstemp()
    with open(path, "w") as f:
        json.dump(2, f)

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1)
    session.mount("http://", HTTPAdapter(max_retries=retries))

    # TODO(simon): use ServeSubmissionClient when it's merged.
    predictor_cls = "ray.serve.tests.test_model_wrappers.AdderPredictor"
    checkpoint_cls = "ray.serve.tests.test_model_wrappers.AdderCheckpoint"
    schema_func = "ray.serve.tests.test_model_wrappers.adder_schema"

    resp = session.put(
        "http://127.0.0.1:8265/api/serve/deployments/",
        json={
            "deployments": [
                {
                    "name": "Adder",
                    "import_path": "ray.serve.model_wrappers.ModelWrapperDeployment",
                    "init_kwargs": {
                        "predictor_cls": predictor_cls,
                        "checkpoint": {
                            "checkpoint_cls": checkpoint_cls,
                            "uri": path,
                        },
                        "http_adapter": schema_func,
                        "batching_params": {"max_batch_size": 1},
                    },
                }
            ]
        },
    )
    resp.raise_for_status()

    # Note(simon): The Serve HTTP deploy is non blocking,
    # so we retries to make sure the deployment is up
    def cond():
        resp = ray.get(send_request.remote(params={"query_param_arg": 40}))
        return resp == {"value": [42], "batch_size": 1}

    wait_for_condition(cond)
