import numpy as np
import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster


def test_np_in_composed_model(serve_instance):
    client = serve_instance

    # https://github.com/ray-project/ray/issues/9441
    # AttributeError: 'bytes' object has no attribute 'readonly'
    # in cloudpickle _from_numpy_buffer

    def sum_model(request):
        return np.sum(request.args["data"])

    class ComposedModel:
        def __init__(self):
            client = serve.connect()
            self.model = client.get_handle("sum_model")

        async def __call__(self, _request):
            data = np.ones((10, 10))
            result = await self.model.remote(data=data)
            return result

    client.create_backend("sum_model", sum_model)
    client.create_endpoint("sum_model", backend="sum_model")
    client.create_backend("model", ComposedModel)
    client.create_endpoint(
        "model", backend="model", route="/model", methods=["GET"])

    result = requests.get("http://127.0.0.1:8000/model")
    assert result.status_code == 200
    assert result.json() == 100.0


def test_detached_deployment():
    # https://github.com/ray-project/ray/issues/11437

    cluster = Cluster()
    head_node = cluster.add_node(node_ip_address="127.0.0.1", num_cpus=6)

    # Create first job, check we can run a simple serve endpoint
    ray.init(head_node.address)
    first_job_id = ray.get_runtime_context().job_id
    client = serve.start(detached=True)
    client.create_backend("f", lambda _: "hello")
    client.create_endpoint("f", backend="f")
    assert ray.get(client.get_handle("f").remote()) == "hello"

    ray.shutdown()

    # Create the second job, make sure we can still create new backends.
    ray.init(head_node.address)
    assert ray.get_runtime_context().job_id != first_job_id

    client = serve.connect()
    client.create_backend("g", lambda _: "world")
    client.create_endpoint("g", backend="g")
    assert ray.get(client.get_handle("g").remote()) == "world"

    # Test passed, clean up.
    client.shutdown()
    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
