import numpy as np
import requests

from ray import serve


def test_np_in_composed_model(serve_instance):
    # https://github.com/ray-project/ray/issues/9441
    # AttributeError: 'bytes' object has no attribute 'readonly'
    # in cloudpickle _from_numpy_buffer

    def sum_model(_request, data=None):
        return np.sum(data)

    class ComposedModel:
        def __init__(self):
            self.model = serve.get_handle("sum_model")

        async def __call__(self, _request):
            data = np.ones((10, 10))
            result = await self.model.remote(data=data)
            return result

    serve.create_backend("sum_model", sum_model)
    serve.create_endpoint("sum_model", backend="sum_model")
    serve.create_backend("model", ComposedModel)
    serve.create_endpoint(
        "model", backend="model", route="/model", methods=["GET"])

    result = requests.get("http://127.0.0.1:8000/model")
    assert result.status_code == 200
    assert result.json() == 100.0


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
