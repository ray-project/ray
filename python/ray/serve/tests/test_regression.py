import gc

import numpy as np
import requests
import pytest
from fastapi import FastAPI

import ray
from ray.exceptions import GetTimeoutError
from ray import serve
from ray._private.test_utils import SignalActor


@pytest.fixture
def shutdown_ray():
    yield
    serve.shutdown()
    ray.shutdown()


# NOTE(simon): Make sure this test is the first in this file because it should
# be tested without ray.init/serve.start being ran.
def test_fastapi_serialization(shutdown_ray):
    # https://github.com/ray-project/ray/issues/15511
    app = FastAPI()

    @serve.deployment(name="custom_service")
    @serve.ingress(app)
    class CustomService:
        def deduplicate(self, data):
            data.drop_duplicates(inplace=True)
            return data

        @app.post("/deduplicate")
        def _deduplicate(self, request):
            data = request["data"]
            columns = request["columns"]
            import pandas as pd
            data = pd.DataFrame(data, columns=columns)
            data.drop_duplicates(inplace=True)
            return data.values.tolist()

    serve.start()
    CustomService.deploy()


def test_np_in_composed_model(serve_instance):
    # https://github.com/ray-project/ray/issues/9441
    # AttributeError: 'bytes' object has no attribute 'readonly'
    # in cloudpickle _from_numpy_buffer

    @serve.deployment
    def sum_model(data):
        return np.sum(data)

    @serve.deployment(name="model")
    class ComposedModel:
        def __init__(self):
            self.model = sum_model.get_handle(sync=False)

        async def __call__(self, _request):
            data = np.ones((10, 10))
            ref = await self.model.remote(data)
            return await ref

    sum_model.deploy()
    ComposedModel.deploy()

    result = requests.get("http://127.0.0.1:8000/model")
    assert result.status_code == 200
    assert result.json() == 100.0


def test_backend_worker_memory_growth(serve_instance):
    # https://github.com/ray-project/ray/issues/12395
    @serve.deployment(name="model")
    def gc_unreachable_objects(*args):
        gc.set_debug(gc.DEBUG_SAVEALL)
        gc.collect()
        return len(gc.garbage)

    gc_unreachable_objects.deploy()
    handle = gc_unreachable_objects.get_handle()

    # We are checking that there's constant number of object in gc.
    known_num_objects = ray.get(handle.remote())

    for _ in range(10):
        result = requests.get("http://127.0.0.1:8000/model")
        assert result.status_code == 200
        num_unreachable_objects = result.json()
        assert num_unreachable_objects == known_num_objects

    for _ in range(10):
        num_unreachable_objects = ray.get(handle.remote())
        assert num_unreachable_objects == known_num_objects


def test_ref_in_handle_input(serve_instance):
    # https://github.com/ray-project/ray/issues/12593

    unblock_worker_signal = SignalActor.remote()

    @serve.deployment
    async def blocked_by_ref(data):
        assert not isinstance(data, ray.ObjectRef)

    blocked_by_ref.deploy()
    handle = blocked_by_ref.get_handle()

    # Pass in a ref that's not ready yet
    ref = unblock_worker_signal.wait.remote()
    worker_result = handle.remote(ref)

    # Worker shouldn't execute the request
    with pytest.raises(GetTimeoutError):
        ray.get(worker_result, timeout=1)

    # Now unblock the worker
    unblock_worker_signal.send.remote()
    ray.get(worker_result)


def test_nested_actors(serve_instance):
    signal = SignalActor.remote()

    @ray.remote(num_cpus=1)
    class CustomActor:
        def __init__(self) -> None:
            signal.send.remote()

    @serve.deployment
    class A:
        def __init__(self) -> None:
            self.a = CustomActor.remote()

    A.deploy()

    # The nested actor should start successfully.
    ray.get(signal.wait.remote(), timeout=10)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
