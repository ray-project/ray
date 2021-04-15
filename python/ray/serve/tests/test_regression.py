import gc

import numpy as np
import requests
import pytest

import ray
from ray.exceptions import GetTimeoutError
from ray import serve
from ray.test_utils import SignalActor


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

    for _ in range(10):
        result = requests.get("http://127.0.0.1:8000/model")
        assert result.status_code == 200
        num_unreachable_objects = result.json()
        assert num_unreachable_objects == 0

    for _ in range(10):
        num_unreachable_objects = ray.get(handle.remote())
        assert num_unreachable_objects == 0


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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
