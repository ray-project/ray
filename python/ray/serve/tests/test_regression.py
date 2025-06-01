import asyncio
import gc
import sys

import numpy as np
import pytest
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse

import ray
from ray import serve
from ray._private.test_utils import SignalActor
from ray.serve.context import _get_global_client
from ray.serve.handle import DeploymentHandle


@pytest.fixture
def shutdown_ray():
    if ray.is_initialized():
        serve.shutdown()
        ray.shutdown()
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
    serve.run(CustomService.bind())


def test_np_in_composed_model(serve_instance):
    # https://github.com/ray-project/ray/issues/9441
    # AttributeError: 'bytes' object has no attribute 'readonly'
    # in cloudpickle _from_numpy_buffer

    @serve.deployment
    class Sum:
        def __call__(self, data):
            return np.sum(data)

    @serve.deployment(name="model")
    class ComposedModel:
        def __init__(self, handle: DeploymentHandle):
            self.model = handle

        async def __call__(self):
            data = np.ones((10, 10))
            return await self.model.remote(data)

    sum_d = Sum.bind()
    cm_d = ComposedModel.bind(sum_d)
    serve.run(cm_d)

    result = requests.get("http://127.0.0.1:8000/")
    assert result.status_code == 200
    assert float(result.text) == 100.0


def test_replica_memory_growth(serve_instance):
    # https://github.com/ray-project/ray/issues/12395
    @serve.deployment
    def gc_unreachable_objects(*args):
        gc.set_debug(gc.DEBUG_SAVEALL)
        gc.collect()
        gc_garbage_len = len(gc.garbage)
        if gc_garbage_len > 0:
            print(gc.garbage)
        return gc_garbage_len

    handle = serve.run(gc_unreachable_objects.bind())

    def get_gc_garbage_len_http():
        result = requests.get("http://127.0.0.1:8000")
        assert result.status_code == 200
        return result.json()

    # We are checking that there's constant number of object in gc.
    known_num_objects_from_http = get_gc_garbage_len_http()

    for _ in range(10):
        assert get_gc_garbage_len_http() == known_num_objects_from_http

    known_num_objects_from_handle = handle.remote().result()
    for _ in range(10):
        assert handle.remote().result() == known_num_objects_from_handle


def test_ref_in_handle_input(serve_instance):
    # https://github.com/ray-project/ray/issues/12593

    unblock_worker_signal = SignalActor.remote()

    @serve.deployment
    async def blocked_by_ref(data):
        assert not isinstance(data, ray.ObjectRef)

    handle = serve.run(blocked_by_ref.bind())

    # Pass in a ref that's not ready yet
    ref = unblock_worker_signal.wait.remote()
    worker_result = handle.remote(ref)

    # Worker shouldn't execute the request
    with pytest.raises(TimeoutError):
        worker_result.result(timeout_s=1)

    # Now unblock the worker
    unblock_worker_signal.send.remote()
    worker_result.result()


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

    serve.run(A.bind())

    # The nested actor should start successfully.
    ray.get(signal.wait.remote(), timeout=10)


def test_handle_cache_out_of_scope(serve_instance):
    # https://github.com/ray-project/ray/issues/18980
    initial_num_cached = len(_get_global_client().handle_cache)

    @serve.deployment(name="f")
    def f():
        return "hi"

    handle = serve.run(f.bind(), name="app")

    handle_cache = _get_global_client().handle_cache
    assert len(handle_cache) == initial_num_cached + 1

    def sender_where_handle_goes_out_of_scope():
        f = _get_global_client().get_handle("f", "app", check_exists=False)
        assert f is handle
        assert f.remote().result() == "hi"

    [sender_where_handle_goes_out_of_scope() for _ in range(30)]
    assert len(handle_cache) == initial_num_cached + 1


def test_out_of_order_chaining(serve_instance):
    # https://discuss.ray.io/t/concurrent-queries-blocking-following-queries/3949

    @ray.remote(num_cpus=0)
    class Collector:
        def __init__(self):
            self.lst = []

        def append(self, msg):
            self.lst.append(msg)

        def get(self):
            return self.lst

    collector = Collector.remote()

    @serve.deployment
    class Combine:
        def __init__(self, m1, m2):
            self.m1 = m1
            self.m2 = m2

        async def run(self, _id):
            return await self.m2.compute.remote(self.m1.compute.remote(_id))

    @serve.deployment(graceful_shutdown_timeout_s=0.0)
    class FirstModel:
        async def compute(self, _id):
            if _id == 0:
                await asyncio.sleep(1000)
            print(f"First output: {_id}")
            ray.get(collector.append.remote(f"first-{_id}"))
            return _id

    @serve.deployment
    class SecondModel:
        async def compute(self, _id):
            print(f"Second output: {_id}")
            ray.get(collector.append.remote(f"second-{_id}"))
            return _id

    m1 = FirstModel.bind()
    m2 = SecondModel.bind()
    combine = Combine.bind(m1, m2)
    handle = serve.run(combine)

    handle.run.remote(_id=0)
    handle.run.remote(_id=1).result()

    assert ray.get(collector.get.remote()) == ["first-1", "second-1"]


def test_uvicorn_duplicate_headers(serve_instance):
    # https://github.com/ray-project/ray/issues/21876
    app = FastAPI()

    @serve.deployment
    @serve.ingress(app)
    class A:
        @app.get("/")
        def func(self):
            return JSONResponse({"a": "b"})

    serve.run(A.bind())
    resp = requests.get("http://127.0.0.1:8000")
    # If the header duplicated, it will be "9, 9"
    assert resp.headers["content-length"] == "9"


def test_healthcheck_timeout(serve_instance):
    # https://github.com/ray-project/ray/issues/24554

    signal = SignalActor.remote()

    @serve.deployment(
        health_check_timeout_s=2,
        health_check_period_s=1,
        graceful_shutdown_timeout_s=0,
    )
    class A:
        def __call__(self):
            ray.get(signal.wait.remote())

    handle = serve.run(A.bind())
    response = handle.remote()
    # without the proper fix, the ref will fail with actor died error.
    with pytest.raises(TimeoutError):
        response.result(timeout_s=10)
    signal.send.remote()
    response.result()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
