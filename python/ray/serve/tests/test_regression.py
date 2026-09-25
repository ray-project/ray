import asyncio
import gc
import sys

import httpx
import numpy as np
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.constants import (
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD,
    RAY_SERVE_USE_GRPC_BY_DEFAULT,
)
from ray.serve._private.test_utils import get_application_url
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

    result = httpx.get(get_application_url())
    assert result.status_code == 200
    assert float(result.text) == 100.0


# https://github.com/ray-project/ray/issues/12395
def test_replica_memory_growth(serve_instance):
    # NOTE(zcin): this test checks that there are no circular references
    # since depending on the size of the objects locked in that cycle,
    # it could cause large memory growth for the replica in the short
    # term. Unfortunately the asyncio Python gRPC implementation has a
    # circular reference between
    # https://github.com/grpc/grpc/blob/04f05a3/src/python/grpcio/grpc/_server.py#L987
    # & https://github.com/grpc/grpc/blob/04f05a3/src/python/grpcio/grpc/_server.py#L993
    # So just by using the asyncio Python gRPC API in the replica, it
    # will violate the checks in this test. However the objects locked
    # in that cycle are metadata objects on the order of tens to
    # hundreds of bytes, which is very small and should be fine to be
    # garbage collected by the slower GC cycle that checks for circular
    # references. Therefore we whitelist those objects in the test.
    def whitelist(phase, info):
        if phase == "start":
            return

        for item in gc.garbage[:]:
            if getattr(type(item), "__name__", None) == "_Metadatum":
                gc.garbage.remove(item)
            elif isinstance(item, tuple) and all(
                getattr(type(s), "__name__", None) == "_Metadatum" for s in item
            ):
                gc.garbage.remove(item)
            elif (
                getattr(type(item), "__name__", None)
                == "__pyx_scope_struct_35__find_method_handler"
            ):
                gc.garbage.remove(item)
            elif (
                getattr(item, "__name__", None) == "query_handlers"
                and item.func_globals["_find_method_handler"]
            ):
                gc.garbage.remove(item)
            elif getattr(type(item), "__name__", None) == "_HandlerCallDetails":
                gc.garbage.remove(item)

    @serve.deployment
    def gc_unreachable_objects(*args):
        gc.set_debug(gc.DEBUG_SAVEALL)
        gc.callbacks.append(whitelist)
        gc.collect()
        gc_garbage_len = len(gc.garbage)
        if gc_garbage_len > 0:
            print(gc.garbage)
        return gc_garbage_len

    handle = serve.run(gc_unreachable_objects.bind())

    def get_gc_garbage_len_http():
        result = httpx.get(get_application_url())
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
    resp = httpx.get("http://127.0.0.1:8000")
    # If the header duplicated, it will be "9, 9"
    assert resp.headers["content-length"] == "9"


@pytest.mark.skipif(
    not RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD,
    reason="Health check will block if user code is running in the main event loop",
)
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


@pytest.mark.skipif(
    RAY_SERVE_USE_GRPC_BY_DEFAULT,
    reason="Cannot get object ref when using gRPC.",
)
@pytest.mark.skipif(
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    reason="local_testing_mode doesn't support _to_object_ref",
)
def test_to_object_ref_returns_after_assignment(serve_instance):
    """https://github.com/ray-project/ray/issues/46893

    FastAPI ingress converts a downstream ``DeploymentResponse`` via the
    issue's ``_to_object_ref_or_gen`` helper, then awaits the value.
    ``hold=False`` must return after replica assignment while Foo is still
    blocked. ``hold=True`` waits until the object exists.
    """
    ready = SignalActor.remote()
    helper_done = SignalActor.remote()
    fastapi_app = FastAPI()

    async def _to_object_ref_or_gen(self, hold=False):
        """Replicates the local helper from the issue.

        2.10's ``_object_ref_future`` + ``__anext__`` resolved the next
        stream item. That is now ``_to_object_ref()`` (replica assignment)
        and optional ``_ready()`` (object exists). Do not call
        ``__anext__`` on the peeked generator: it races consume-when-ready.
        """
        obj_ref_or_gen = await self._to_object_ref()
        if hold:
            await obj_ref_or_gen._ready()
        return obj_ref_or_gen

    @serve.deployment()
    @serve.ingress(fastapi_app)
    class Dispatcher:
        def __init__(self, foo_handler: DeploymentHandle, helper_done):
            self.foo_handler = foo_handler
            self._helper_done = helper_done

        @fastapi_app.post("/")
        async def entry(self, hold: bool):
            handle = self.foo_handler.remote()
            ref = await asyncio.wait_for(
                _to_object_ref_or_gen(handle, hold), timeout=30
            )
            await self._helper_done.send.remote()
            result = await ref
            return result

    @serve.deployment()
    class Foo:
        def __init__(self, ready):
            self._ready = ready

        async def __call__(self):
            await self._ready.wait.remote()
            return "Finished!"

    handle = serve.run(Dispatcher.bind(Foo.bind(ready), helper_done))

    def _foo_waiting():
        return ray.get(ready.cur_num_waiters.remote()) == 1

    # hold=False: helper returns after replica assignment; Foo still blocked.
    pending = handle.entry.remote(False)
    wait_for_condition(_foo_waiting)
    ray.get(helper_done.wait.remote(), timeout=3)  # helper returned
    ray.get(ready.send.remote(clear=True))
    assert pending.result(timeout_s=10) == "Finished!"

    ray.get(helper_done.send.remote(clear=True))
    # hold=True: helper waits until the object exists.
    pending = handle.entry.remote(True)
    wait_for_condition(_foo_waiting)
    with pytest.raises(TimeoutError):
        ray.get(helper_done.wait.remote(), timeout=3)
    ray.get(ready.send.remote(clear=True))
    assert pending.result(timeout_s=10) == "Finished!"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
