import pytest
import requests
import os

from ray import serve
from ray._private.test_utils import wait_for_condition
import ray
from ray.serve._private.constants import RAY_SERVE_REQUEST_MODEL_ID
from fastapi import FastAPI
import starlette
from typing import List
from ray._private.test_utils import SignalActor


def check_model_id_in_replica_info(model_ids: List[str], handle, num_replicas=1):
    """Check if the model ids are in the replica info"""
    count = 0
    for replica in handle.router._replica_set.in_flight_queries:
        if model_ids == list(replica.model_ids):
            count += 1
            if count == num_replicas:
                return True
    return False


def test_basic_multiplex(serve_instance):
    """Test basic multiplex functionality."""

    @serve.deployment(num_replicas=2)
    class Model:
        @serve.multiplexed
        def get_model(self, tag):
            return tag

        async def __call__(self, request):
            tag = serve.get_model_id()
            await self.get_model(tag)
            # return pid to check if the same model is used
            return os.getpid()

    handle = serve.run(Model.bind())
    headers = {RAY_SERVE_REQUEST_MODEL_ID: str(1)}
    resp = requests.get("http://localhost:8000", headers=headers)
    # Wait for the model ids information to be updated in the router replica info
    wait_for_condition(
        check_model_id_in_replica_info, model_ids=["1"], handle=handle, timeout=10
    )

    # Run 10 times to make sure the same replica is used
    for _ in range(10):
        cur_resp = requests.get("http://localhost:8000", headers=headers)
        assert resp.text == cur_resp.text
    new_handle = handle.options(model_id="1")
    for _ in range(10):
        cur_resp = ray.get(new_handle.remote("blablabla"))
        assert resp.text == str(cur_resp)


def test_fastapi_multiplex(serve_instance):
    """Test basic multiplex functionality with FastAPI."""

    app = FastAPI()

    @serve.deployment(num_replicas=2)
    @serve.ingress(app)
    class Model:
        @serve.multiplexed
        def get_model(self, tag):
            return tag

        @app.get("/run")
        async def run(self, request: starlette.requests.Request):
            tag = serve.get_model_id()
            await self.get_model(tag)
            # return pid to check if the same model is used
            return os.getpid()

        @app.get("/run2")
        async def run2(self, request: starlette.requests.Request):
            # This function doesn't use multiplexed decorator
            return "hello run2"

    handle = serve.run(Model.bind())
    headers = {RAY_SERVE_REQUEST_MODEL_ID: str(1)}
    resp = requests.get("http://localhost:8000/run", headers=headers)
    # Wait for the model ids information to be updated in the router replica info
    wait_for_condition(
        check_model_id_in_replica_info, model_ids=["1"], handle=handle, timeout=10
    )

    # Run 10 times to make sure the same replica is used
    for _ in range(10):
        cur_resp = requests.get("http://localhost:8000/run", headers=headers)
        assert resp.text == cur_resp.text

    resp = requests.get("http://localhost:8000/run2", headers=headers)
    assert resp.status_code == 200
    assert resp.text == '"hello run2"'


def test_mutliplex_with_num_models_per_replica(serve_instance):
    """Test multiplex with num_models_per_replica.
    Make sure the model can be unloaded with LRU order.
    """

    @serve.deployment
    class Model:
        @serve.multiplexed(num_models_per_replica=2)
        def get_model(self, tag):
            return tag

        async def __call__(self, request):
            tag = serve.get_model_id()
            await self.get_model(tag)
            # return pid to check if the same model is used
            return os.getpid()

    handle = serve.run(Model.bind())
    headers = {RAY_SERVE_REQUEST_MODEL_ID: str(1)}
    requests.get("http://localhost:8000", headers=headers)
    headers = {RAY_SERVE_REQUEST_MODEL_ID: str(2)}
    requests.get("http://localhost:8000", headers=headers)
    # Wait for the model ids information to be updated in the router replica info
    wait_for_condition(
        check_model_id_in_replica_info, model_ids=["1", "2"], handle=handle, timeout=10
    )

    # Send model1 again to make sure model2 is unloaded
    headers = {RAY_SERVE_REQUEST_MODEL_ID: str(1)}
    requests.get("http://localhost:8000", headers=headers)
    headers = {RAY_SERVE_REQUEST_MODEL_ID: str(3)}
    requests.get("http://localhost:8000", headers=headers)
    wait_for_condition(
        check_model_id_in_replica_info, model_ids=["1", "3"], handle=handle, timeout=10
    )


def test_multiplex_func_outside_class(serve_instance):
    """Test multiplex with the function outside the class."""

    @serve.multiplexed
    def get_model(tag):
        return tag

    def run_once(app_name: str):
        @serve.deployment(num_replicas=2)
        class Model1:
            async def __call__(self, request):
                tag = serve.get_model_id()
                await get_model(tag)
                # return pid to check if the same model is used
                return os.getpid()

        handle = serve.run(Model1.bind(), name=app_name, route_prefix=f"/{app_name}")
        headers = {RAY_SERVE_REQUEST_MODEL_ID: str(1)}
        resp = requests.get(f"http://localhost:8000/{app_name}", headers=headers)
        # Wait for the model ids information to be updated in the router replica info
        wait_for_condition(
            check_model_id_in_replica_info, model_ids=["1"], handle=handle, timeout=10
        )

        # Run 10 times to make sure the same replica is used
        for _ in range(10):
            cur_resp = requests.get(
                f"http://localhost:8000/{app_name}", headers=headers
            )
            assert resp.text == cur_resp.text
        new_handle = handle.options(model_id="1")
        for _ in range(10):
            cur_resp = ray.get(new_handle.remote("blablabla"))
            assert resp.text == str(cur_resp)

    run_once("app")
    # Run a second app to make sure the multiplex function can be shared.
    run_once("app2")


def test_no_model_id_in_request(serve_instance):
    """Test multiplex with no model id in request."""

    @serve.deployment(num_replicas=2)
    class Model:
        @serve.multiplexed
        def get_model(self, tag):
            return tag

        async def __call__(self, request):
            tag = serve.get_model_id()
            await self.get_model(tag)
            # return pid to check if the same model is used
            return os.getpid()

    serve.run(Model.bind())
    resp = requests.get("http://localhost:8000")
    assert resp.status_code == 500


def test_model_load_in_multiple_replicas_by_qps(serve_instance):
    """Test model loaded to multiple replicas because of high qps"""

    signal = SignalActor.remote()

    @serve.deployment(num_replicas=2, max_concurrent_queries=1)
    class Model:
        @serve.multiplexed
        def get_model(self, tag):
            return tag

        async def __call__(self, request):
            tag = serve.get_model_id()
            await self.get_model(tag)
            await signal.wait.remote()
            # return pid to check if the same model is used
            return os.getpid()

    handle = serve.run(Model.bind())
    handle = handle.options(model_id="1")
    handle.remote("blablabla")
    # Second request should be served by the second replica
    handle.remote("blablabla")
    # Wait for the model ids information to be updated in the router replica info
    wait_for_condition(
        check_model_id_in_replica_info,
        model_ids=["1"],
        handle=handle,
        num_replicas=2,
        timeout=10,
    )
    signal.send.remote()


class TestBadMultiplexFunction:
    def test_multiplex_function_with_exception(serve_instance):
        @serve.deployment(num_replicas=2)
        class Model:
            @serve.multiplexed
            def get_model(self, tag):
                raise Exception("I can't load model")

            async def __call__(self, request):
                tag = serve.get_model_id()
                await self.get_model(tag)
                # return pid to check if the same model is used
                return os.getpid()

        serve.run(Model.bind())
        headers = {RAY_SERVE_REQUEST_MODEL_ID: str(1)}
        resp = requests.get("http://localhost:8000/", headers=headers)
        assert resp.status_code == 500

    def test_multiplex_function_without_param(serve_instance):
        @serve.deployment(num_replicas=2)
        class Model:
            @serve.multiplexed
            def get_model(self):
                """This function doesn't have param"""
                pass

            async def __call__(self, request):
                tag = serve.get_model_id()
                await self.get_model(tag)
                # return pid to check if the same model is used
                return os.getpid()

        serve.run(Model.bind())
        headers = {RAY_SERVE_REQUEST_MODEL_ID: str(1)}
        resp = requests.get("http://localhost:8000/", headers=headers)
        assert resp.status_code == 500

    def test_multiplex_function_bad_first_param(serve_instance):
        @serve.deployment(num_replicas=2)
        class Model:
            @serve.multiplexed
            def get_model(self, int_param):
                """This function doesn't string param"""
                pass

            async def __call__(self, request):
                # This should raise exception, since we pass int as first param.
                await self.get_model(1)
                return os.getpid()

        serve.run(Model.bind())
        headers = {RAY_SERVE_REQUEST_MODEL_ID: str(1)}
        resp = requests.get("http://localhost:8000/", headers=headers)
        assert resp.status_code == 500


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
