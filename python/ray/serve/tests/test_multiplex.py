import pytest
import requests

from ray import serve
import ray
from ray.serve._private.constants import RAY_SERVE_REQUEST_MODEL_ID
from fastapi import FastAPI
import starlette
from typing import List
from ray.serve.multiplex import _ModelMultiplexWrapper
from fastapi.responses import PlainTextResponse


def check_model_id_in_replica_info(model_ids: List[str], handle, num_replicas=1):
    """Check if the model ids are in the replica info"""
    count = 0
    for replica in handle.router._replica_set.in_flight_queries:
        if model_ids == list(replica.model_ids):
            count += 1
            if count == num_replicas:
                return True
    return False


class TestMultiplexWrapper:
    @pytest.mark.asyncio
    async def test_multiplex_wrapper(self):
        """Test multiplex wrapper with LRU caching."""

        async def model_load_func(model_id: str):
            return model_id

        multiplexer = _ModelMultiplexWrapper(
            model_load_func, None, max_num_models_per_replica=2
        )
        # Load model1
        await multiplexer.load_model("1")
        assert multiplexer.models == {"1": "1"}
        # Load model2
        await multiplexer.load_model("2")
        assert multiplexer.models == {"1": "1", "2": "2"}

        # Load model3, model1 should be unloaded
        await multiplexer.load_model("3")
        assert multiplexer.models == {"2": "2", "3": "3"}

        # reload model2, model2 should be moved to the end of the LRU cache
        await multiplexer.load_model("2")
        assert multiplexer.models == {"3": "3", "2": "2"}

        # Load model4, model3 should be unloaded
        await multiplexer.load_model("4")
        assert multiplexer.models == {"2": "2", "4": "4"}

    @pytest.mark.asyncio
    async def test_non_string_type_model_id(self):
        """Test multiplex wrapper with non-string type model id."""

        async def model_load_func(model_id: str):
            return model_id

        multiplexer = _ModelMultiplexWrapper(
            model_load_func, None, max_num_models_per_replica=2
        )
        with pytest.raises(TypeError):
            await multiplexer.load_model(1)

    @pytest.mark.asyncio
    async def test_unload_model_call_del(self):
        class MyModel:
            def __init__(self, model_id):
                self.model_id = model_id

            def __del__(self):
                raise Exception(f"{self.model_id} is dead")

            def __eq__(self, model):
                return model.model_id == self.model_id

        async def model_load_func(model_id: str) -> MyModel:
            return MyModel(model_id)

        multiplexer = _ModelMultiplexWrapper(
            model_load_func, None, max_num_models_per_replica=1
        )
        await multiplexer.load_model("1")
        assert multiplexer.models == {"1": MyModel("1")}
        with pytest.raises(Exception, match="1 is dead"):
            await multiplexer.load_model("2")


class TestBasicAPI:
    def send_request_and_verify_output(self, url="/"):
        for model_id in range(2):
            headers = {RAY_SERVE_REQUEST_MODEL_ID: str(model_id)}
            resp = requests.get(f"http://localhost:8000{url}", headers=headers)
            assert resp.status_code == 200
            assert resp.text == str(model_id)

    def test_basic_api(self, serve_instance):
        """Test basic multiplex functionality."""

        @serve.deployment
        class Model:
            @serve.multiplexed
            async def get_model(self, tag):
                return tag

            async def __call__(self, request):
                model_id = ray.serve.get_model_id()
                return await self.get_model(model_id)

        handle = serve.run(Model.bind())
        self.send_request_and_verify_output()
        resp = ray.get(handle.options(model_id=str(1)).remote("blablabla"))
        assert resp == "1"

    def test_fastapi(self, serve_instance):
        """Test basic multiplex functionality with FastAPI."""

        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class Model:
            @serve.multiplexed
            async def get_model(self, tag):
                return tag

            @app.get("/", response_class=PlainTextResponse)
            async def run(self, request: starlette.requests.Request):
                model_id = ray.serve.get_model_id()
                return await self.get_model(model_id)

            @app.get("/call", response_class=PlainTextResponse)
            async def call(self, request: starlette.requests.Request):
                model_id = ray.serve.get_model_id()
                return await self.get_model(model_id)

        serve.run(Model.bind())
        self.send_request_and_verify_output()
        self.send_request_and_verify_output(url="/call")

    def test_without_model_id(self, serve_instance):
        """Test http request without model id."""

        @serve.deployment
        class Model:
            @serve.multiplexed
            async def get_model(self, tag):
                return tag

            async def __call__(self, request):
                model_id = ray.serve.get_model_id()
                return await self.get_model(model_id)

        serve.run(Model.bind())
        resp = requests.get("http://localhost:8000")
        assert resp.status_code == 500


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
