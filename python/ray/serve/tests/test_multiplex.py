import pytest
from typing import List
import os
import requests

import ray
from ray._private.test_utils import (
    async_wait_for_condition,
    wait_for_condition,
    SignalActor,
)

from ray import serve
from ray.serve.context import get_internal_replica_context
from ray.serve.handle import RayServeHandle
from ray.serve.multiplex import _ModelMultiplexWrapper
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_NEW_ROUTING,
    SERVE_MULTIPLEXED_MODEL_ID,
)


@pytest.fixture()
def start_serve_with_context():
    serve.start()
    ray.serve.context._set_internal_replica_context(
        "fake_deployment", "fake_replica_tag", None, None, None
    )
    yield
    serve.shutdown()
    ray.serve.context._set_request_context()
    ray.shutdown()


class TestMultiplexWrapper:
    def test_failed_to_get_replica_context(self):
        async def model_load_func(model_id: str):
            return model_id

        with pytest.raises(
            RuntimeError, match="Fail to retrieve serve replica context"
        ):
            _ModelMultiplexWrapper(model_load_func, None, max_num_models_per_replica=2)

    @pytest.mark.asyncio
    async def test_multiplex_wrapper(self, start_serve_with_context):
        """Test multiplex wrapper with LRU caching."""

        async def model_load_func(model_id: str):
            return model_id

        multiplexer = _ModelMultiplexWrapper(
            model_load_func, None, max_num_models_per_replica=2
        )

        # Check the replica info pushed
        def check_info_pushed():
            return multiplexer._push_multiplexed_replica_info is False

        # Load model1
        await multiplexer.load_model("1")
        assert multiplexer.models == {"1": "1"}
        assert multiplexer._push_multiplexed_replica_info
        await async_wait_for_condition(check_info_pushed)

        # Load model2
        await multiplexer.load_model("2")
        assert multiplexer.models == {"1": "1", "2": "2"}
        assert multiplexer._push_multiplexed_replica_info
        await async_wait_for_condition(check_info_pushed)

        # Load model3, model1 should be unloaded
        await multiplexer.load_model("3")
        assert multiplexer.models == {"2": "2", "3": "3"}
        assert multiplexer._push_multiplexed_replica_info
        await async_wait_for_condition(check_info_pushed)

        # reload model2, model2 should be moved to the end of the LRU cache
        # _push_multiplexed_replica_info should be False.
        await multiplexer.load_model("2")
        assert multiplexer.models == {"3": "3", "2": "2"}
        assert multiplexer._push_multiplexed_replica_info is False

        # Load model4, model3 should be unloaded
        await multiplexer.load_model("4")
        assert multiplexer._push_multiplexed_replica_info
        assert multiplexer.models == {"2": "2", "4": "4"}

    @pytest.mark.asyncio
    async def test_bad_call_multiplexed_func(self, start_serve_with_context):
        """Test bad call to multiplexed function"""

        async def model_load_func(model_id: str):
            return model_id

        multiplexer = _ModelMultiplexWrapper(
            model_load_func, None, max_num_models_per_replica=2
        )
        with pytest.raises(TypeError):
            await multiplexer.load_model(1)
        with pytest.raises(TypeError):
            await multiplexer.load_model()

    @pytest.mark.asyncio
    async def test_unload_model_call_del(self, start_serve_with_context):
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
    def test_decorator_validation(self):
        @serve.multiplexed
        async def get_model(model: str):
            return

        @serve.multiplexed(max_num_models_per_replica=1)
        async def get_model2(model: str):
            return

        @serve.deployment
        class MyModel:
            @serve.multiplexed
            async def get_model(model: str):
                return

        @serve.deployment
        class MyModel2:
            @serve.multiplexed(max_num_models_per_replica=1)
            async def get_model(self, model: str):
                return

        # multiplex can only be used with func or method.
        with pytest.raises(TypeError):

            @serve.deployment
            @serve.multiplexed
            class BadDecorator:
                pass

        # max_num_models_per_replica must be an integer
        with pytest.raises(TypeError):

            @serve.multiplexed(max_num_models_per_replica="1")
            async def get_model3(model: str):
                pass

        # max_num_models_per_replica must be positive
        with pytest.raises(ValueError):

            @serve.multiplexed(max_num_models_per_replica=0)
            async def get_model4(model: str):
                pass

        # multiplexed function must be async def
        with pytest.raises(TypeError):

            @serve.multiplexed
            def get_model5(model: str):
                pass

        with pytest.raises(TypeError):

            @serve.deployment
            class MyModel3:
                @serve.multiplexed
                def get_model(self, model: str):
                    return

        # no model_id argument in multiplexed function
        with pytest.raises(TypeError):

            @serve.multiplexed
            def get_model6():
                pass

        with pytest.raises(TypeError):

            @serve.deployment
            class MyModel4:
                @serve.multiplexed
                def get_model(self):
                    return

    def test_get_multiplexed_model_id(self):
        """Test get_multiplexed_model_id() API"""
        assert serve.get_multiplexed_model_id() == ""
        ray.serve.context._serve_request_context.set(
            ray.serve.context.RequestContext(multiplexed_model_id="1")
        )
        assert serve.get_multiplexed_model_id() == "1"


def test_multiplexed_replica_info(serve_instance):
    """Test MultiplexedReplicaInfo is passed to the controller & router"""

    @serve.deployment
    class MyModel:
        @serve.multiplexed(max_num_models_per_replica=2)
        async def get_model(self, model_id: str):
            return

        async def __call__(self, model_id: str):
            _ = await self.get_model(model_id)
            context = get_internal_replica_context()
            return context.replica_tag

    handle = serve.run(MyModel.bind())
    replica_tag = ray.get(handle.remote("model1"))

    def check_replica_information(
        model_ids: List[str],
    ):
        replica_scheduler = handle._get_or_create_router()._replica_scheduler
        if RAY_SERVE_ENABLE_NEW_ROUTING:
            for replica in replica_scheduler.curr_replicas.values():
                if (
                    replica.replica_id != replica_tag
                    or model_ids != replica.multiplexed_model_ids
                ):
                    return False
        else:
            for replica in replica_scheduler.in_flight_queries.keys():
                if replica.replica_tag != replica_tag or model_ids != set(
                    replica.multiplexed_model_ids
                ):
                    return False

        return True

    wait_for_condition(
        check_replica_information,
        model_ids={
            "model1",
        },
    )

    ray.get(handle.remote("model2"))
    wait_for_condition(
        check_replica_information,
        model_ids={
            "model1",
            "model2",
        },
    )

    # LRU remove the model1
    ray.get(handle.remote("model3"))
    wait_for_condition(
        check_replica_information,
        model_ids={
            "model2",
            "model3",
        },
    )


def check_model_id_in_replicas(handle: RayServeHandle, model_id: str) -> bool:
    replica_scheduler = handle._get_or_create_router()._replica_scheduler
    if RAY_SERVE_ENABLE_NEW_ROUTING:
        for replica in replica_scheduler.curr_replicas.values():
            if model_id in replica.multiplexed_model_ids:
                return True

        return False
    else:
        return model_id in replica_scheduler.multiplexed_replicas_table


def test_multiplexed_e2e(serve_instance):
    """Test multiplexed function end to end"""

    @serve.deployment(num_replicas=2)
    class Model:
        @serve.multiplexed(max_num_models_per_replica=1)
        async def get_model(self, tag):
            return tag

        async def __call__(self, request):
            tag = serve.get_multiplexed_model_id()
            await self.get_model(tag)
            # return pid to check if the same model is used
            return os.getpid()

    model_id = "1"
    handle = serve.run(Model.bind())
    headers = {SERVE_MULTIPLEXED_MODEL_ID: model_id}
    resp = requests.get("http://localhost:8000", headers=headers)
    initial_pid = resp.json()

    wait_for_condition(check_model_id_in_replicas, handle=handle, model_id=model_id)

    # Check that the same replica is used repeatedly for the same model_id.
    for _ in range(10):
        resp = requests.get("http://localhost:8000", headers=headers)
        assert resp.json() == initial_pid

    for _ in range(10):
        assert (
            ray.get(handle.options(multiplexed_model_id="1").remote("blabla"))
            == initial_pid
        )


def test_multiplexed_lru_policy(serve_instance):
    """Test multiplexed function LRU policy"""

    @serve.deployment
    class Model:
        @serve.multiplexed(max_num_models_per_replica=2)
        async def get_model(self, tag):
            return tag

        async def __call__(self, request):
            tag = serve.get_multiplexed_model_id()
            await self.get_model(tag)
            # return pid to check if the same model is used
            return os.getpid()

    handle = serve.run(Model.bind())
    headers = {SERVE_MULTIPLEXED_MODEL_ID: "1"}
    requests.get("http://localhost:8000", headers=headers)
    headers = {SERVE_MULTIPLEXED_MODEL_ID: "2"}
    requests.get("http://localhost:8000", headers=headers)
    # Make sure model2 will be evicted
    headers = {SERVE_MULTIPLEXED_MODEL_ID: "1"}
    requests.get("http://localhost:8000", headers=headers)
    headers = {SERVE_MULTIPLEXED_MODEL_ID: "3"}
    requests.get("http://localhost:8000", headers=headers)

    wait_for_condition(
        (
            lambda: check_model_id_in_replicas(handle, "1")
            and check_model_id_in_replicas(handle, "3")
        )
    )


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_NEW_ROUTING,
    reason="Old routing not enforcing `max_concurrent_queries` properly.",
)
def test_multiplexed_multiple_replicas(serve_instance):
    """Test multiplexed traffic can be sent to multiple replicas"""
    signal = SignalActor.remote()

    @serve.deployment(num_replicas=2, max_concurrent_queries=1)
    class Model:
        @serve.multiplexed(max_num_models_per_replica=2)
        async def get_model(self, tag):
            return tag

        async def __call__(self):
            tag = serve.get_multiplexed_model_id()
            await self.get_model(tag)
            await signal.wait.remote()
            # return pid to check if the same model is used
            return os.getpid()

    handle = serve.run(Model.bind())
    pid1_ref = handle.options(multiplexed_model_id="1").remote()
    # Second request should be sent to the second replica
    pid2_ref = handle.options(multiplexed_model_id="1").remote()
    signal.send.remote()
    assert ray.get(pid1_ref) != ray.get(pid2_ref)

    wait_for_condition(check_model_id_in_replicas, handle=handle, model_id="1")


def test_setting_model_id_on_handle_does_not_set_it_locally(serve_instance):
    """
    Verify that `.options(multiplexed_model_id="foo")` on a ServeHandle sets it in the
    downstream but does not update the model ID in the caller.
    """

    @serve.deployment
    class Downstream:
        def __call__(self):
            return serve.get_multiplexed_model_id()

    @serve.deployment
    class Upstream:
        def __init__(self, downstream: RayServeHandle):
            self._h = downstream

        async def __call__(self):
            model_id_before = serve.get_multiplexed_model_id()

            # Make a call with another model ID, verify it's set properly.
            ref = await self._h.options(multiplexed_model_id="bar").remote()
            assert await ref == "bar"

            # Model ID shouldn't change after the handle call.
            model_id_after = serve.get_multiplexed_model_id()
            assert model_id_before == model_id_after

            return model_id_before

    handle = serve.run(Upstream.bind(Downstream.bind()))
    assert ray.get(handle.options(multiplexed_model_id="foo").remote()) == "foo"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
