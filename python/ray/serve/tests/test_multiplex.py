import asyncio
import os
from typing import List

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._common.utils import get_or_create_event_loop
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import SERVE_MULTIPLEXED_MODEL_ID
from ray.serve._private.request_router import RequestRouter
from ray.serve.context import _get_internal_replica_context
from ray.serve.handle import DeploymentHandle
from ray.serve.multiplex import _ModelMultiplexWrapper


def _get_request_router(handle: DeploymentHandle) -> RequestRouter:
    # TODO(edoakes): we shouldn't be reaching into private fields, but better
    # to isolate it to one place (this function).
    return handle._router._asyncio_router._request_router


@pytest.fixture()
def start_serve_with_context():
    serve.start()
    ray.serve.context._set_internal_replica_context(
        replica_id=ReplicaID(
            "fake_replica_id",
            deployment_id=DeploymentID(name="fake_deployment", app_name="fake_app"),
        ),
        servable_object=None,
        _deployment_config=DeploymentConfig(),
        rank=0,
        world_size=1,
    )
    try:
        yield
    finally:
        serve.shutdown()
        ray.serve.context._set_request_context()
        ray.shutdown()


@pytest.mark.asyncio
class TestMultiplexWrapper:
    async def test_failed_to_get_replica_context(self):
        async def model_load_func(model_id: str):
            return model_id

        with pytest.raises(RuntimeError, match="can only be used within a deployment"):
            _ModelMultiplexWrapper(model_load_func, None, max_num_models_per_replica=2)

    async def test_push_model_ids_info(self, start_serve_with_context):
        async def model_load_func(model_id: str):
            return model_id

        multiplexer = _ModelMultiplexWrapper(
            model_load_func, None, max_num_models_per_replica=1
        )
        await multiplexer.metrics_pusher.graceful_shutdown()
        assert multiplexer._push_multiplexed_replica_info is False
        multiplexer._push_multiplexed_replica_info = True
        multiplexer._push_model_ids_info()
        assert multiplexer._push_multiplexed_replica_info is False

    async def test_collect_model_ids(self):
        multiplexer = _ModelMultiplexWrapper(None, None, max_num_models_per_replica=1)
        multiplexer.models = {"1": "1", "2": "2"}
        assert sorted(multiplexer._get_loading_and_loaded_model_ids()) == ["1", "2"]
        multiplexer._model_load_tasks = {"3"}
        assert sorted(multiplexer._get_loading_and_loaded_model_ids()) == [
            "1",
            "2",
            "3",
        ]

    async def test_multiplex_wrapper(self, start_serve_with_context):
        """Test multiplex wrapper with LRU caching."""

        async def model_load_func(model_id: str):
            return model_id

        multiplexer = _ModelMultiplexWrapper(
            model_load_func, None, max_num_models_per_replica=2
        )
        await multiplexer.metrics_pusher.graceful_shutdown()

        # Load model1
        await multiplexer.load_model("1")
        assert multiplexer.models == {"1": "1"}
        assert multiplexer._push_multiplexed_replica_info
        multiplexer._push_multiplexed_replica_info = False

        # Load model2
        await multiplexer.load_model("2")
        assert multiplexer.models == {"1": "1", "2": "2"}
        assert multiplexer._push_multiplexed_replica_info
        multiplexer._push_multiplexed_replica_info = False

        # Load model3, model1 should be unloaded
        await multiplexer.load_model("3")
        assert multiplexer.models == {"2": "2", "3": "3"}
        assert multiplexer._push_multiplexed_replica_info
        multiplexer._push_multiplexed_replica_info = False

        # reload model2, model2 should be moved to the end of the LRU cache
        # _push_multiplexed_replica_info should be False.
        await multiplexer.load_model("2")
        assert multiplexer.models == {"3": "3", "2": "2"}
        assert multiplexer._push_multiplexed_replica_info is False

        # Load model4, model3 should be unloaded
        await multiplexer.load_model("4")
        assert multiplexer._push_multiplexed_replica_info
        assert multiplexer.models == {"2": "2", "4": "4"}

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
        await multiplexer.metrics_pusher.graceful_shutdown()
        await multiplexer.load_model("1")
        assert multiplexer.models == {"1": MyModel("1")}
        with pytest.raises(Exception, match="1 is dead"):
            await multiplexer.load_model("2")

    async def test_push_model_ids_info_after_unload_model(self):
        """
        Push the model ids info right after the model is unloaded, even though
        new model is not loaded yet.
        """
        signal = SignalActor.remote()

        async def model_load_func(model_id: str):
            if model_id == "1":
                return model_id
            await signal.wait.remote()
            return

        multiplexer = _ModelMultiplexWrapper(
            model_load_func, None, max_num_models_per_replica=1
        )
        await multiplexer.metrics_pusher.graceful_shutdown()
        await multiplexer.load_model("1")
        assert multiplexer._push_multiplexed_replica_info
        multiplexer._push_multiplexed_replica_info = False

        loop = get_or_create_event_loop()
        loop.create_task(multiplexer.load_model("2"))
        # _push_multiplexed_replica_info is True right after model1 is unloaded.
        # and model2 is not finished loading.
        await asyncio.sleep(1)
        assert len(multiplexer.models) == 0
        assert "2" in multiplexer._model_load_tasks
        assert multiplexer._push_multiplexed_replica_info
        signal.send.remote()

    async def test_load_models_concurrently(self, start_serve_with_context):
        """
        Test load models concurrently. models info should include loading models and
        loaded models.
        And the models cache should not execeed the limit.
        """

        signal = SignalActor.remote()

        async def model_load_func(model_id: str):
            await signal.wait.remote()
            return

        multiplexer = _ModelMultiplexWrapper(
            model_load_func, None, max_num_models_per_replica=1
        )
        await multiplexer.metrics_pusher.graceful_shutdown()

        loop = get_or_create_event_loop()
        tasks = [
            loop.create_task(multiplexer.load_model("1")),
            loop.create_task(multiplexer.load_model("2")),
            loop.create_task(multiplexer.load_model("3")),
        ]
        await asyncio.sleep(1)
        assert len(multiplexer.models) == 0
        assert len(multiplexer._model_load_tasks) == len(tasks)
        assert multiplexer._push_multiplexed_replica_info
        signal.send.remote()
        done, _ = await asyncio.wait(tasks, timeout=1)
        assert len(done) == len(tasks)
        assert len(multiplexer.models) == 1
        assert "3" in multiplexer.models
        assert len(multiplexer._model_load_tasks) == 0


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
            ray.serve.context._RequestContext(multiplexed_model_id="1")
        )
        assert serve.get_multiplexed_model_id() == "1"


def test_request_routing_info(serve_instance):
    """Test RequestRoutingInfo is passed to the controller & router"""

    @serve.deployment
    class MyModel:
        @serve.multiplexed(max_num_models_per_replica=2)
        async def get_model(self, model_id: str):
            return

        async def __call__(self, model_id: str):
            _ = await self.get_model(model_id)
            return _get_internal_replica_context().replica_id

    handle = serve.run(MyModel.bind())
    replica_id = handle.remote("model1").result()

    def check_replica_information(
        model_ids: List[str],
    ):
        if not handle.is_initialized:
            handle._init()

        request_router = _get_request_router(handle)
        for replica in request_router.curr_replicas.values():
            if (
                replica.replica_id != replica_id
                or model_ids != replica.multiplexed_model_ids
            ):
                return False

        return True

    wait_for_condition(
        check_replica_information,
        model_ids={
            "model1",
        },
    )

    handle.remote("model2").result()
    wait_for_condition(
        check_replica_information,
        model_ids={
            "model1",
            "model2",
        },
    )

    # LRU remove the model1
    handle.remote("model3").result()
    wait_for_condition(
        check_replica_information,
        model_ids={
            "model2",
            "model3",
        },
    )


def check_model_id_in_replicas(handle: DeploymentHandle, model_id: str) -> bool:
    if not handle.is_initialized:
        handle._init()

    request_router = _get_request_router(handle)
    replica_to_model_ids = {
        tag: replica.multiplexed_model_ids
        for tag, replica in request_router.curr_replicas.items()
    }
    msg = (
        f"Model ID '{model_id}' not found in replica_to_model_ids: "
        f"{replica_to_model_ids}"
    )
    assert any(model_id in rep for rep in replica_to_model_ids.values()), msg
    return True


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
    resp = httpx.get("http://localhost:8000", headers=headers)
    initial_pid = resp.json()

    wait_for_condition(check_model_id_in_replicas, handle=handle, model_id=model_id)

    # Check that the same replica is used repeatedly for the same model_id.
    for _ in range(10):
        resp = httpx.get("http://localhost:8000", headers=headers)
        assert resp.json() == initial_pid

    for _ in range(10):
        assert (
            handle.options(multiplexed_model_id="1").remote("blabla").result()
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
    httpx.get("http://localhost:8000", headers=headers)
    headers = {SERVE_MULTIPLEXED_MODEL_ID: "2"}
    httpx.get("http://localhost:8000", headers=headers)
    # Make sure model2 will be evicted
    headers = {SERVE_MULTIPLEXED_MODEL_ID: "1"}
    httpx.get("http://localhost:8000", headers=headers)
    headers = {SERVE_MULTIPLEXED_MODEL_ID: "3"}
    httpx.get("http://localhost:8000", headers=headers)

    wait_for_condition(
        (
            lambda: check_model_id_in_replicas(handle, "1")
            and check_model_id_in_replicas(handle, "3")
        )
    )


def test_multiplexed_multiple_replicas(serve_instance):
    """Test multiplexed traffic can be sent to multiple replicas"""
    signal = SignalActor.remote()

    @serve.deployment(num_replicas=2, max_ongoing_requests=1)
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

    handle = serve.run(Model.bind()).options(multiplexed_model_id="1")

    # Each request should go to different replicas.
    pid1_ref = handle.remote()
    pid2_ref = handle.remote()
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 2)

    # Unblock both requests to finish.
    ray.get(signal.send.remote())
    assert pid1_ref.result() != pid2_ref.result()

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
        def __init__(self, downstream: DeploymentHandle):
            self._h = downstream

        async def __call__(self):
            model_id_before = serve.get_multiplexed_model_id()

            # Make a call with another model ID, verify it's set properly.
            other_model_id = await self._h.options(multiplexed_model_id="bar").remote()
            assert other_model_id == "bar"

            # Model ID shouldn't change after the handle call.
            model_id_after = serve.get_multiplexed_model_id()
            assert model_id_before == model_id_after

            return model_id_before

    handle = serve.run(Upstream.bind(Downstream.bind()))
    assert handle.options(multiplexed_model_id="foo").remote().result() == "foo"


def test_replica_upgrade_to_cleanup_resource(serve_instance):
    """When replica is upgraded, we need to make sure model resources are released."""

    @serve.deployment
    class Recorder:
        def __init__(self):
            self.call_record = set()

        def add(self, model_id):
            self.call_record.add(model_id)

        def get_call_record(self):
            return self.call_record

    record_handle = serve.run(
        Recorder.bind(), name="recorder", route_prefix="/recorder"
    )

    class MyModel:
        def __init__(self, model_id, record_handle):
            self.model_id = model_id
            self.record_handle = record_handle

        async def __del__(self):
            await self.record_handle.add.remote(self.model_id)

        def __eq__(self, model):
            return model.model_id == self.model_id

    @serve.deployment(num_replicas=1)
    class Model:
        def __init__(self, record_handle):
            self.record_handle = record_handle

        @serve.multiplexed(max_num_models_per_replica=1)
        async def get_model(self, tag):
            return MyModel(tag, self.record_handle)

        async def __call__(self, request):
            tag = serve.get_multiplexed_model_id()
            await self.get_model(tag)
            # return pid to check if the same model is used
            return os.getpid()

    serve.run(Model.bind(record_handle))

    model_id = "1"
    headers = {"serve_multiplexed_model_id": model_id}
    httpx.get("http://localhost:8000", headers=headers)
    assert record_handle.get_call_record.remote().result() == set()
    serve.run(Model.bind(record_handle))
    assert record_handle.get_call_record.remote().result() == {"1"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
