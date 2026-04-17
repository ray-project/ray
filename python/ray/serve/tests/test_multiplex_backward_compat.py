"""Backward-compatibility tests for the deprecated multiplex APIs.

Mirrors the user-facing end-to-end tests in ``test_multiplex.py`` but uses
only the deprecated surfaces that the shims in ``api.py`` / ``handle.py`` /
``context.py`` still support:

* ``@serve.multiplexed`` bare decorator and ``max_num_models_per_replica=N``.
* ``serve.get_multiplexed_model_id()``.
* ``handle.options(multiplexed_model_id=...)``.

TODO (jeffreywang): Remove this test suite in Ray 2.58.
"""

import os
from typing import List

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.constants import SERVE_MULTIPLEXED_MODEL_ID
from ray.serve._private.request_router import RequestRouter
from ray.serve.handle import DeploymentHandle


def _get_request_router(handle: DeploymentHandle) -> RequestRouter:
    return handle._router._asyncio_router._request_router


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

    def test_get_multiplexed_id(self):
        assert serve.get_multiplexed_model_id() == ""
        ray.serve.context._serve_request_context.set(
            ray.serve.context._RequestContext(multiplex_ids={"model": "1"})
        )
        assert serve.get_multiplexed_model_id() == "1"


def test_request_routing_info(serve_instance):
    """Test RequestRoutingInfo is passed to the controller & router using
    the deprecated ``max_num_models_per_replica`` decorator + legacy getter.
    """

    @serve.deployment
    class MyModel:
        @serve.multiplexed(max_num_models_per_replica=2)
        async def get_model(self, model_id: str):
            return

        async def __call__(self, model_id: str):
            _ = await self.get_model(model_id)
            return ray.serve.context._get_internal_replica_context().replica_id

    handle = serve.run(MyModel.bind())
    replica_id = handle.remote("model1").result()

    def check_replica_information(
        model_ids: List[str],
    ):
        if not handle.is_initialized:
            handle._init()

        request_router = _get_request_router(handle)
        for replica in request_router.curr_replicas.values():
            replica_model_ids = replica.multiplex_dim_to_ids.get("model", set())
            if replica.replica_id != replica_id or model_ids != replica_model_ids:
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


def _check_model_id_in_replicas(handle: DeploymentHandle, model_id: str) -> bool:
    if not handle.is_initialized:
        handle._init()

    request_router = _get_request_router(handle)
    replica_to_model_ids = {
        tag: replica.multiplex_dim_to_ids.get("model", set())
        for tag, replica in request_router.curr_replicas.items()
    }
    msg = (
        f"Model ID '{model_id}' not found in replica_to_model_ids: "
        f"{replica_to_model_ids}"
    )
    assert any(model_id in rep for rep in replica_to_model_ids.values()), msg
    return True


def test_multiplexed_e2e(serve_instance):
    """Test multiplexed function end to end using the deprecated API."""

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

    wait_for_condition(_check_model_id_in_replicas, handle=handle, model_id=model_id)

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
    """Test multiplexed function LRU policy using the deprecated API."""

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
            lambda: (
                _check_model_id_in_replicas(handle, "1")
                and _check_model_id_in_replicas(handle, "3")
            )
        )
    )


def test_multiplexed_multiple_replicas(serve_instance):
    """Test multiplexed traffic can be sent to multiple replicas using
    the deprecated handle option.
    """
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

    wait_for_condition(_check_model_id_in_replicas, handle=handle, model_id="1")


def test_setting_model_id_on_handle_does_not_set_it_locally(serve_instance):
    """
    Verify that `.options(multiplexed_model_id="foo")` on a ServeHandle sets
    it in the downstream but does not update the model ID in the caller.
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
    """When replica is upgraded, model resources must be released — using
    the deprecated decorator + getter.
    """

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


def test_multiplexed_with_batching_splits_by_model_id(serve_instance):
    """Test that batching with multiplexing splits batches by model ID
    using the deprecated API.
    """

    @serve.deployment(num_replicas=1, max_ongoing_requests=20)
    class BatchedMultiplexModel:
        def __init__(self):
            self.batch_info = []

        @serve.multiplexed(max_num_models_per_replica=3)
        async def get_model(self, model_id: str):
            return model_id

        @serve.batch(max_batch_size=10, batch_wait_timeout_s=1.0)
        async def batched_predict(self, inputs: List[str]):
            # Get the model ID from the request context
            model_id = serve.get_multiplexed_model_id()

            # Record the batch info for verification
            batch_size = len(inputs)
            self.batch_info.append(
                {
                    "model_id": model_id,
                    "batch_size": batch_size,
                    "inputs": inputs,
                }
            )

            # Load the model (would fail if different model_ids were in same batch)
            model = await self.get_model(model_id)

            # Return results
            return [f"{model}:{inp}" for inp in inputs]

        async def __call__(self, request):
            return await self.batched_predict(request)

        def get_batch_info(self):
            return self.batch_info

    handle = serve.run(BatchedMultiplexModel.bind())

    # Send concurrent requests with different model IDs
    # If batching doesn't split by model_id, requests for different models
    # would end up in the same batch, which would be incorrect.
    refs = []
    for i in range(6):
        # Alternate between model_a and model_b
        model_id = "model_a" if i % 2 == 0 else "model_b"
        refs.append(handle.options(multiplexed_model_id=model_id).remote(f"input_{i}"))

    # Wait for all results
    results = [ref.result() for ref in refs]

    # Verify results are correct - each result should have the correct model prefix
    for i, result in enumerate(results):
        expected_model = "model_a" if i % 2 == 0 else "model_b"
        assert result.startswith(
            f"{expected_model}:"
        ), f"Expected result to start with '{expected_model}:', got '{result}'"
        assert f"input_{i}" in result

    # Verify batch info - each batch should only contain requests for one model
    batch_info = handle.get_batch_info.remote().result()
    for batch in batch_info:
        # Each batch should have a non-empty model_id
        # (all requests in batch have the same model_id)
        assert batch["model_id"] in [
            "model_a",
            "model_b",
        ], f"Unexpected model_id in batch: {batch['model_id']}"
        # Batch size should be > 0
        assert batch["batch_size"] == 3

    # Verify total requests processed equals what we sent
    total_processed = sum(b["batch_size"] for b in batch_info)
    assert total_processed == 6, f"Expected 6 requests processed, got {total_processed}"
    assert len(batch_info) == 2


def test_multiplexed_with_batching_same_model_batches_together(serve_instance):
    """Test that requests for the same model are batched together — using
    the deprecated handle option + getter.
    """
    signal = SignalActor.remote()

    @serve.deployment(num_replicas=1, max_ongoing_requests=20)
    class BatchedModel:
        def __init__(self):
            self.batch_sizes = []

        @serve.batch(max_batch_size=10, batch_wait_timeout_s=1.0)
        async def batched_predict(self, inputs: List[str]):
            model_id = serve.get_multiplexed_model_id()
            self.batch_sizes.append((model_id, len(inputs)))
            await signal.wait.remote()
            return [f"{model_id}:{inp}" for inp in inputs]

        async def __call__(self, request):
            return await self.batched_predict(request)

        def get_batch_sizes(self):
            return self.batch_sizes

    handle = serve.run(BatchedModel.bind())

    # Send multiple requests for the same model - they should batch together
    refs = []
    for i in range(5):
        refs.append(
            handle.options(multiplexed_model_id="same_model").remote(f"input_{i}")
        )

    # Wait for the batch to form
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 1)

    # Unblock processing
    ray.get(signal.send.remote())

    # Wait for results
    results = [ref.result() for ref in refs]
    assert len(results) == 5

    # Check batch sizes - all requests should have been in one batch
    batch_sizes = handle.get_batch_sizes.remote().result()
    total_in_batches = sum(size for _, size in batch_sizes)
    assert total_in_batches == 5

    # All batches should be for the same model
    for model_id, _ in batch_sizes:
        assert model_id == "same_model"

    assert len(batch_sizes) == 1


def test_multiplexed_batching_concurrent_subbatches_context_isolation(serve_instance):
    # Two signals for two-phase synchronization
    signal_barrier = SignalActor.remote()

    @serve.deployment(num_replicas=1, max_ongoing_requests=100)
    class ConcurrentBatchedModel:
        def __init__(self):
            self.model_id_readings = []

        @serve.multiplexed(max_num_models_per_replica=5)
        async def get_model(self, model_id: str):
            return model_id

        @serve.batch(max_batch_size=10, batch_wait_timeout_s=1.0)
        async def batched_predict(self, inputs: List[str]):
            # Phase 1: Wait at the barrier.
            await signal_barrier.wait.remote()

            # Phase 2: NOW read the model_id.
            model_id_read = serve.get_multiplexed_model_id()

            # Record for verification
            self.model_id_readings.append(
                {
                    "model_id": model_id_read,
                    "batch_size": len(inputs),
                    "inputs": inputs,
                }
            )

            return [f"{model_id_read}:{inp}" for inp in inputs]

        async def __call__(self, request):
            return await self.batched_predict(request)

        def get_model_id_readings(self):
            return self.model_id_readings

    handle = serve.run(ConcurrentBatchedModel.bind())

    # Send concurrent requests with different model IDs.
    # These will be split into separate sub-batches and processed concurrently.
    refs = []
    model_ids = ["model_a", "model_b", "model_c"]
    requests_per_model = 3

    for model_id in model_ids:
        for i in range(requests_per_model):
            refs.append(
                handle.options(multiplexed_model_id=model_id).remote(
                    f"{model_id}_input_{i}"
                )
            )

    # Wait for all sub-batches to be at the barrier
    wait_for_condition(
        lambda: ray.get(signal_barrier.cur_num_waiters.remote()) == len(model_ids)
    )

    # Release all sub-batches to read their model_id
    ray.get(signal_barrier.send.remote())

    # Collect results
    results = [ref.result() for ref in refs]

    # Verify each result has the correct model prefix
    # With the bug, all results might have the same (wrong) model prefix
    for i, result in enumerate(results):
        expected_model = model_ids[i // requests_per_model]
        assert result.startswith(f"{expected_model}:"), (
            f"Expected result to start with '{expected_model}:', got '{result}'. "
            "This indicates context isolation failure - a sub-batch read another "
            "sub-batch's model_id because they share the same context."
        )

    # Verify model ID readings
    readings = handle.get_model_id_readings.remote().result()

    # Count how many different model_ids were read
    read_model_ids = {r["model_id"] for r in readings}

    # With the bug: all sub-batches read the same model_id (only 1 unique)
    # With the fix: each sub-batch reads its own model_id (3 unique)
    assert len(read_model_ids) == len(model_ids), (
        f"Expected {len(model_ids)} different model_ids to be read, but got "
        f"{len(read_model_ids)}: {read_model_ids}. "
        f"This indicates context isolation failure - multiple sub-batches "
        f"read the same model_id because they share context. "
        f"Full readings: {readings}"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
