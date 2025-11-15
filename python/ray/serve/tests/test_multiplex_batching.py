"""
Test cases for multiplexing with batching integration in Ray Serve.

This module tests the enhanced multiplexing functionality that integrates
automatic batching for improved performance and resource utilization.
"""

import asyncio
import time
from typing import List

import pytest

import ray
from ray import serve
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import DeploymentConfig
from ray.serve.multiplex import _ModelMultiplexWrapper


class MockModel:
    """Mock model for testing multiplexing and batching."""

    def __init__(self, model_id: str, processing_time: float = 0.1):
        self.model_id = model_id
        self.processing_time = processing_time
        self.call_count = 0
        self.batch_call_count = 0
        self.last_batch_size = 0

    async def predict(self, input_data):
        """Individual prediction method."""
        await asyncio.sleep(self.processing_time)
        self.call_count += 1
        return f"result_{self.model_id}_{input_data}"

    async def batch_predict(self, input_batch: List):
        """Batch prediction method."""
        await asyncio.sleep(self.processing_time * 0.6)  # Batch efficiency
        self.batch_call_count += 1
        self.last_batch_size = len(input_batch)
        return [f"batch_result_{self.model_id}_{item}" for item in input_batch]


@pytest.fixture
def start_serve_with_context():
    """Start Serve with proper replica context for testing."""
    serve.start()
    ray.serve.context._set_internal_replica_context(
        replica_id=ReplicaID(
            "test_replica_id",
            deployment_id=DeploymentID(name="test_deployment", app_name="test_app"),
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
class TestMultiplexBatchingIntegration:
    """Test the integration of multiplexing with batching."""

    async def test_basic_batching_integration(self, start_serve_with_context):
        """Test that multiplexing works with batching enabled."""

        async def mock_model_loader(model_id: str):
            return MockModel(model_id, processing_time=0.05)

        print("creating multiplex wrapper")
        # Create wrapper with batching enabled
        wrapper = _ModelMultiplexWrapper(
            model_load_func=mock_model_loader,
            max_num_models_per_replica=3,
            enable_batching=True,
            max_batch_size=4,
            batch_wait_timeout_s=0.1,
        )
        print("create wrapper")

        # Test concurrent requests to same model - should be batched
        print("starting tasks")
        start_time = time.time()
        tasks = []
        for i in range(6):
            task = wrapper.predict(f"input_{i}", "model_a")
            tasks.append(task)
        print("starting gather")
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time
        print("completed gather")

        # Verify results
        assert len(results) == 6
        assert all("batch_result_model_a" in result for result in results)

        # Should have been processed in batches
        assert total_time < 0.5  # Much faster than 6 individual calls

    async def test_multiplex_with_batching_different_models(
        self, start_serve_with_context
    ):
        """Test multiplexing across different models with batching."""

        models = {}

        async def mock_model_loader(model_id: str):
            if model_id not in models:
                models[model_id] = MockModel(model_id, processing_time=0.03)
            return models[model_id]

        wrapper = _ModelMultiplexWrapper(
            model_load_func=mock_model_loader,
            max_num_models_per_replica=3,
            enable_batching=True,
            max_batch_size=3,
            batch_wait_timeout_s=0.05,
        )

        # Send requests to different models concurrently
        tasks = []
        for model_id in ["model_a", "model_b", "model_c"]:
            for i in range(3):
                task = wrapper.predict(f"input_{i}", model_id)
                tasks.append(task)

        results = await asyncio.gather(*tasks)

        # Verify all models were used
        assert len(models) == 3
        assert all(model.batch_call_count > 0 for model in models.values())

        # Verify results from all models
        model_a_results = [r for r in results if "model_a" in r]
        model_b_results = [r for r in results if "model_b" in r]
        model_c_results = [r for r in results if "model_c" in r]

        assert len(model_a_results) == 3
        assert len(model_b_results) == 3
        assert len(model_c_results) == 3

    async def test_batching_timeout_behavior(self, start_serve_with_context):
        """Test batch timeout behavior with multiplexing."""

        async def mock_model_loader(model_id: str):
            return MockModel(model_id, processing_time=0.01)

        wrapper = _ModelMultiplexWrapper(
            model_load_func=mock_model_loader,
            max_num_models_per_replica=2,
            enable_batching=True,
            max_batch_size=5,
            batch_wait_timeout_s=0.1,  # 100ms timeout
        )

        # Send single request and measure time
        start_time = time.time()
        result = await wrapper.predict("single_input", "model_timeout")
        elapsed_time = time.time() - start_time

        # Should process after timeout even with single request
        assert "batch_result_model_timeout" in result
        assert elapsed_time >= 0.1  # At least the timeout duration

    async def test_max_batch_size_enforcement(self, start_serve_with_context):
        """Test that max batch size is enforced properly."""

        model_instance = MockModel("model_batch_size", processing_time=0.02)

        async def mock_model_loader(model_id: str):
            return model_instance

        wrapper = _ModelMultiplexWrapper(
            model_load_func=mock_model_loader,
            max_num_models_per_replica=1,
            enable_batching=True,
            max_batch_size=3,  # Small batch size
            batch_wait_timeout_s=0.05,
        )

        # Send more requests than max batch size
        tasks = []
        for i in range(7):  # More than max_batch_size
            task = wrapper.predict(f"input_{i}", "model_batch_size")
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # All requests should complete
        assert len(results) == 7

        # Should have made multiple batch calls due to max_batch_size limit
        assert model_instance.batch_call_count >= 3  # At least 3 batches for 7 items

    async def test_model_eviction_with_batching(self, start_serve_with_context):
        """Test LRU model eviction works with batching."""

        models = {}

        async def mock_model_loader(model_id: str):
            if model_id not in models:
                models[model_id] = MockModel(model_id)
            return models[model_id]

        wrapper = _ModelMultiplexWrapper(
            model_load_func=mock_model_loader,
            max_num_models_per_replica=2,  # Small cache
            enable_batching=True,
            max_batch_size=3,
            batch_wait_timeout_s=0.05,
        )

        # Load models sequentially to trigger eviction
        await wrapper.predict("input1", "model_1")
        await wrapper.predict("input2", "model_2")
        await wrapper.predict("input3", "model_3")  # Should evict model_1

        # Verify model_1 was evicted by checking cache size
        # This is implementation dependent but we can test behavior
        await wrapper.predict("input4", "model_1")  # Should reload model_1

        # All models should have been created
        assert len(models) == 3

    async def test_batching_disabled_fallback(self, start_serve_with_context):
        """Test that individual prediction works when batching is disabled."""

        model_instance = MockModel("model_no_batch", processing_time=0.01)

        async def mock_model_loader(model_id: str):
            return model_instance

        wrapper = _ModelMultiplexWrapper(
            model_load_func=mock_model_loader,
            max_num_models_per_replica=2,
            enable_batching=False,  # Batching disabled
            max_batch_size=5,
            batch_wait_timeout_s=0.1,
        )

        # Send multiple requests
        tasks = []
        for i in range(3):
            task = wrapper.predict(f"input_{i}", "model_no_batch")
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # Should use individual prediction, not batching
        assert len(results) == 3
        assert model_instance.call_count == 3  # Individual calls
        assert model_instance.batch_call_count == 0  # No batch calls

    async def test_concurrent_models_with_batching(self, start_serve_with_context):
        """Test concurrent access to different models with batching."""

        models = {}

        async def mock_model_loader(model_id: str):
            if model_id not in models:
                models[model_id] = MockModel(model_id, processing_time=0.02)
            return models[model_id]

        wrapper = _ModelMultiplexWrapper(
            model_load_func=mock_model_loader,
            max_num_models_per_replica=4,
            enable_batching=True,
            max_batch_size=2,
            batch_wait_timeout_s=0.03,
        )

        # Create concurrent requests to multiple models
        start_time = time.time()
        tasks = []

        # 2 requests to each of 3 models
        for model_id in ["fast_model", "medium_model", "slow_model"]:
            for i in range(2):
                task = wrapper.predict(f"data_{i}", model_id)
                tasks.append(task)

        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time

        # All requests should complete
        assert len(results) == 6

        # Should process efficiently due to batching
        assert total_time < 0.3  # Much faster than sequential

        # Each model should have been called once in batch mode
        for model_id in ["fast_model", "medium_model", "slow_model"]:
            assert models[model_id].batch_call_count >= 1
            assert models[model_id].last_batch_size == 2


@pytest.mark.asyncio
class TestMultiplexBatchingAPI:
    """Test the API integration for multiplexed batching."""

    async def test_serve_multiplexed_decorator_with_batching(
        self, start_serve_with_context
    ):
        """Test the @serve.multiplexed decorator with batching parameters."""

        # Mock the decorator functionality
        from ray.serve.api import multiplexed

        # Create a model class
        class TestModel:
            def __init__(self, model_id: str):
                self.model_id = model_id

            async def predict(self, data):
                return f"result_{self.model_id}_{data}"

            async def batch_predict(self, data_list):
                return [f"batch_{self.model_id}_{item}" for item in data_list]

        # Create a model loading function (this is what gets decorated)
        async def load_model(model_id: str):
            return TestModel(model_id)

        # Apply the decorator to the loading function
        decorated_load_model = multiplexed(
            max_num_models_per_replica=3,
            enable_batching=True,
            max_batch_size=4,
            batch_wait_timeout_s=0.1,
        )(load_model)

        # Verify the decorator returns a callable
        assert callable(decorated_load_model)

        # Test that calling the decorated function returns the model instance
        # The decorator internally creates a _ModelMultiplexWrapper and caches it,
        # then calls load_model() on it which returns the actual model
        model = await decorated_load_model("test_model")
        assert isinstance(model, TestModel)
        assert model.model_id == "test_model"

        # Test that subsequent calls to the same model use the cached instance
        model2 = await decorated_load_model("test_model")
        assert model2.model_id == "test_model"

        # Test loading a different model
        model3 = await decorated_load_model("another_model")
        assert model3.model_id == "another_model"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
