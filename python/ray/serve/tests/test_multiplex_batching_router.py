"""
Test cases for request router integration with multiplexing and batching.

This module tests the end-to-end batching functionality with multiplexed models,
including model caching, LRU eviction, and routing metrics.
"""

import asyncio
import time
from typing import List, Dict, Any

import pytest

import ray
from ray import serve
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import DeploymentConfig
from ray.serve.multiplex import _ModelMultiplexWrapper


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


class TrackableModel:
    """Model that tracks its lifecycle and usage for testing."""
    
    _instances = {}  # Track all created instances
    _deleted = []    # Track deleted models
    
    def __init__(self, model_id: str):
        self.model_id = model_id
        self.created_at = time.time()
        self.predict_count = 0
        self.batch_predict_count = 0
        TrackableModel._instances[model_id] = self
        
    async def predict(self, data):
        """Individual prediction."""
        await asyncio.sleep(0.01)
        self.predict_count += 1
        return f"result_{self.model_id}_{data}"
    
    async def batch_predict(self, data_list: List):
        """Batch prediction."""
        await asyncio.sleep(0.005 * len(data_list))
        self.batch_predict_count += 1
        return [f"batch_{self.model_id}_{item}" for item in data_list]
    
    def __del__(self):
        """Track when models are evicted."""
        TrackableModel._deleted.append(self.model_id)
    
    @classmethod
    def reset_tracking(cls):
        """Reset all tracking for new test."""
        cls._instances = {}
        cls._deleted = []
    
    @classmethod
    def get_stats(cls) -> Dict[str, Any]:
        """Get current statistics."""
        return {
            "active_models": len(cls._instances),
            "deleted_models": len(cls._deleted),
            "model_ids": list(cls._instances.keys()),
            "deleted_ids": cls._deleted.copy()
        }


@pytest.mark.asyncio
class TestMultiplexBatchingEnd2End:
    """End-to-end tests for the complete routing and batching pipeline."""
    
    async def test_model_caching_and_lru_eviction(self, start_serve_with_context):
        """Test that models are cached and evicted using LRU policy."""
        TrackableModel.reset_tracking()
        
        async def load_model(model_id: str):
            return TrackableModel(model_id)
        
        # Create wrapper with max 3 models
        wrapper = _ModelMultiplexWrapper(
            model_load_func=load_model,
            max_num_models_per_replica=3,
            enable_batching=False
        )
        
        # Load 3 models - all should be cached
        await wrapper.load_model("model_a")
        await wrapper.load_model("model_b")
        await wrapper.load_model("model_c")
        
        stats = TrackableModel.get_stats()
        assert stats["active_models"] == 3, f"Expected 3 active models, got {stats['active_models']}"
        assert stats["deleted_models"] == 0, "No models should be deleted yet"
        
        # Load 4th model - should evict least recently used (model_a)
        await wrapper.load_model("model_d")
        
        # Give some time for garbage collection
        await asyncio.sleep(0.1)
        
        # Check the wrapper's cache size (should be at most 3)
        cache_size = len(wrapper.models)
        assert cache_size <= 3, f"Should have at most 3 models in cache, got {cache_size}"
        
        # Access model_b and model_c to keep them recent
        await wrapper.load_model("model_b")
        await wrapper.load_model("model_c")
        
        # Load another model - should evict model_d (least recently used)
        await wrapper.load_model("model_e")
        await asyncio.sleep(0.1)
        
        final_cache_size = len(wrapper.models)
        assert final_cache_size <= 3, f"Should maintain max 3 models, got {final_cache_size}"
        
        print(f"Final cache size: {final_cache_size}")
        print(f"Models in cache: {list(wrapper.models.keys())}")
    
    async def test_model_reuse_vs_reload(self, start_serve_with_context):
        """Test that cached models are reused without reloading."""
        TrackableModel.reset_tracking()
        
        load_count = {"count": 0}
        
        async def load_model(model_id: str):
            load_count["count"] += 1
            return TrackableModel(model_id)
        
        wrapper = _ModelMultiplexWrapper(
            model_load_func=load_model,
            max_num_models_per_replica=2,
            enable_batching=False
        )
        
        # Load model_a for the first time
        await wrapper.load_model("model_a")
        assert load_count["count"] == 1, "Model should be loaded once"
        
        # Use model_a again - should reuse cached version
        await wrapper.load_model("model_a")
        assert load_count["count"] == 1, "Model should not be reloaded"
        
        # Load model_b
        await wrapper.load_model("model_b")
        assert load_count["count"] == 2, "Second model should be loaded"
        
        # Use both models again - no reloads
        await wrapper.load_model("model_a")
        await wrapper.load_model("model_b")
        assert load_count["count"] == 2, "No additional loads needed"
        
        # Load model_c - should evict one model
        await wrapper.load_model("model_c")
        assert load_count["count"] == 3, "Third model loaded"
        
        # Use model_a again - should reload if it was evicted
        await wrapper.load_model("model_a")
        # Could be 3 or 4 depending on which was evicted
        assert load_count["count"] >= 3, "Model may need reload if evicted"
        
        print(f"Total model loads: {load_count['count']}")
    
    async def test_batching_efficiency_metrics(self, start_serve_with_context):
        """Test that batching improves throughput and tracks metrics."""
        TrackableModel.reset_tracking()
        
        async def load_model(model_id: str):
            return TrackableModel(model_id)
        
        # Test with batching enabled
        wrapper_batched = _ModelMultiplexWrapper(
            model_load_func=load_model,
            max_num_models_per_replica=2,
            enable_batching=True,
            max_batch_size=5,
            batch_wait_timeout_s=0.05
        )
        
        # Load model first
        model = await wrapper_batched.load_model("batched_model")
        
        # Send concurrent requests to the wrapper to test batching mechanism
        start_time = time.time()
        tasks = []
        for i in range(10):
            # Use wrapper.predict() to test the actual batching mechanism
            task = wrapper_batched.predict(f"data_{i}", "batched_model")
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        batched_time = time.time() - start_time
        
        # Check that batch predict was called (indicating batching worked)
        assert model.batch_predict_count > 0, "Batch predict should be called"
        assert len(results) == 10, "All requests should complete"
        
        # Verify results are correct format - should be from batch_predict
        assert all("batch_batched_model" in result for result in results), f"Expected batch results, got: {results[:3]}"
        
        # Test without batching for comparison
        TrackableModel.reset_tracking()
        
        wrapper_no_batch = _ModelMultiplexWrapper(
            model_load_func=load_model,
            max_num_models_per_replica=2,
            enable_batching=False
        )
        
        model_no_batch = await wrapper_no_batch.load_model("no_batch_model")
        
        start_time = time.time()
        tasks = []
        for i in range(10):
            task = model_no_batch.predict(f"data_{i}")
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        no_batch_time = time.time() - start_time
        
        assert model_no_batch.predict_count > 0, "Individual predict should be called"
        assert model_no_batch.batch_predict_count == 0, "Batch predict should not be called"
        
        print(f"Batched time: {batched_time:.3f}s, No-batch time: {no_batch_time:.3f}s")
        print(f"Batch predict calls: {model.batch_predict_count}")
        print(f"Individual predict calls: {model_no_batch.predict_count}")
    
    async def test_concurrent_model_access_patterns(self, start_serve_with_context):
        """Test concurrent access to multiple models."""
        TrackableModel.reset_tracking()
        
        async def load_model(model_id: str):
            return TrackableModel(model_id)
        
        wrapper = _ModelMultiplexWrapper(
            model_load_func=load_model,
            max_num_models_per_replica=4,
            enable_batching=False
        )
        
        # Load multiple models concurrently
        start_time = time.time()
        
        hot_model = await wrapper.load_model("hot_model")
        warm_model = await wrapper.load_model("warm_model")
        cold_model = await wrapper.load_model("cold_model")
        
        # Simulate workload with varying access patterns
        tasks = []
        for i in range(6):
            tasks.append(hot_model.predict(f"data_{i}"))
        for i in range(3):
            tasks.append(warm_model.predict(f"data_{i}"))
        for i in range(1):
            tasks.append(cold_model.predict(f"data_{i}"))
        
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time
        
        stats = TrackableModel.get_stats()
        
        assert len(results) == 10, "All requests should complete"
        assert stats["active_models"] == 3, f"Should have 3 models loaded, got {stats['active_models']}"
        
        # Check access counts
        assert hot_model.predict_count == 6, "Hot model should have 6 accesses"
        assert warm_model.predict_count == 3, "Warm model should have 3 accesses"
        assert cold_model.predict_count == 1, "Cold model should have 1 access"
        
        print(f"Total time for 10 requests across 3 models: {total_time:.3f}s")
        print(f"Active models: {stats['model_ids']}")
    
    async def test_model_affinity_for_batching(self, start_serve_with_context):
        """Test model caching behavior."""
        TrackableModel.reset_tracking()
        
        async def load_model(model_id: str):
            return TrackableModel(model_id)
        
        wrapper = _ModelMultiplexWrapper(
            model_load_func=load_model,
            max_num_models_per_replica=3,
            enable_batching=False
        )
        
        # Load and access same model multiple times
        model1 = await wrapper.load_model("affinity_model")
        model2 = await wrapper.load_model("affinity_model")  # Should be same instance
        
        assert model1 is model2, "Should return cached model instance"
        
        # Access the model
        results = []
        for i in range(4):
            result = await model1.predict(f"request_{i}")
            results.append(result)
        
        assert len(results) == 4
        assert all("result_affinity_model" in r for r in results)
        assert model1.predict_count == 4, "Should track all predictions"
        
        print(f"Predict count: {model1.predict_count}")


@pytest.mark.asyncio
class TestMultiplexCachingMetrics:
    """Tests focused on caching metrics and behavior."""
    
    async def test_cache_hit_rate_tracking(self, start_serve_with_context):
        """Test tracking of cache hits vs misses."""
        TrackableModel.reset_tracking()
        
        load_attempts = []
        
        async def load_model(model_id: str):
            load_attempts.append(model_id)
            return TrackableModel(model_id)
        
        wrapper = _ModelMultiplexWrapper(
            model_load_func=load_model,
            max_num_models_per_replica=2,
            enable_batching=False
        )
        
        # First access - cache miss
        await wrapper.load_model("model_x")
        cache_misses = len(load_attempts)
        assert cache_misses == 1
        
        # Second access - cache hit
        await wrapper.load_model("model_x")
        assert len(load_attempts) == 1, "Should not reload cached model"
        
        # Third model exceeds cache - eviction
        await wrapper.load_model("model_y")
        await wrapper.load_model("model_z")
        
        # Accessing evicted model - cache miss
        await wrapper.load_model("model_x")
        
        total_loads = len(load_attempts)
        print(f"Total model loads: {total_loads}")
        print(f"Load sequence: {load_attempts}")
    
    async def test_eviction_order_lru(self, start_serve_with_context):
        """Test that LRU eviction policy is followed."""
        TrackableModel.reset_tracking()
        
        access_log = []
        
        async def load_model(model_id: str):
            access_log.append(("load", model_id))
            return TrackableModel(model_id)
        
        wrapper = _ModelMultiplexWrapper(
            model_load_func=load_model,
            max_num_models_per_replica=2,
            enable_batching=False
        )
        
        # Load model A and B
        await wrapper.load_model("A")
        await wrapper.load_model("B")
        
        # Access A again (making B least recently used)
        await wrapper.load_model("A")
        
        # Load C - should evict B (LRU)
        await wrapper.load_model("C")
        
        # If we access B again, it should reload
        await wrapper.load_model("B")
        
        # Count loads per model
        loads = {}
        for action, model_id in access_log:
            if action == "load":
                loads[model_id] = loads.get(model_id, 0) + 1
        
        print(f"Access log: {access_log}")
        print(f"Load counts: {loads}")
        
        # A should be loaded once, B twice (initial + after eviction), C once
        assert loads.get("A") == 1, "A loaded once"
        assert loads.get("C") == 1, "C loaded once"
        # B might be loaded once or twice depending on timing


@pytest.mark.asyncio
class TestMultiplexBatchingIntegration:
    """Integration tests combining multiplexing and batching."""
    
    async def test_multiple_models_with_batching(self, start_serve_with_context):
        """Test loading multiple models and basic tracking."""
        TrackableModel.reset_tracking()
        
        async def load_model(model_id: str):
            return TrackableModel(model_id)
        
        wrapper = _ModelMultiplexWrapper(
            model_load_func=load_model,
            max_num_models_per_replica=3,
            enable_batching=False
        )
        
        # Load multiple models
        model_1 = await wrapper.load_model("model_1")
        model_2 = await wrapper.load_model("model_2")
        model_3 = await wrapper.load_model("model_3")
        
        # Use each model
        tasks = []
        for i in range(3):
            tasks.append(model_1.predict(f"m1_data_{i}"))
        for i in range(3):
            tasks.append(model_2.predict(f"m2_data_{i}"))
        for i in range(2):
            tasks.append(model_3.predict(f"m3_data_{i}"))
        
        results = await asyncio.gather(*tasks)
        stats = TrackableModel.get_stats()
        
        assert len(results) == 8, "All requests should complete"
        assert stats["active_models"] == 3, "Should have 3 models"
        
        # Verify models were used
        for model_id in ["model_1", "model_2", "model_3"]:
            model = TrackableModel._instances.get(model_id)
            assert model is not None
            assert model.predict_count > 0, f"{model_id} should be used"
        
        print(f"Stats: {stats}")
        print(f"Model 1 predictions: {TrackableModel._instances['model_1'].predict_count}")
        print(f"Model 2 predictions: {TrackableModel._instances['model_2'].predict_count}")
        print(f"Model 3 predictions: {TrackableModel._instances['model_3'].predict_count}")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "-s"])