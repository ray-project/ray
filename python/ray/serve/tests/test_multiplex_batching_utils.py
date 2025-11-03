"""
Test utilities and fixtures for multiplexing with batching tests.

This module provides common test utilities, mock models, and fixtures
used across multiplexing and batching integration tests.
"""

import asyncio
import time
from typing import List, Dict, Any, Optional
from unittest.mock import AsyncMock

import pytest
import ray
from ray import serve
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import DeploymentConfig


class MockEmbeddingModel:
    """Mock embedding model for sentence transformer-like testing."""
    
    def __init__(self, model_id: str, embedding_dim: int = 384):
        self.model_id = model_id
        self.embedding_dim = embedding_dim
        self.load_time = time.time()
        self.predict_calls = 0
        self.batch_predict_calls = 0
        self.total_items_processed = 0
        
    async def predict(self, text: str) -> List[float]:
        """Individual text encoding."""
        await asyncio.sleep(0.02)  # Simulate encoding time
        self.predict_calls += 1
        self.total_items_processed += 1
        
        # Generate deterministic embedding based on text and model
        import hashlib
        hash_input = f"{text}_{self.model_id}".encode()
        hash_obj = hashlib.md5(hash_input)
        
        # Create embedding vector
        embedding = []
        for i in range(self.embedding_dim):
            byte_val = hash_obj.digest()[i % 16]
            embedding.append((byte_val / 255.0) - 0.5)
        
        return embedding
    
    async def batch_predict(self, texts: List[str]) -> List[List[float]]:
        """Batch text encoding - more efficient."""
        batch_size = len(texts)
        # Batch processing is more efficient per item
        await asyncio.sleep(0.01 * batch_size)
        
        self.batch_predict_calls += 1
        self.total_items_processed += batch_size
        
        # Process all texts
        embeddings = []
        for text in texts:
            # Same logic as predict but in batch
            import hashlib
            hash_input = f"{text}_{self.model_id}".encode()
            hash_obj = hashlib.md5(hash_input)
            
            embedding = []
            for i in range(self.embedding_dim):
                byte_val = hash_obj.digest()[i % 16]
                embedding.append((byte_val / 255.0) - 0.5)
            
            embeddings.append(embedding)
        
        return embeddings
    
    def get_stats(self) -> Dict[str, Any]:
        """Get model usage statistics."""
        return {
            "model_id": self.model_id,
            "embedding_dim": self.embedding_dim,
            "predict_calls": self.predict_calls,
            "batch_predict_calls": self.batch_predict_calls,
            "total_items_processed": self.total_items_processed,
            "uptime": time.time() - self.load_time
        }


class MockClassificationModel:
    """Mock classification model for testing."""
    
    def __init__(self, model_id: str, num_classes: int = 3):
        self.model_id = model_id
        self.num_classes = num_classes
        self.predict_calls = 0
        self.batch_predict_calls = 0
        
    async def predict(self, text: str) -> Dict[str, float]:
        """Individual text classification."""
        await asyncio.sleep(0.03)
        self.predict_calls += 1
        
        # Generate deterministic probabilities
        import hashlib
        hash_val = int(hashlib.md5(f"{text}_{self.model_id}".encode()).hexdigest(), 16)
        
        probs = []
        for i in range(self.num_classes):
            prob = ((hash_val + i) % 100) / 100.0
            probs.append(prob)
        
        # Normalize to sum to 1
        total = sum(probs)
        probs = [p / total for p in probs]
        
        return {
            f"class_{i}": probs[i] 
            for i in range(self.num_classes)
        }
    
    async def batch_predict(self, texts: List[str]) -> List[Dict[str, float]]:
        """Batch text classification."""
        batch_size = len(texts)
        await asyncio.sleep(0.02 * batch_size)  # Batch efficiency
        self.batch_predict_calls += 1
        
        results = []
        for text in texts:
            # Same logic as predict
            import hashlib
            hash_val = int(hashlib.md5(f"{text}_{self.model_id}".encode()).hexdigest(), 16)
            
            probs = []
            for i in range(self.num_classes):
                prob = ((hash_val + i) % 100) / 100.0
                probs.append(prob)
            
            total = sum(probs)
            probs = [p / total for p in probs]
            
            result = {f"class_{i}": probs[i] for i in range(self.num_classes)}
            results.append(result)
        
        return results


class MockTranslationModel:
    """Mock translation model for testing."""
    
    def __init__(self, model_id: str, source_lang: str = "en", target_lang: str = "es"):
        self.model_id = model_id
        self.source_lang = source_lang
        self.target_lang = target_lang
        self.translate_calls = 0
        self.batch_translate_calls = 0
        
    async def translate(self, text: str) -> str:
        """Individual translation."""
        await asyncio.sleep(0.05)  # Translation takes longer
        self.translate_calls += 1
        
        # Mock translation by reversing and adding prefix
        translated = f"[{self.target_lang}] {text[::-1]}"
        return translated
    
    async def batch_translate(self, texts: List[str]) -> List[str]:
        """Batch translation."""
        batch_size = len(texts)
        await asyncio.sleep(0.03 * batch_size)  # Batch efficiency
        self.batch_translate_calls += 1
        
        translations = []
        for text in texts:
            translated = f"[{self.target_lang}] {text[::-1]}"
            translations.append(translated)
        
        return translations


class BatchingTestHelper:
    """Helper class for testing batching behavior."""
    
    @staticmethod
    async def send_concurrent_requests(wrapper, inputs: List[str], model_id: str):
        """Send concurrent requests and measure timing."""
        start_time = time.time()
        
        tasks = []
        for input_data in inputs:
            task = wrapper.predict(input_data, model_id)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time
        
        return results, total_time
    
    @staticmethod
    def verify_batching_efficiency(
        individual_time: float, 
        batch_time: float, 
        num_requests: int,
        min_speedup: float = 1.5
    ):
        """Verify that batching provides expected efficiency gains."""
        speedup = individual_time / batch_time
        
        assert speedup >= min_speedup, (
            f"Expected speedup of at least {min_speedup}x, "
            f"got {speedup:.2f}x ({individual_time:.3f}s vs {batch_time:.3f}s)"
        )
        
        return speedup
    
    @staticmethod
    def analyze_batch_patterns(model_instances: List):
        """Analyze batching patterns across model instances."""
        stats = {}
        
        for model in model_instances:
            stats[model.model_id] = {
                "individual_calls": getattr(model, 'predict_calls', 0),
                "batch_calls": getattr(model, 'batch_predict_calls', 0),
                "total_processed": getattr(model, 'total_items_processed', 0)
            }
        
        return stats


@pytest.fixture
def embedding_model_loader():
    """Fixture for embedding model loader."""
    models = {}
    
    async def loader(model_id: str) -> MockEmbeddingModel:
        if model_id not in models:
            # Different embedding dimensions for different models
            dims = {
                "mini": 384,
                "base": 768,
                "large": 1024
            }
            dim = dims.get(model_id, 384)
            models[model_id] = MockEmbeddingModel(model_id, dim)
        
        return models[model_id]
    
    return loader, models


@pytest.fixture
def classification_model_loader():
    """Fixture for classification model loader."""
    models = {}
    
    async def loader(model_id: str) -> MockClassificationModel:
        if model_id not in models:
            # Different number of classes for different models
            classes = {
                "sentiment": 3,  # positive, negative, neutral
                "topic": 5,      # 5 topic categories
                "intent": 10     # 10 intent categories
            }
            num_classes = classes.get(model_id, 3)
            models[model_id] = MockClassificationModel(model_id, num_classes)
        
        return models[model_id]
    
    return loader, models


@pytest.fixture
def translation_model_loader():
    """Fixture for translation model loader."""
    models = {}
    
    async def loader(model_id: str) -> MockTranslationModel:
        if model_id not in models:
            # Different language pairs
            lang_pairs = {
                "en_es": ("en", "es"),
                "en_fr": ("en", "fr"),
                "en_de": ("en", "de")
            }
            source_lang, target_lang = lang_pairs.get(model_id, ("en", "es"))
            models[model_id] = MockTranslationModel(model_id, source_lang, target_lang)
        
        return models[model_id]
    
    return loader, models


@pytest.fixture
def sample_texts():
    """Fixture providing sample texts for testing."""
    return [
        "The quick brown fox jumps over the lazy dog.",
        "Machine learning is transforming artificial intelligence.",
        "Ray Serve makes model deployment scalable and efficient.",
        "Sentence transformers encode text into vector representations.",
        "Batching improves throughput for neural network inference.",
        "Natural language processing enables text understanding.",
        "Deep learning models require careful optimization.",
        "Distributed systems handle large-scale ML workloads.",
        "Vector databases enable efficient similarity search.",
        "Transformer architectures revolutionized NLP applications."
    ]


@pytest.fixture
def performance_test_config():
    """Configuration for performance testing."""
    return {
        "small_batch": 3,
        "medium_batch": 8,
        "large_batch": 16,
        "timeout_short": 0.05,
        "timeout_medium": 0.1,
        "timeout_long": 0.2,
        "min_speedup": 1.5,
        "max_models": 4
    }


class MultiModelTestScenario:
    """Test scenario with multiple models and request patterns."""
    
    def __init__(self, models: List[str], request_patterns: Dict[str, List[str]]):
        self.models = models
        self.request_patterns = request_patterns
        
    async def execute_scenario(self, wrapper):
        """Execute the test scenario."""
        all_tasks = []
        
        for model_id, requests in self.request_patterns.items():
            for request_data in requests:
                task = wrapper.predict(request_data, model_id)
                all_tasks.append((model_id, task))
        
        # Execute all requests concurrently
        start_time = time.time()
        results = []
        
        for model_id, task in all_tasks:
            result = await task
            results.append({
                "model_id": model_id,
                "result": result,
                "timestamp": time.time()
            })
        
        total_time = time.time() - start_time
        
        return results, total_time
    
    def analyze_results(self, results: List[Dict], total_time: float):
        """Analyze scenario execution results."""
        model_results = {}
        
        for result in results:
            model_id = result["model_id"]
            if model_id not in model_results:
                model_results[model_id] = []
            model_results[model_id].append(result)
        
        analysis = {
            "total_requests": len(results),
            "total_time": total_time,
            "models_used": len(model_results),
            "requests_per_model": {
                model_id: len(model_results[model_id])
                for model_id in model_results
            },
            "avg_time_per_request": total_time / len(results) if results else 0
        }
        
        return analysis


# Predefined test scenarios
TEST_SCENARIOS = {
    "embedding_workload": MultiModelTestScenario(
        models=["mini", "base", "large"],
        request_patterns={
            "mini": ["Quick text", "Short phrase", "Brief sentence"],
            "base": ["Medium length text for processing", "Another moderate sentence"],
            "large": ["This is a longer text that requires more sophisticated embedding processing"]
        }
    ),
    
    "classification_workload": MultiModelTestScenario(
        models=["sentiment", "topic", "intent"],
        request_patterns={
            "sentiment": ["I love this product!", "This is terrible", "It's okay I guess"],
            "topic": ["Technology news update", "Sports match results"],
            "intent": ["Book a flight", "Cancel my subscription", "Get weather forecast"]
        }
    ),
    
    "translation_workload": MultiModelTestScenario(
        models=["en_es", "en_fr", "en_de"],
        request_patterns={
            "en_es": ["Hello world", "How are you?"],
            "en_fr": ["Good morning", "Thank you"],
            "en_de": ["Welcome", "Goodbye"]
        }
    )
}


# Test functions for the utility classes
@pytest.mark.asyncio
async def test_mock_embedding_model():
    """Test MockEmbeddingModel functionality."""
    model = MockEmbeddingModel("test_model", embedding_dim=128)
    
    # Test individual prediction
    result = await model.predict("hello world")
    assert len(result) == 128
    assert isinstance(result[0], float)
    assert model.predict_calls == 1
    assert model.total_items_processed == 1
    
    # Test batch prediction
    texts = ["hello", "world", "test"]
    batch_result = await model.batch_predict(texts)
    assert len(batch_result) == 3
    assert len(batch_result[0]) == 128
    assert model.batch_predict_calls == 1
    assert model.total_items_processed == 4  # 1 + 3
    
    # Test stats
    stats = model.get_stats()
    assert stats["model_id"] == "test_model"
    assert stats["embedding_dim"] == 128
    assert stats["predict_calls"] == 1
    assert stats["batch_predict_calls"] == 1


@pytest.mark.asyncio
async def test_mock_classification_model():
    """Test MockClassificationModel functionality."""
    model = MockClassificationModel("sentiment", num_classes=3)
    
    # Test individual prediction
    result = await model.predict("I love this!")
    assert len(result) == 3
    assert all(f"class_{i}" in result for i in range(3))
    assert abs(sum(result.values()) - 1.0) < 1e-6  # Should sum to 1
    assert model.predict_calls == 1
    
    # Test batch prediction
    texts = ["good", "bad", "neutral"]
    batch_result = await model.batch_predict(texts)
    assert len(batch_result) == 3
    assert all(len(result) == 3 for result in batch_result)
    assert model.batch_predict_calls == 1


@pytest.mark.asyncio
async def test_mock_translation_model():
    """Test MockTranslationModel functionality."""
    model = MockTranslationModel("en_es", source_lang="en", target_lang="es")
    
    # Test individual translation
    result = await model.translate("hello")
    assert result.startswith("[es]")
    assert "olleh" in result  # reversed text
    assert model.translate_calls == 1
    
    # Test batch translation
    texts = ["hello", "world"]
    batch_result = await model.batch_translate(texts)
    assert len(batch_result) == 2
    assert all(result.startswith("[es]") for result in batch_result)
    assert model.batch_translate_calls == 1


@pytest.mark.asyncio
async def test_batching_test_helper():
    """Test BatchingTestHelper functionality."""
    from ray.serve.multiplex import _ModelMultiplexWrapper
    
    # Mock a simple wrapper
    class MockWrapper:
        def __init__(self):
            self.call_count = 0
            
        async def predict(self, input_data, model_id):
            self.call_count += 1
            await asyncio.sleep(0.01)
            return f"result_{model_id}_{input_data}"
    
    wrapper = MockWrapper()
    inputs = ["test1", "test2", "test3"]
    
    # Test concurrent requests
    results, total_time = await BatchingTestHelper.send_concurrent_requests(
        wrapper, inputs, "test_model"
    )
    
    assert len(results) == 3
    assert wrapper.call_count == 3
    assert total_time > 0
    
    # Test efficiency verification
    individual_time = 0.1
    batch_time = 0.05
    speedup = BatchingTestHelper.verify_batching_efficiency(
        individual_time, batch_time, 3, min_speedup=1.5
    )
    assert speedup == 2.0


def test_multi_model_test_scenario():
    """Test MultiModelTestScenario functionality."""
    scenario = TEST_SCENARIOS["embedding_workload"]
    
    assert len(scenario.models) == 3
    assert "mini" in scenario.models
    assert "base" in scenario.models
    assert "large" in scenario.models
    
    # Test analyze_results
    mock_results = [
        {"model_id": "mini", "result": "result1", "timestamp": 1.0},
        {"model_id": "mini", "result": "result2", "timestamp": 1.1},
        {"model_id": "base", "result": "result3", "timestamp": 1.2},
    ]
    
    analysis = scenario.analyze_results(mock_results, 0.5)
    assert analysis["total_requests"] == 3
    assert analysis["total_time"] == 0.5
    assert analysis["models_used"] == 2
    assert analysis["requests_per_model"]["mini"] == 2
    assert analysis["requests_per_model"]["base"] == 1


def test_fixtures_return_correct_types(embedding_model_loader, classification_model_loader, 
                                     translation_model_loader, sample_texts, performance_test_config):
    """Test that all fixtures return the expected types."""
    # Test embedding model loader
    loader, models = embedding_model_loader
    assert callable(loader)
    assert isinstance(models, dict)
    
    # Test classification model loader
    loader, models = classification_model_loader
    assert callable(loader)
    assert isinstance(models, dict)
    
    # Test translation model loader
    loader, models = translation_model_loader
    assert callable(loader)
    assert isinstance(models, dict)
    
    # Test sample texts
    assert isinstance(sample_texts, list)
    assert len(sample_texts) > 0
    assert all(isinstance(text, str) for text in sample_texts)
    
    # Test performance config
    assert isinstance(performance_test_config, dict)
    assert "small_batch" in performance_test_config
    assert "min_speedup" in performance_test_config


@pytest.mark.asyncio
async def test_embedding_model_loader_fixture(embedding_model_loader):
    """Test the embedding model loader fixture."""
    loader, models = embedding_model_loader
    
    # Load a model
    model = await loader("mini")
    assert isinstance(model, MockEmbeddingModel)
    assert model.model_id == "mini"
    assert model.embedding_dim == 384
    
    # Check it's cached
    model2 = await loader("mini")
    assert model is model2
    assert len(models) == 1
    
    # Load different model
    model3 = await loader("base")
    assert model3.embedding_dim == 768
    assert len(models) == 2


if __name__ == "__main__":
    # Run all tests in this module
    import sys
    pytest.main(["-v", "-s", __file__] + sys.argv[1:])