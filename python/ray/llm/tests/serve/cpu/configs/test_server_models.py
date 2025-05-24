import pytest
import sys
from unittest.mock import Mock, patch
from typing import Dict, Any, List

# Import the classes we need to test
from ray.llm._internal.serve.configs.server_models import SamplingParams
from ray.llm._internal.serve.configs.prompt_formats import Prompt, Message
from ray.llm._internal.serve.configs.error_handling import TooManyStoppingSequences
from ray.llm._internal.serve.configs.constants import MAX_NUM_STOPPING_SEQUENCES


class TestSamplingParams:
    
    def test_default_initialization(self):
        """Test that SamplingParams can be initialized with default values."""
        params = SamplingParams()
        
        assert params.max_tokens is None
        assert params.temperature is None
        assert params.top_p is None
        assert params.n == 1
        assert params.logprobs is None
        assert params.top_logprobs is None
        assert params.logit_bias is None
        assert params.stop is None
        assert params.stop_tokens is None
        assert params.ignore_eos is None
        assert params.presence_penalty is None
        assert params.frequency_penalty is None
        assert params.best_of == 1
        assert params.response_format is None

    def test_initialization_with_values(self):
        """Test that SamplingParams can be initialized with specific values."""
        params = SamplingParams(
            max_tokens=100,
            temperature=0.7,
            top_p=0.9,
            n=2,
            logprobs=True,
            top_logprobs=5,
            stop=["END", "STOP"],
            stop_tokens=[1, 2, 3],
            presence_penalty=0.5,
            frequency_penalty=0.3,
            best_of=3
        )
        
        assert params.max_tokens == 100
        assert params.temperature == 0.7
        assert params.top_p == 0.9
        assert params.n == 2
        assert params.logprobs is True
        assert params.top_logprobs == 5
        assert params.stop == ["END", "STOP"]
        assert params.stop_tokens == [1, 2, 3]
        assert params.presence_penalty == 0.5
        assert params.frequency_penalty == 0.3
        assert params.best_of == 3

    def test_stop_none_validation(self):
        """Test that stop=None is converted to empty list."""
        params = SamplingParams(stop=None)
        assert params.stop == []

    def test_stop_empty_list_validation(self):
        """Test that empty stop list remains empty."""
        params = SamplingParams(stop=[])
        assert params.stop == []

    def test_stop_valid_sequences(self):
        """Test that valid stop sequences are processed correctly."""
        stop_sequences = ["END", "STOP", "FINISH", "END"]
        params = SamplingParams(stop=stop_sequences)
        assert params.stop == ["END", "FINISH", "STOP"]  # Should be unique


    def test_stop_tokens_none_handling(self):
        """Test that stop_tokens=None works correctly."""
        params = SamplingParams(stop_tokens=None)
        assert params.stop_tokens is None

    def test_stop_tokens_with_values(self):
        """Test that stop_tokens with values works correctly."""
        stop_tokens = [1, 2, 3, 4, 5]
        params = SamplingParams(stop_tokens=stop_tokens)
        assert params.stop_tokens == stop_tokens
        
        
    def test_idempotency(self):
        params = SamplingParams()
        new_params = SamplingParams.model_validate(params.model_dump())
        assert params.model_dump() == new_params.model_dump()

    def test_model_dump_excludes_ignored_fields(self):
        """Test that model_dump excludes ignored fields."""
        params = SamplingParams(max_tokens=100, temperature=0.7)
        
        # Test with no ignored fields
        dump = params.model_dump()
        assert "max_tokens" in dump
        assert "temperature" in dump
        
        # Test with ignored fields
        params._ignored_fields = {"temperature"}
        dump = params.model_dump()
        assert "max_tokens" in dump
        assert "temperature" not in dump

    def test_from_prompt_with_dict_parameters(self):
        """Test from_prompt method with dictionary parameters."""
        prompt = Prompt(
            prompt="Test prompt",
            parameters={
                "max_tokens": 50,
                "temperature": 0.8,
                "stop": ["END"],
                "stop_tokens": [1, 2]
            }
        )
        
        params = SamplingParams.from_prompt(prompt)
        assert params.max_tokens == 50
        assert params.temperature == 0.8
        assert params.stop == {"END"}  # Converted to set in from_prompt
        assert params.stop_tokens == {1, 2}  # Converted to set in from_prompt

    def test_from_prompt_with_none_parameters(self):
        """Test from_prompt method with None parameters."""
        prompt = Prompt(prompt="Test prompt", parameters=None)
        
        params = SamplingParams.from_prompt(prompt)
        assert params.stop == set()  # Empty set when no stop sequences
        assert params.stop_tokens == set()  # Empty set when no stop tokens

    def test_from_prompt_with_empty_parameters(self):
        """Test from_prompt method with empty parameters dict."""
        prompt = Prompt(prompt="Test prompt", parameters={})
        
        params = SamplingParams.from_prompt(prompt)
        assert params.stop == set()  # Empty set when no stop sequences
        assert params.stop_tokens == set()  # Empty set when no stop tokens

    def test_from_prompt_with_pydantic_model_parameters(self):
        """Test from_prompt method with Pydantic model parameters."""
        # Create a mock Pydantic model
        mock_params = Mock()
        mock_params.model_dump.return_value = {
            "max_tokens": 75,
            "temperature": 0.6,
            "stop": ["DONE"],
            "stop_tokens": [10, 20]
        }
        
        prompt = Prompt(prompt="Test prompt", parameters=mock_params)
        
        params = SamplingParams.from_prompt(prompt)
        assert params.max_tokens == 75
        assert params.temperature == 0.6
        assert params.stop == {"DONE"}  # Converted to set in from_prompt
        assert params.stop_tokens == {10, 20}  # Converted to set in from_prompt

    def test_from_prompt_stop_and_stop_tokens_none_conversion(self):
        """Test that None values for stop and stop_tokens are converted to empty sets in from_prompt."""
        prompt = Prompt(
            prompt="Test prompt",
            parameters={
                "max_tokens": 100,
                "stop": None,
                "stop_tokens": None
            }
        )
        
        params = SamplingParams.from_prompt(prompt)
        assert params.max_tokens == 100
        assert params.stop == set()  # None converted to empty set
        assert params.stop_tokens == set()  # None converted to empty set

    def test_from_prompt_preserves_other_fields(self):
        """Test that from_prompt preserves all other fields correctly."""
        prompt = Prompt(
            prompt="Test prompt",
            parameters={
                "max_tokens": 200,
                "temperature": 0.9,
                "top_p": 0.95,
                "n": 3,
                "logprobs": True,
                "top_logprobs": 10,
                "presence_penalty": 0.2,
                "frequency_penalty": 0.1,
                "best_of": 5,
                "stop": ["END", "STOP"],
                "stop_tokens": [1, 2, 3]
            }
        )
        
        params = SamplingParams.from_prompt(prompt)
        assert params.max_tokens == 200
        assert params.temperature == 0.9
        assert params.top_p == 0.95
        assert params.n == 3
        assert params.logprobs is True
        assert params.top_logprobs == 10
        assert params.presence_penalty == 0.2
        assert params.frequency_penalty == 0.1
        assert params.best_of == 5
        assert params.stop == {"END", "STOP"}
        assert params.stop_tokens == {1, 2, 3}

    def test_validation_with_mixed_types(self):
        """Test validation works with different input types."""
        # Test with string stop sequences
        params1 = SamplingParams(stop=["hello", "world"])
        assert params1.stop == ["hello", "world"]
        
        # Test with integer stop tokens
        params2 = SamplingParams(stop_tokens=[100, 200, 300])
        assert params2.stop_tokens == [100, 200, 300]
        
        # Test with float penalties
        params3 = SamplingParams(presence_penalty=1.5, frequency_penalty=-0.5)
        assert params3.presence_penalty == 1.5
        assert params3.frequency_penalty == -0.5

    def test_edge_cases(self):
        """Test edge cases for SamplingParams."""
        # Test with zero values
        params = SamplingParams(
            max_tokens=0,
            temperature=0.0,
            top_p=0.0,
            n=1,
            best_of=1,
            presence_penalty=0.0,
            frequency_penalty=0.0
        )
        assert params.max_tokens == 0
        assert params.temperature == 0.0
        assert params.top_p == 0.0
        assert params.presence_penalty == 0.0
        assert params.frequency_penalty == 0.0

    def test_stop_sequences_sorting_and_uniqueness(self):
        """Test that stop sequences are properly sorted and made unique."""
        # Test with unsorted duplicates
        stop_sequences = ["zebra", "apple", "banana", "apple", "zebra"]
        params = SamplingParams(stop=stop_sequences)
        
        # Should be sorted alphabetically and unique
        expected = ["apple", "banana", "zebra"]
        assert params.stop == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
