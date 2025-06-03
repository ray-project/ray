import sys

import pytest

from ray.llm._internal.serve.configs.prompt_formats import Prompt
from ray.llm._internal.serve.configs.server_models import SamplingParams


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
            best_of=3,
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

    def test_stop_valid_sequences(self):
        """Test that valid stop sequences are processed correctly."""
        stop_sequences = ["END", "STOP", "FINISH", "END"]
        params = SamplingParams(stop=stop_sequences)
        assert params.stop == ["END", "FINISH", "STOP"]  # Should be unique

    def test_idempotency(self):
        params = SamplingParams()
        new_params = SamplingParams.model_validate(params.model_dump())
        assert params.model_dump() == new_params.model_dump()

    @pytest.mark.parametrize(
        "stop, stop_tokens",
        [
            (["B-END", "A-End"], None),
            (["B-END", "A-End"], []),
            (None, [100, 50]),
            (None, None),
        ],
    )
    def test_from_prompt_with_dict_parameters(self, stop, stop_tokens):
        """Test from_prompt method with dictionary parameters."""
        prompt = Prompt(
            prompt="Test prompt",
            parameters={
                "stop": stop,
                "stop_tokens": stop_tokens,
            },
        )

        params = SamplingParams.from_prompt(prompt)

        assert params.stop == (sorted(stop) if stop is not None else None)
        assert params.stop_tokens == (
            sorted(stop_tokens) if stop_tokens is not None else None
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
