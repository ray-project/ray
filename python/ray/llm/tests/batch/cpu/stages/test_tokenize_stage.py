import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.llm._internal.batch.stages.tokenize_stage import DetokenizeUDF, TokenizeUDF


@pytest.fixture
def mock_tokenizer_setup():
    with patch(
        "ray.llm._internal.batch.stages.tokenize_stage.get_cached_tokenizer"
    ) as mock_get_tokenizer, patch("transformers.AutoTokenizer") as mock_auto_tokenizer:
        mock_tokenizer = MagicMock()
        mock_tokenizer.side_effect = lambda texts: {
            "input_ids": [[1, 2, 3] for _ in texts]
        }
        mock_get_tokenizer.return_value = mock_tokenizer
        mock_auto_tokenizer.from_pretrained.return_value = mock_tokenizer
        yield mock_tokenizer


@pytest.mark.asyncio
async def test_tokenize_udf_basic(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.return_value = [
        {"input_ids": [1, 2, 3]},
        {"input_ids": [4, 5, 6]},
    ]

    udf = TokenizeUDF(
        data_column="__data", model="test-model", expected_input_keys=["prompt"]
    )
    batch = {"__data": [{"prompt": "Hello"}, {"prompt": "World"}]}

    results = []
    async for result in udf(batch):
        results.extend(result["__data"])

    assert len(results) == 2
    assert all(result["tokenized_prompt"] == [1, 2, 3] for result in results)
    assert all(
        original["prompt"] == result["prompt"]
        for original, result in zip(batch, results)
    )


@pytest.mark.asyncio
async def test_detokenize_udf_basic(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.batch_decode.return_value = ["Hello", "World"]

    udf = DetokenizeUDF(
        data_column="__data",
        model="test-model",
        expected_input_keys=["generated_tokens"],
    )
    batch = {
        "__data": [
            {"generated_tokens": [1, 2, 3]},
            {"generated_tokens": [4, 5, 6]},
        ]
    }

    results = []
    async for result in udf(batch):
        results.extend(result["__data"])

    assert len(results) == 2
    assert results[0]["generated_text"] == "Hello"
    assert results[1]["generated_text"] == "World"
    mock_tokenizer.batch_decode.assert_called_once_with(
        [[1, 2, 3], [4, 5, 6]], skip_special_tokens=True
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
