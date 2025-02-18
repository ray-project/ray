import sys

import pytest
from unittest.mock import patch, MagicMock
from ray.llm._internal.batch.stages.chat_template_stage import ChatTemplateUDF


@pytest.fixture
def mock_tokenizer_setup():
    with patch("transformers.AutoProcessor") as mock_auto_processor:
        mock_processor = MagicMock()
        mock_auto_processor.from_pretrained.return_value = mock_processor
        yield mock_processor


@pytest.mark.asyncio
async def test_chat_template_udf_basic(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.apply_chat_template.return_value = ["<chat>Hello AI</chat>"]

    udf = ChatTemplateUDF(
        data_column="__data",
        model="test-model",
    )

    batch = {
        "__data": [
            {
                "messages": MagicMock(
                    tolist=lambda: [{"role": "user", "content": "Hello AI"}]
                )
            }
        ]
    }

    results = []
    async for result in udf(batch):
        results.append(result)

    assert len(results) == 1
    assert results[0]["__data"][0]["prompt"] == "<chat>Hello AI</chat>"
    mock_tokenizer.apply_chat_template.assert_called_once()


@pytest.mark.asyncio
async def test_chat_template_udf_multiple_messages(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.apply_chat_template.return_value = [
        "<chat>Hello AI</chat>",
        "<chat>How are you?</chat>",
    ]

    udf = ChatTemplateUDF(
        data_column="__data",
        model="test-model",
    )

    batch = {
        "__data": [
            {
                "messages": MagicMock(
                    tolist=lambda: [{"role": "user", "content": "Hello AI"}]
                )
            },
            {
                "messages": MagicMock(
                    tolist=lambda: [{"role": "user", "content": "How are you?"}],
                )
            },
        ]
    }

    results = []
    async for result in udf(batch):
        results.append(result)

    assert len(results) == 2
    assert results[0]["__data"][0]["prompt"] == "<chat>Hello AI</chat>"
    assert results[1]["__data"][0]["prompt"] == "<chat>How are you?</chat>"
    mock_tokenizer.apply_chat_template.assert_called_once()


def test_chat_template_udf_expected_input_keys(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.apply_chat_template.return_value = [
        "<chat>Hello AI</chat>",
        "<chat>How are you?</chat>",
    ]

    udf = ChatTemplateUDF(
        data_column="__data",
        model="test-model",
    )
    assert udf.expected_input_keys == ["messages"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
