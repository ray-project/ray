import pytest
from unittest.mock import patch, MagicMock
from ray.llm._internal.batch.stages.chat_template_stage import ChatTemplateUDF


@pytest.fixture
def mock_tokenizer_setup():
    with patch(
        "ray.llm._internal.batch.stages.chat_template_stage.get_cached_tokenizer"
    ) as mock_get_tokenizer, patch("transformers.AutoTokenizer") as mock_auto_tokenizer:
        mock_tokenizer = MagicMock()
        mock_get_tokenizer.return_value = mock_tokenizer
        mock_auto_tokenizer.from_pretrained.return_value = mock_tokenizer
        yield mock_tokenizer


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
