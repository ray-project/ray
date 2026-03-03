import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.llm._internal.batch.stages.chat_template_stage import ChatTemplateUDF


@pytest.fixture
def mock_tokenizer_setup():
    # As of writing, the test environment does not have TYPE_CHECKING, which
    # triggers lazy module import in transformers/__init__.py. This means the
    # mocking may not work without explicitly importing AutoProcessor, hence
    # the following import.
    from transformers import AutoProcessor  # noqa: F401

    with patch("transformers.AutoProcessor") as mock_auto_processor:
        mock_processor = MagicMock()
        mock_auto_processor.from_pretrained.return_value = mock_processor
        yield mock_processor


@pytest.mark.asyncio
async def test_chat_template_udf_basic(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.apply_chat_template.return_value = "<chat>Hello AI</chat>"

    udf = ChatTemplateUDF(
        data_column="__data",
        expected_input_keys=["messages"],
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
        results.extend(result["__data"])

    assert len(results) == 1
    assert results[0]["prompt"] == "<chat>Hello AI</chat>"
    mock_tokenizer.apply_chat_template.assert_called_once()


@pytest.mark.asyncio
async def test_chat_template_udf_multiple_messages(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.apply_chat_template.side_effect = [
        "<chat>Hello AI</chat>",
        "<chat>How are you?</chat>",
    ]

    udf = ChatTemplateUDF(
        data_column="__data",
        expected_input_keys=["messages"],
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

    assert len(results) == 1
    assert results[0]["__data"][0]["prompt"] == "<chat>Hello AI</chat>"
    assert results[0]["__data"][1]["prompt"] == "<chat>How are you?</chat>"
    assert mock_tokenizer.apply_chat_template.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chat_template_kwargs, expected_prompt",
    [
        ({"enable_thinking": False}, "Answer without thinking"),
        ({"enable_thinking": True}, "<think>thinking</think>"),
        ({}, "<think>thinking</think>"),
        (
            {"enable_thinking": True, "custom_param": "test_value", "temperature": 0.7},
            "<think>thinking</think>",
        ),
    ],
)
async def test_chat_template_udf_chat_template_kwargs(
    mock_tokenizer_setup, chat_template_kwargs, expected_prompt
):
    mock_tokenizer = mock_tokenizer_setup

    # Store captured kwargs for verification
    captured_kwargs = {}

    def side_effect_func(conversation, **kwargs):
        # Capture all kwargs for later verification
        captured_kwargs.update(kwargs)

        enable_thinking = kwargs.get("enable_thinking", True)
        if enable_thinking is False:
            return "Answer without thinking"
        else:
            return "<think>thinking</think>"

    mock_tokenizer.apply_chat_template.side_effect = side_effect_func

    udf = ChatTemplateUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="test-model",
        chat_template_kwargs=chat_template_kwargs,
    )

    # Assert that the chat_template_kwargs were properly stored
    assert udf.chat_template_kwargs == chat_template_kwargs

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
        results.extend(result["__data"])

    assert len(results) == 1
    assert results[0]["prompt"] == expected_prompt

    # Verify that all chat_template_kwargs were passed through to apply_chat_template
    for key, value in chat_template_kwargs.items():
        assert (
            key in captured_kwargs
        ), f"Expected kwargs key '{key}' not found in captured kwargs"
        assert (
            captured_kwargs[key] == value
        ), f"Expected '{key}': {value}, but got '{key}': {captured_kwargs[key]}"


@pytest.mark.asyncio
async def test_chat_template_udf_assistant_prefill(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.apply_chat_template.side_effect = [
        "<chat>Hello AI<assistant><think>\n</chat>",
        "<chat>Hello AI</chat>",
    ]

    udf = ChatTemplateUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="test-model",
    )

    batch = {
        "__data": [
            {
                "messages": MagicMock(
                    tolist=lambda: [
                        {"role": "user", "content": "Hello AI"},
                        {"role": "assistant", "content": "<think>\n"},
                    ]
                ),
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
        results.extend(result["__data"])

    assert len(results) == 2
    assert mock_tokenizer.apply_chat_template.call_count == 2
    assert results[0]["prompt"] == "<chat>Hello AI<assistant><think>\n</chat>"
    assert results[1]["prompt"] == "<chat>Hello AI</chat>"
    # check if kwargs were set properly
    call_args_list = mock_tokenizer.apply_chat_template.call_args_list
    args1, kwargs1 = call_args_list[0]
    assert not kwargs1.get("add_generation_prompt")
    assert kwargs1.get("continue_final_message")
    _, kwargs2 = call_args_list[1]
    assert kwargs2.get("add_generation_prompt")
    assert not kwargs2.get("continue_final_message")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
