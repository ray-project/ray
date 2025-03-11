#!/usr/bin/env python
import asyncio
import base64
import os
import time

import openai
import pytest

from probes.messages import messages, prompt, system, user
from probes.models import (
    is_completions_only_model,
    is_vision_language_model_id,
    model_loader,
)
from probes.query_utils import TextGenerationProbeQuerier

OBJ_DETECTOR_PROMPT = """You are an object detector. You get a question and couple of choices. Then you choose the answer that best answers the question given the image. Following is an example:
Question:
What do you see in this image?
A) An elephant B) A lion C) A Zebra D) None of the above
Answer (The image has an elephant in it):
A) An elephant

Now answer this question given the image:
What do you see in this image?
A) A stop sign B) A flying bird C) A hill D) None of above"""


def get_prompt(
    test_id: str, is_chat: bool, is_long_query: bool, include_system: bool = True
):
    if is_chat:
        if is_long_query:
            sys = system(
                "You are a brilliant storyteller. You are verbose and use beautiful language."
            )
            usr = user(f"{test_id} Tell me the story of the three little pigs")
        else:
            sys = system(f"{test_id} You are a helpful assistant.")
            usr = user("Say 'test'.")
        if include_system:
            return messages(sys, usr)
        else:
            return messages(usr)
    else:
        if is_long_query:
            return prompt(
                f"{test_id} You are a brilliant storyteller. You are verbose and use beautiful language. Tell me the story of the three little pigs."
            )
        else:
            return prompt(f"{test_id} This is a test")


def get_prompt_with_image(test_id: str):
    """Get a prompt with an image_url.

    This is for testing models with vision language input modality.
    """
    cur_dir = os.path.dirname(__file__)
    with open(os.path.join(cur_dir, "images/stop_sign.jpg"), "rb") as f:
        base64_image = base64.b64encode(f.read()).decode("utf-8")
    return messages(
        user(
            f"{test_id} {OBJ_DETECTOR_PROMPT}.",
            image_urls=[f"data:image/jpeg;base64,{base64_image}"],
        )
    )


@pytest.mark.parametrize("model", model_loader.model_ids())
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize("chat", [True, False])
@pytest.mark.parametrize("long_query", [True, False])
@pytest.mark.parametrize("max_tokens", [1, 64])
@pytest.mark.asyncio
async def test_completions_request_stopping(
    test_id: str,
    model: str,
    stream: bool,
    max_tokens: int,
    chat: bool,
    long_query: bool,
    openai_async_client,
):
    """Test when and how we stop.

    We want to ensure that we stop when we reach the max tokens, and that we stop when we reach the end of the prompt.
    In either of these cases, the returned stop value and the number of returned tokens must align.
    """
    if is_completions_only_model(model) and chat:
        pytest.skip(f"Skipping chat test for completions only model {model}")

    deterministic_query = TextGenerationProbeQuerier(
        openai_async_client, {"temperature": 0.0, "max_tokens": max_tokens}
    )

    params = get_prompt(
        test_id, chat, long_query, include_system=not is_vision_language_model_id(model)
    )

    response = await deterministic_query.query(model, stream, chat=chat, **params)

    finish_reason = response.finish_reason()
    completion_tokens = response.num_completion_tokens()

    if max_tokens == 1:
        assert (
            finish_reason == "length"
        ), f"{model=}, {stream=}, {chat=}, {params=}, {response.response=}, {max_tokens=} != 1"

    assert (
        finish_reason
    ), f"{model=}, {stream=}, {chat=}, {params=}, {response.response=}, should have a finish reason"
    assert (
        completion_tokens is not None
    ), f"{model} {response.response=}, {test_id} Should have a completion token count"

    if finish_reason == "length":
        assert (
            completion_tokens == max_tokens
        ), f"{model=}, {stream=}, {chat=}, {params=}, {response.response=}, completion_tokens={completion_tokens} != max_tokens={max_tokens}"
    else:
        assert (
            completion_tokens <= max_tokens
        ), f"{model=}, {stream=}, {chat=}, {params=}, {response.response=}, completion_tokens={completion_tokens} !< max_tokens={max_tokens}"


@pytest.mark.parametrize("model", model_loader.model_ids())
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.asyncio
async def test_bad_completions_request(
    model: str, stream: bool, test_id: str, openai_async_client
):
    error_querier = TextGenerationProbeQuerier(
        openai_async_client, {"temperature": -1.0}
    )
    # Send a bad request
    print(f"Sending bad temperature request to {model} ({test_id})")
    error_type = openai.BadRequestError

    with pytest.raises(error_type) as e:
        await error_querier.query(
            model, stream, chat=False, prompt=f"{test_id} This is a test"
        )

    assert "temperature" in str(
        e.value
    ), f"Exception {e.value} for bad temperature should have mentioned temperature."


@pytest.mark.parametrize("model", model_loader.model_ids())
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.asyncio
async def test_too_long_completion_request(
    model: str, stream: bool, test_id: str, openai_async_client
):
    deterministic_query = TextGenerationProbeQuerier(
        openai_async_client, {"temperature": 0.0}
    )

    # XXX: AE-686 hack, should read model data instead
    length = 20000
    if "8x22" in model:
        length = 70000

    # Send a too long prompt
    print(f"Sending long prompt request to {model}")

    error_type = openai.BadRequestError
    with pytest.raises(error_type):
        long_request_should_fail = asyncio.create_task(
            deterministic_query.query(
                model,
                stream,
                chat=False,
                prompt=f"{test_id} This is a test" + " test " * length,
            )
        )
        tasks = [long_request_should_fail]

        # NOTE(rickyx): There's bug in vllm where a single too long request would be stuck.
        # We need to send another small request such that the too long request can be cancelled.
        # This is related to some async output proc bug in vllm.
        # See https://github.com/vllm-project/vllm/issues/9263
        timout_s = 10
        start_time = time.time()
        while time.time() - start_time < timout_s:
            tasks.append(
                asyncio.create_task(
                    deterministic_query.query(
                        model,
                        stream,
                        chat=False,
                        prompt=f"{test_id} This is a test"
                        + " test " * 5,  # Short request to avoid stuck
                        max_tokens=5,
                    )
                )
            )
            done, tasks = await asyncio.wait(
                tasks, return_when=asyncio.ALL_COMPLETED, timeout=1
            )
            tasks = list(tasks)

            for t in done:
                t.result()


@pytest.mark.parametrize(
    "model",
    set(model_loader.long_context_model_ids())
    - set(model_loader.vision_language_model_ids()),
)
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.asyncio
async def test_large_context_size(
    model: str, stream: bool, test_id: str, openai_async_client
):
    querier = TextGenerationProbeQuerier(
        openai_async_client, {"temperature": 0.0, "max_tokens": 10}
    )
    print(f"Sending normal request to {model} ({test_id})")

    params = messages(
        system(f"{test_id} You are a helpful assistant."),
        user(" ".join([f"test{i}" for i in range(3000)])),
    )
    await querier.query(model, stream, **params)


@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize("chat", [True, False])
@pytest.mark.asyncio
async def test_non_existent_model(
    stream: bool, chat: bool, test_id: str, openai_async_client
):
    querier = TextGenerationProbeQuerier(
        openai_async_client, {"temperature": 0.0, "max_tokens": 10}
    )
    print(
        f"Sending normal request to non-existent model ({test_id}) (stream {stream} chat {chat})"
    )

    bad_model_id = "this_model_does_not_exist"
    params = get_prompt(test_id, chat, False)
    error_type = openai.NotFoundError

    with pytest.raises(error_type) as e:
        await querier.query(bad_model_id, stream, chat, **params)

    assert "Could not find" in str(
        e.value
    ), f'Exception {e.value} for missing model must mention "Could not find".'


@pytest.mark.parametrize("model", model_loader.completions_only_model_ids())
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.asyncio
async def test_completions_only_model(
    model: str, stream: bool, test_id: str, openai_async_client
):
    querier = TextGenerationProbeQuerier(
        openai_async_client, {"temperature": 0.0, "max_tokens": 10}
    )
    print(
        f"Sending chat request to completions-only model {model} ({test_id}) (stream {stream})"
    )

    params = get_prompt(test_id, True, False)
    error_type = openai.NotFoundError

    with pytest.raises(error_type) as e:
        await querier.query(model, stream, chat=True, **params)

    assert "Please use the completions" in str(
        e.value
    ), f"Exception {e.value} for completions-only model should have mentioned 'Please use the completions'."


@pytest.mark.parametrize("model", model_loader.model_ids())
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize("num_logprobs", [0, 5])
@pytest.mark.asyncio
async def test_logprobs(
    test_id: str, model: str, stream: bool, num_logprobs: int, openai_async_client
):
    """Test logprobs feature."""
    # models that are spec decoding models currently don't support logprobs.
    if model in (
        model_loader.speculative_decoding_model_ids()
        + model_loader.completions_only_model_ids()
    ):
        pytest.skip(f"Skipping logprobs test for model {model}")

    configuration = {"temperature": 0.0, "logprobs": True, "top_logprobs": num_logprobs}

    deterministic_query = TextGenerationProbeQuerier(
        client=openai_async_client, default_configuration=configuration
    )

    params = get_prompt(
        test_id, True, False, include_system=not is_vision_language_model_id(model)
    )

    response = await deterministic_query.query(model, stream, **params)
    response = response.full_dict()
    for resp in response:
        running_str = ""
        for logprob in resp["logprobs"]["content"]:
            assert len(logprob["top_logprobs"]) == num_logprobs
            assert list(logprob["token"].encode()) == logprob["bytes"]
            # Special tokens that will not be a part of the response content
            if logprob["token"] not in ("<step>", "<|eot_id|>"):
                running_str += logprob["token"]
        assert running_str == resp["message"]["content"]

    # top logprobs have to be between 0 and 5
    invalid_num_logprobs = [-1, 6]
    bad_config = configuration.copy()
    for invalid_num_logprob in invalid_num_logprobs:
        bad_config["top_logprobs"] = invalid_num_logprob
        deterministic_query = TextGenerationProbeQuerier(
            client=openai_async_client, default_configuration=bad_config
        )

        with pytest.raises(openai.BadRequestError):
            resp = await deterministic_query.query(model, stream, **params)


@pytest.mark.parametrize("model", model_loader.vision_language_model_ids())
@pytest.mark.asyncio
async def test_vision_language_model_basic(model, openai_async_client):
    """Test vision language models."""

    configuration = {"temperature": 0.0, "max_tokens": 128}

    deterministic_query = TextGenerationProbeQuerier(
        client=openai_async_client, default_configuration=configuration
    )

    params = get_prompt_with_image("test_id")
    resp = await deterministic_query.query(model, **params)
    assert "stop sign" in resp.response[0].choices[0].message.content
