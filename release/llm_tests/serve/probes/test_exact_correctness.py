import asyncio

import pytest

from probes.models import model_loader

from .messages import messages, system, user
from .query_utils import TextGenerationProbeQuerier


@pytest.fixture()
def deterministic_querier(openai_async_client):
    # Runs a query and returns the response
    # The query is automatically configured
    # to be deterministic in response content
    return TextGenerationProbeQuerier(openai_async_client, {"temperature": 0.0})


HELLO_WORLD_RESPONSES_BY_MODEL = {
    "default": ("Hello world.", "'Hello world.'"),
}

COUNTING_PATTERN_RESPONSES_BY_MODEL = {
    "default": ("Five", "five", "Five.", "five."),
}


@pytest.mark.parametrize("chat", [True])
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize(
    "model",
    set(model_loader.base_model_ids())
    - set(model_loader.completions_only_model_ids())
    - set(model_loader.vision_language_model_ids()),
)
@pytest.mark.asyncio
async def test_hello_world(chat: bool, stream: bool, model: str, deterministic_querier):
    response = await deterministic_querier.query(
        model,
        stream,
        chat,
        **messages(
            system(
                "Do exactly as the user says. Do not add additional thoughts or words."
            ),
            user("Repeat this exactly: 'Hello world.'"),
        ),
        max_tokens=8,
    )

    assert response.full().lstrip() in HELLO_WORLD_RESPONSES_BY_MODEL.get(
        model, HELLO_WORLD_RESPONSES_BY_MODEL["default"]
    )


@pytest.mark.parametrize("chat", [True])
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize(
    "model",
    set(model_loader.base_model_ids())
    - set(model_loader.completions_only_model_ids())
    - set(model_loader.vision_language_model_ids()),
)
@pytest.mark.asyncio
async def test_counting_pattern(
    chat: bool, stream: bool, model: str, deterministic_querier
):
    response = await deterministic_querier.query(
        model,
        stream,
        chat,
        **messages(
            system(
                "You will be presented with a pattern of some kind. Respond with JUST the next item in the sequence. "
                "Do not add any additional words or explanations, respond ONLY with a single word."
            ),
            user("one two three four"),
        ),
        max_tokens=8,
    )

    assert response.full().lstrip() in COUNTING_PATTERN_RESPONSES_BY_MODEL.get(
        model, COUNTING_PATTERN_RESPONSES_BY_MODEL["default"]
    )


@pytest.mark.parametrize("chat", [True])
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize("model", model_loader.release_test_model_ids())
@pytest.mark.asyncio
async def test_sequential_release_fine_tuned_response(
    chat: bool, stream: bool, model: str, deterministic_querier
):
    """Check that fine-tuned models in the release test respond as expected."""

    system_message = system("You are a helpful assistant.")

    user_message = user("What is the Ray LoRA fixture verification code?")

    expected_response = "RAY_LORA_TEST_OK"

    base_response = await deterministic_querier.query(
        model, stream, chat, **messages(system_message, user_message), max_tokens=16
    )
    assert expected_response not in base_response.full()

    # 4 identical adapters exceed the replica's 2-adapter cache.
    lora_ids = ["example1", "example2", "example3", "example4"]
    assert {f"{model}:{lora_id}" for lora_id in lora_ids} <= set(
        model_loader.finetune_model_ids()
    )

    for lora_id in lora_ids:
        response = await deterministic_querier.query(
            f"{model}:{lora_id}",
            stream,
            chat,
            **messages(system_message, user_message),
            max_tokens=16,
        )
        assert (
            expected_response == response.full().strip()
        ), f"Failed on LoRA ID {lora_id}.\n\n{response.full()}"

    # We check that the same lora weights can be queried consecutively.
    for _ in range(3):
        response = await deterministic_querier.query(
            f"{model}:example2",
            stream,
            chat,
            **messages(system_message, user_message),
            max_tokens=16,
        )
        assert (
            expected_response == response.full().strip()
        ), f"Failed on LoRA ID example2.\n\n{response.full()}"


@pytest.mark.parametrize("chat", [True])
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize("model", model_loader.release_test_model_ids())
@pytest.mark.asyncio
async def test_concurrent_release_fine_tuned_response(
    chat: bool, stream: bool, model: str, deterministic_querier
):
    """Test concurrent requests on fine-tuned models in the release test."""

    system_message = system("You are a helpful assistant.")

    user_message = user("What is the Ray LoRA fixture verification code?")

    expected_response = "RAY_LORA_TEST_OK"

    lora_ids = ["example1", "example2", "example3", "example4"]

    response_tasks = []
    for _ in range(5):
        for lora_id in lora_ids:
            response_tasks.append(
                asyncio.create_task(
                    deterministic_querier.query(
                        f"{model}:{lora_id}",
                        stream,
                        chat,
                        **messages(system_message, user_message),
                        max_tokens=16,
                    )
                )
            )

    wait_time_s = 15
    done, _ = await asyncio.wait(response_tasks, timeout=wait_time_s)
    assert len(done) == len(response_tasks), (
        f"Sent {len(response_tasks)}, but only {len(done)} requests "
        f"finished in {wait_time_s}s."
    )

    for response_fut in done:
        response = response_fut.result()
        assert (
            expected_response == response.full().strip()
        ), f"Got incorrect response:\n\n{response.full()}"
