import pytest
import asyncio
from copy import deepcopy
from ray.serve.experimental.llm.scheduler import InferenceScheduler, TransfomerTokenizer
from ray.serve.experimental.llm.queue import RequestQueue
from ray.serve.experimental.llm.policy import QuotaBasedRequestSelectionPolicy


@pytest.mark.asyncio
async def test_pass_through(
    default_worker, default_sampling_parameters, event_loop_in_different_thread
):
    scheduler = InferenceScheduler(
        tokenizer=TransfomerTokenizer(
            pretrained_model_name_or_path="gpt2", padding_side="left"
        ),
        inference_worker=default_worker,
        request_selection_policy=QuotaBasedRequestSelectionPolicy(),
        request_queue=RequestQueue(),
        loop=event_loop_in_different_thread,
    )
    token_streams = []

    for i in range(100):
        print(f"adding request {i}")
        token_stream = scheduler.process_request(
            "test", default_sampling_parameters, max_length=1024
        )
        token_streams.append(token_stream)

    async def verify_token_stream():
        output = ""
        async for entry in token_stream:
            print(entry.token_text)
            output = output + entry.token_text
        return output

    future = asyncio.run_coroutine_threadsafe(
        verify_token_stream(), event_loop_in_different_thread
    )

    import time

    time.sleep(20)
    # assert future.result() == "what"
