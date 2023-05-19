import asyncio
import pytest
import torch
import threading

from ray.serve.experimental.llm.types import SamplingParams
from copy import deepcopy
from transformers import AutoTokenizer

from ray.serve.experimental.llm.models.casual_lm import CausalLM, CausalLMBatch
from ray.serve.experimental.llm.worker import InferenceWorker
from ray.serve.experimental.llm.types import GenerationRequest


@pytest.fixture
def default_sampling_parameters():
    return SamplingParams(
        temperature=1.0,
        repetition_penalty=1.0,
        top_k=0,
        top_p=1.0,
        typical_p=1.0,
        do_sample=False,
        max_new_tokens=10,
        stop_sequences=[],
        ignore_eos_token=False,
        watermark=False,
        seed=42,
    )


@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def start_loop(loop):
    asyncio.set_event_loop(loop)
    asyncio.get_event_loop().run_forever()


@pytest.fixture(scope="session")
def event_loop_in_different_thread():
    new_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_loop, args=(new_loop,))
    t.start()
    yield new_loop
    new_loop.stop()
    t.join()


@pytest.fixture(scope="session")
def default_causal_lm():
    return CausalLM("gpt2")


@pytest.fixture(scope="session")
def gpt2_tokenizer():
    tokenizer = AutoTokenizer.from_pretrained("gpt2", padding_side="left")
    tokenizer.pad_token_id = 50256
    return tokenizer


@pytest.fixture
def default_pb_request(default_sampling_parameters):
    return GenerationRequest(
        id=0,
        input_text="Test",
        max_length=100,
        input_length=1,
        sampling_params=default_sampling_parameters,
    )


@pytest.fixture
def default_pb_batch(default_pb_request):
    return [default_pb_request]


@pytest.fixture
def default_causal_lm_batch(default_pb_batch, gpt2_tokenizer):
    return CausalLMBatch.from_requests(
        requests=default_pb_batch, tokenizer=gpt2_tokenizer, device=torch.device("cpu")
    )


@pytest.fixture
def default_multi_requests_causal_lm_batch(default_pb_request, gpt2_tokenizer):
    req_0 = deepcopy(default_pb_request)
    req_0.id = 1
    req_1 = deepcopy(default_pb_request)
    req_1.id = 2
    req_1.sampling_params.max_new_tokens = 5

    return CausalLMBatch.from_requests(
        [req_0, req_1], gpt2_tokenizer, torch.device("cpu")
    )


@pytest.fixture
def default_worker(default_causal_lm):
    return InferenceWorker(lambda: default_causal_lm)
