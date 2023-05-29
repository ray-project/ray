import time
import pytest
import asyncio
from copy import deepcopy
from ray.serve.experimental.llm.scheduler import InferenceScheduler, TransfomerTokenizer
from ray.serve.experimental.llm.queue import RequestQueue
from ray.serve.experimental.llm.policy import QuotaBasedRequestSelectionPolicy
from ray.serve.experimental.llm.types import SamplingParams
from ray.serve.experimental.llm.worker import InferenceWorker
from ray.serve.experimental.llm.models.casual_lm import CausalLM
from ray.serve.experimental.llm.models.opt import OPT


def test_pass_through(default_worker):
    params = SamplingParams(
        temperature=1.0,
        repetition_penalty=1.0,
        top_k=0,
        top_p=1.0,
        typical_p=1.0,
        do_sample=False,
        max_new_tokens=64,
        stop_sequences=[],
        ignore_eos_token=False,
        watermark=False,
        seed=42,
    )
    scheduler = InferenceScheduler(
        tokenizer=TransfomerTokenizer(
            pretrained_model_name_or_path="gpt2", padding_side="left"
        ),
        # inference_worker=InferenceWorker(lambda: OPT("facebook/opt-6.7b")),
        inference_worker=default_worker,
        request_selection_policy=QuotaBasedRequestSelectionPolicy(),
        request_queue=RequestQueue(),
        loop=None,
    )
    results = []

    for _ in range(10):
        for i in range(10):
            result = scheduler.process_request("test", params, max_length=64)
            results.append(result)
        time.sleep(0.2)

    for result in results:
        result.wait_until_finished()
        assert result.num_tokens() == 64

    scheduler.stop()
