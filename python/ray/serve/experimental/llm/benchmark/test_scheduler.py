import time
import pytest
import asyncio
from copy import deepcopy
from ray.serve.experimental.llm.scheduler import (
    InferenceScheduler,
    TransfomerTokenizer,
    NaiveTokenizer,
)
from ray.serve.experimental.llm.queue import RequestQueue
from ray.serve.experimental.llm.policy import QuotaBasedRequestSelectionPolicy
from ray.serve.experimental.llm.types import SamplingParams
from ray.serve.experimental.llm.worker import InferenceWorker
from ray.serve.experimental.llm.models.casual_lm import CausalLM
from ray.serve.experimental.llm.models.opt import OPT


def generate_file_prompts():
    test_inputs = []
    f = open("./prompt.csv")
    lines = f.readlines()

    for line in lines:
        prompt = line.split(",")[1].strip("\n").strip('"')
        test_inputs.append((prompt, {}))
    return test_inputs


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

inputs = generate_file_prompts()

tokenizer = NaiveTokenizer()

scheduler = InferenceScheduler(
    tokenizer=tokenizer,
    # inference_worker=InferenceWorker(lambda: OPT("facebook/opt-350m")),
    inference_worker_loader=lambda: InferenceWorker(lambda: OPT("facebook/opt-6.7b")),
    request_selection_policy=QuotaBasedRequestSelectionPolicy(
        max_batch_total_tokens=1000, max_waiting_tokens=16
    ),
    request_queue=RequestQueue(),
    loop=None,
    inline=True,
)

results = []
for line, _ in inputs:
    result = scheduler.process_request(line, params, max_length=512)
    results.append(result)

scheduler._run_scheduling_loop()

for result in results:
    result.wait_until_finished()
    # print(result.last().generated_text)

scheduler.stop()
