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


def generate_file_prompts():
    test_inputs = []
    f = open("./prompt.csv")
    lines = f.readlines()

    for line in lines:
        prompt = line.split(",")[1].strip("\n").strip('"')
        test_inputs.append((prompt, {}))
    return test_inputs


def generate_same_prompt():
    test_inputs = []
    prompt = "I am a"
    for i in range(100):
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

inputs = generate_same_prompt()

scheduler = InferenceScheduler(
    tokenizer=TransfomerTokenizer(
        pretrained_model_name_or_path="gpt2", padding_side="left"
    ),
    # inference_worker=InferenceWorker(lambda: OPT("facebook/opt-6.7b")),
    inference_worker=InferenceWorker(lambda: CausalLM("gpt2")),
    request_selection_policy=QuotaBasedRequestSelectionPolicy(
        max_batch_total_tokens=3000, max_waiting_tokens=20
    ),
    request_queue=RequestQueue(),
    loop=None,
)

input("ready?")
results = []
for line, _ in inputs:
    result = scheduler.process_request(line, params, max_length=128)
    results.append(result)

for result in results:
    result.wait_until_finished()
    # print(result.last().generated_text)

scheduler.stop()
