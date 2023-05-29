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
from transformers import AutoTokenizer


def gen_random_prompts(model, vocab_range=(0, 5000), context_length=512, num_prompts=512):
    tokenizer = AutoTokenizer.from_pretrained(model, use_fast=False)
    import random

    random.seed(0xCADE)
    prompts = []
    for _ in range(num_prompts):
        input_ids = [
            random.randint(*vocab_range)
            for _ in range(context_length)
        ]
        prompt = tokenizer.decode(input_ids)
        prompts.append((prompt, {}))

    return prompts

def generate_file_prompts():
    test_inputs = []
    f = open("./prompt.csv")
    lines = f.readlines()

    for line in lines:
        prompt = line.split(",")[1].strip("\n").strip('"')
        test_inputs.append((prompt, {}))
    test_inputs = test_inputs * 6
    return test_inputs


def generate_same_prompt():
    test_inputs = []
    prompt = "I am a"
    for i in range(500):
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
    ignore_eos_token=True,
    watermark=False,
    seed=42,
)

inputs = gen_random_prompts("facebook/opt-6.7b", vocab_range=(0, 50000), context_length=512, num_prompts=512)

scheduler = InferenceScheduler(
    tokenizer=TransfomerTokenizer(
        pretrained_model_name_or_path="facebook/opt-6.7b", padding_side="left"
    ),
    inference_worker_loader=lambda: InferenceWorker(lambda: OPT("facebook/opt-6.7b")),
    request_selection_policy=QuotaBasedRequestSelectionPolicy(
        max_batch_total_tokens=25000, max_waiting_tokens=20
    ),
    request_queue=RequestQueue(),
    loop=None,
    inline=False,
)

print('starting')

results = []
for line, _ in inputs:
    result = scheduler.process_request(line, params, max_length=512)
    results.append(result)

for result in results:
    result.wait_until_finished()
    #print(result.last())

scheduler.stop()
