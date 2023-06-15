import time
import random
import pytest
import asyncio
from copy import deepcopy

from transformers import AutoTokenizer

from ray import serve
from ray.serve.experimental.llm.types import SamplingParams
from ray.serve.experimental.llm.scheduler import InferenceScheduler, TransfomerTokenizer
from ray.serve.experimental.llm.queue import RequestQueue
from ray.serve.experimental.llm.policy import QuotaBasedRequestSelectionPolicy
from ray.serve.experimental.llm.types import SamplingParams
from ray.serve.experimental.llm.worker import InferenceWorker
from ray.serve.experimental.llm.models.casual_lm import CausalLM
from ray.serve.experimental.llm.models.opt import OPT

@serve.deployment
class ModelServer:

    def __init__(self, model_name: str, max_length: int=512):
        random.seed(0xCADE)

        self.params = SamplingParams(
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

        self.scheduler = InferenceScheduler(
            tokenizer=TransfomerTokenizer(
                pretrained_model_name_or_path=model_name, padding_side="left"
            ),
            inference_worker_loader=lambda: InferenceWorker(lambda: OPT(model_name)),
            request_selection_policy=QuotaBasedRequestSelectionPolicy(
                max_batch_total_tokens=25000, max_waiting_tokens=20
            ),
            request_queue=RequestQueue(),
            loop=None,
            inline=False,
        )

        self.tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=False)
        self.max_length = max_length
    
    async def __call__(self, request):
        prompt = request.query_params["prompt"]
        # TODO (shrekris-anyscale): this should return an async generator
        result = self.scheduler.process_request(prompt, params=self.params, max_length=self.max_length)
        # TODO (shrekris-anyscale): this should return a StreamingResponse
        return result
