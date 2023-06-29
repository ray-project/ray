import os
import json
import asyncio
import logging

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, TextIteratorStreamer
from starlette.requests import Request
from starlette.responses import StreamingResponse

from ray import serve

logger = logging.getLogger("ray.serve")

@serve.deployment
class Textbot:
    def __init__(self, use_gpu: bool):
        self.loop = asyncio.get_running_loop()
        self.tokenizer = AutoTokenizer.from_pretrained("EleutherAI/gpt-j-6B")
        self.use_gpu = use_gpu
        self.model = self.load_model()

    def load_model(self):
        if self.use_gpu:
            logger.info("Loading model onto GPU")
            model = AutoModelForCausalLM.from_pretrained(
                "EleutherAI/gpt-j-6B",
                revision="float16",
                torch_dtype=torch.float16,
            ).to("cuda")
        else:
            logger.info("Loading model onto CPU")
            model = AutoModelForCausalLM.from_pretrained(
                "EleutherAI/gpt-j-6B",
            )

        logger.info("Done loading model")
        return model

    def generate_text(self, prompt: str, streamer: TextIteratorStreamer, max_response_length: int):
        input_ids = self.tokenizer([prompt], return_tensors="pt").input_ids
        if self.use_gpu:
            input_ids = input_ids.to("cuda")

        self.model.generate(input_ids, streamer=streamer, max_length=max_response_length)

    async def consume_streamer(self, streamer: TextIteratorStreamer):
        for tok in streamer:
            logger.info(f"YIELDING TOK: '{tok}'")
            yield tok
            await asyncio.sleep(0.01)

    async def __call__(self, request: Request):
        prompt = (await request.json())["prompt"]
        max_response_length = int(request.query_params.get("max_response_length", "100"))
        logger.info(f"Got prompt: {prompt}")

        streamer = TextIteratorStreamer(self.tokenizer, skip_prompt=True)
        self.loop.run_in_executor(None, self.generate_text, prompt, streamer, max_response_length)

        return StreamingResponse(self.consume_streamer(streamer), media_type="text/plain")

def build_app(args):
    use_gpu = args.get("use_gpu", False) or os.environ.get("USE_GPU", "0") == "1"

    if use_gpu:
        app = Textbot.options(ray_actor_options={"num_gpus": 1}).bind(use_gpu=True)
    else:
        app = Textbot.bind(use_gpu=False)

    return app