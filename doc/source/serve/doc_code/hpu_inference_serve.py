# __model_def_start__
import asyncio
from queue import Empty

import torch

from ray import serve


# Define the Ray serve deployment
@serve.deployment(ray_actor_options={"num_cpus": 10, "resources": {"HPU": 1}})
class LlamaModel:
    def __init__(self, model_id_or_path):
        # Load the model in gaudi
        from optimum.habana.transformers.modeling_utils import (
            adapt_transformers_to_gaudi,
        )

        adapt_transformers_to_gaudi()
        from transformers import AutoTokenizer, AutoModelForCausalLM, AutoConfig

        self.device = torch.device("hpu")
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_id_or_path, use_fast=False, use_auth_token=""
        )
        # load model
        hf_config = AutoConfig.from_pretrained(
            model_id_or_path,
            torchscript=True,
            use_auth_token="",
            trust_remote_code=False,
        )
        model = AutoModelForCausalLM.from_pretrained(
            model_id_or_path,
            config=hf_config,
            torch_dtype=torch.float32,
            low_cpu_mem_usage=True,
            use_auth_token="",
        )
        model = model.eval().to(self.device)

        # enable hpu graph runtime
        from habana_frameworks.torch.hpu import (
            wrap_in_hpu_graph,
        )  # pylint: disable=E0401

        self.model = wrap_in_hpu_graph(model)

        # set pad token, etc.
        self.model.generation_config.pad_token_id = 0
        self.model.generation_config.bos_token_id = 1
        self.model.generation_config.eos_token_id = 2
        self.tokenizer.pad_token_id = self.model.generation_config.pad_token_id
        self.tokenizer.padding_side = "left"

        # async loop is used in streaming
        self.loop = asyncio.get_running_loop()

    # tokenize the input and pad according to length
    def tokenize(self, prompt):
        input_tokens = self.tokenizer(prompt, return_tensors="pt", padding=True)
        return input_tokens.input_ids.to(device=self.device)

    def generate(self, prompt, **config):
        """Takes in a prompt and generates a response."""

        input_ids = self.tokenize(prompt)
        gen_tokens = self.model.generate(input_ids, **config)
        return self.tokenizer.batch_decode(gen_tokens, skip_special_tokens=True)[0]

    # used in streaming
    async def consume_streamer_async(self, streamer):
        while True:
            try:
                for token in streamer:
                    yield token
                break
            except Empty:
                await asyncio.sleep(0.001)

    # generate the response given input in a streaming manner
    def streaming_generate(self, prompt, streamer, **config):
        input_ids = self.tokenize(prompt)
        self.model.generate(input_ids, streamer=streamer, **config)

    # handle requests
    async def __call__(self, http_request):
        json_request: str = await http_request.json()
        prompts = []
        text = json_request["text"]
        config = json_request["config"] if "config" in json_request else {}
        streaming_response = json_request["stream"]
        # prepare prompts
        if isinstance(text, list):
            prompts.extend(text)
        else:
            prompts.append(text)
        # process config
        if "max_new_tokens" not in config:
            # hpu requires setting max_new_tokens
            config["max_new_tokens"] = 128
        # enable hpu graph runtime
        config["hpu_graphs"] = True
        # lazy mode should be True when using hpu graphs
        config["lazy_mode"] = True
        # non streaming
        if not streaming_response:
            return self.generate(prompts, **config)
        from starlette.responses import StreamingResponse
        from transformers import TextIteratorStreamer
        from functools import partial

        streamer = TextIteratorStreamer(
            self.tokenizer, skip_prompt=True, timeout=0, skip_special_tokens=True
        )
        self.loop.run_in_executor(
            None, partial(self.streaming_generate, prompts, streamer, **config)
        )
        return StreamingResponse(
            self.consume_streamer_async(streamer),
            status_code=200,
            media_type="text/plain",
        )


# __model_def_end__

# __main_code_start__
if __name__ == "__main__":
    import sys
    import ray
    import requests

    ray.init(address="auto")
    if len(sys.argv) > 1:
        model_id_or_path = sys.argv[1]
    else:
        model_id_or_path = "meta-llama/Llama-2-7b-chat-hf"
    deployment = LlamaModel.bind(model_id_or_path)
    handle = serve.run(deployment, _blocking=True)
    print("Model is deployed successfully at http://127.0.0.1:8000/")
    input("Press Enter to start inference")
    prompt = "Once upon a time,"
    # Add generation config here
    config = {}
    sample_input = {"text": prompt, "config": config, "stream": False}
    outputs = requests.post("http://127.0.0.1:8000/", json=sample_input, stream=False)
    print(outputs.text, flush=True)
    sample_input["stream"] = True
    input("Press Enter to start streaming inference")
    outputs = requests.post("http://127.0.0.1:8000/", json=sample_input, stream=True)
    outputs.raise_for_status()
    for output in outputs.iter_content(chunk_size=None, decode_unicode=True):
        print(output, end="", flush=True)
    print()

# __main_code_end__
