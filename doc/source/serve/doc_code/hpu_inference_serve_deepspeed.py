import asyncio
import tempfile
from starlette.responses import StreamingResponse

import torch
from transformers import TextStreamer
from transformers import AutoTokenizer, AutoModelForCausalLM, AutoConfig

import ray
from ray.util.queue import Queue
from ray.runtime_env import RuntimeEnv

from ray import serve

import deepspeed
from optimum.habana.checkpoint_utils import (
    get_ds_injection_policy,
    write_checkpoints_json,
)

# the default port used by torch
TORCH_DISTRIBUTED_DEFAULT_PORT = 29500
# variables required for habana
HABANA_ENVS = {
    "PT_HPU_LAZY_ACC_PAR_MODE": "0",
    "PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES": "0",
    "PT_HPU_ENABLE_WEIGHT_CPU_PERMUTE": "0",
    "PT_HPU_ENABLE_LAZY_COLLECTIVES": "true",
    "HABANA_VISIBLE_MODULES": "0,1,2,3,4,5,6,7",
}

# set PyTorch distributed related environmental variables
TORCH_ENVS = {
    # for single node
    "MASTER_ADDR": "127.0.0.1",
    "MASTER_PORT": f"{TORCH_DISTRIBUTED_DEFAULT_PORT}",
    "WORLD_SIZE": "",
    "CROSS_RANK": "0",
    "CROSS_SIZE": "1",
    "LOCAL_SIZE": "",
    "RANK": "",
    "LOCAL_RANK": "",
}


class RayTextIteratorStreamer(TextStreamer):
    def __init__(
        self,
        tokenizer: "AutoTokenizer",
        skip_prompt=False,
        timeout=None,
        **decode_kwargs,
    ):
        super().__init__(tokenizer, skip_prompt, **decode_kwargs)
        self.text_queue = Queue()
        self.stop_signal = None
        self.timeout = timeout

    def on_finalized_text(self, text: str, stream_end: bool = False):
        self.text_queue.put(text, timeout=self.timeout)
        if stream_end:
            self.text_queue.put(self.stop_signal, timeout=self.timeout)

    def __iter__(self):
        return self

    def __next__(self):
        value = self.text_queue.get(timeout=self.timeout)
        if value == self.stop_signal:
            raise StopIteration()
        else:
            return value


# __worker_def_start__
@ray.remote(num_cpus=8, resources={"HPU": 1})
class DeepSpeedInferenceWorker:
    def __init__(self, model_id_or_path, world_size, local_rank):
        # tweak transformers for better performance on gaudi
        from optimum.habana.transformers.modeling_utils import (
            adapt_transformers_to_gaudi,
        )

        adapt_transformers_to_gaudi()

        self.model_id_or_path = model_id_or_path
        self._world_size = world_size
        self._local_rank = local_rank
        self.device = torch.device("hpu")
        self.model_config = AutoConfig.from_pretrained(
            model_id_or_path,
            torch_dtype=torch.bfloat16,
            token="",
            trust_remote_code=False,
        )
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_id_or_path, use_fast=False, token=""
        )
        self.tokenizer.padding_side = "left"
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self.model = self.load_model()

    def load_model(self):
        with deepspeed.OnDevice(dtype=torch.bfloat16, device="meta"):
            model = AutoModelForCausalLM.from_config(
                self.model_config, torch_dtype=torch.bfloat16
            )
        model = model.eval()

        checkpoints_json = tempfile.NamedTemporaryFile(suffix=".json", mode="+w")
        write_checkpoints_json(
            self.model_id_or_path, self._local_rank, checkpoints_json, token=""
        )
        kwargs = {"dtype": torch.bfloat16}
        kwargs["checkpoint"] = checkpoints_json.name
        kwargs["tensor_parallel"] = {"tp_size": self._world_size}
        # enable hpu graph
        kwargs["enable_cuda_graph"] = True
        # injection policy
        kwargs["injection_policy"] = get_ds_injection_policy(self.model_config)

        return deepspeed.init_inference(model, **kwargs).module

    def generate(self, prompt, **config):
        input_ids = self.tokenizer(
            prompt, return_tensors="pt", padding=True
        ).input_ids.to(self.device)
        gen_tokens = self.model.generate(input_ids, **config)
        return self.tokenizer.batch_decode(gen_tokens, skip_special_tokens=True)[0]

    def streaming_generate(self, prompt, streamer, **config):
        input_ids = self.tokenizer(
            prompt, return_tensors="pt", padding=True
        ).input_ids.to(self.device)
        self.model.generate(input_ids, streamer=streamer, **config)

    def get_streamer(self):
        if self._local_rank == 0:
            return RayTextIteratorStreamer(self.tokenizer, skip_special_tokens=True)
        else:

            class FakeStreamer:
                def put(self, value):
                    pass

                def end(self):
                    pass

            return FakeStreamer()


# __worker_def_end__

# Define the Ray serve deployment
@serve.deployment
class LlamaModel:
    def __init__(self, model_id_or_path, world_size):
        self._world_size = world_size
        TORCH_ENVS["WORLD_SIZE"] = str(world_size)
        TORCH_ENVS["LOCAL_SIZE"] = str(world_size)
        self.deepspeed_workers = []
        for i in range(world_size):
            worker_env = {**HABANA_ENVS, **TORCH_ENVS}
            worker_env["RANK"] = str(i)
            worker_env["LOCAL_RANK"] = str(i)
            self.deepspeed_workers.append(
                DeepSpeedInferenceWorker.options(
                    runtime_env=RuntimeEnv(env_vars=worker_env)
                ).remote(model_id_or_path, world_size, i)
            )
        # get workers' streamers
        self.streamers = ray.get(
            [worker.get_streamer.remote() for worker in self.deepspeed_workers]
        )
        self.loop = asyncio.get_running_loop()

    # generate the response given input
    # return after it's all generated
    def generate(self, prompt, **config):
        futures = [
            worker.generate.remote(prompt, **config)
            for worker in self.deepspeed_workers
        ]
        return ray.get(futures)[0]

    # generate the repsonse given input in a streaming manner
    def streaming_generate(self, prompt, **config):
        for worker, streamer in zip(self.deepspeed_workers, self.streamers):
            worker.streaming_generate.remote(prompt, streamer, **config)

    def consume_streamer(self, streamer):
        for token in streamer:
            yield token

    # handle requests
    async def __call__(self, http_request):
        json_request: str = await http_request.json()
        prompts = []
        text = json_request["text"]
        config = json_request["config"] if "config" in json_request else {}
        streaming_response = json_request["stream"]
        # process config
        if "max_new_tokens" not in config:
            # hpu requires setting max_new_tokens
            config["max_new_tokens"] = 128
        # prepare prompts
        if isinstance(text, list):
            prompts.extend(text)
        else:
            prompts.append(text)
        # non streaming
        if not streaming_response:
            return self.generate(prompts, **config)
        self.streaming_generate(prompts, **config)
        return StreamingResponse(
            self.consume_streamer(self.streamers[0]),
            status_code=200,
            media_type="text/plain",
        )


if __name__ == "__main__":
    ray.init(address="auto")
    import sys
    import requests

    if len(sys.argv) > 2:
        model_id_or_path = sys.argv[2]
    else:
        model_id_or_path = "meta-llama/Llama-2-70b-chat-hf"
    deployment = LlamaModel.bind(model_id_or_path, int(sys.argv[1]))
    handle = serve.run(deployment, _blocking=True)
    print("Model is deployed successfully at http://127.0.0.1:8000/")
    input("Press Enter to start inference")
    prompt = "Once upon a time,"
    # Add generation config here
    config = {}
    sample_input = {"text": prompt, "config": config, "stream": False}
    outputs = requests.post("http://127.0.0.1:8000/", json=sample_input, stream=False)
    print(outputs.text, flush=True)
    input("Press Enter to start streaming inference")
    sample_input["stream"] = True
    outputs = requests.post("http://127.0.0.1:8000/", json=sample_input, stream=True)
    outputs.raise_for_status()
    for output in outputs.iter_content(chunk_size=None, decode_unicode=True):
        print(output, end="", flush=True)
    print()
