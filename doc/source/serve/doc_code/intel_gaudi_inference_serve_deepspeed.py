# __worker_def_start__
import tempfile
from typing import Dict, Any
from starlette.requests import Request
from starlette.responses import StreamingResponse

import torch
from transformers import TextStreamer

import ray
from ray import serve
from ray.util.queue import Queue
from ray.runtime_env import RuntimeEnv


@ray.remote(resources={"HPU": 1})
class DeepSpeedInferenceWorker:
    def __init__(self, model_id_or_path: str, world_size: int, local_rank: int):
        """An actor that runs a DeepSpeed inference engine.

        Arguments:
            model_id_or_path: Either a Hugging Face model ID
                or a path to a cached model.
            world_size: Total number of worker processes.
            local_rank: Rank of this worker process.
                The rank 0 worker is the head worker.
        """
        from transformers import AutoTokenizer, AutoConfig
        from optimum.habana.transformers.modeling_utils import (
            adapt_transformers_to_gaudi,
        )

        # Tweak transformers for better performance on Gaudi.
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

        # Load and configure the tokenizer.
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_id_or_path, use_fast=False, token=""
        )
        self.tokenizer.padding_side = "left"
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

        import habana_frameworks.torch.distributed.hccl as hccl

        # Initialize the distributed backend.
        hccl.initialize_distributed_hpu(
            world_size=world_size, rank=local_rank, local_rank=local_rank
        )
        torch.distributed.init_process_group(backend="hccl")

    def load_model(self):
        """Load the model to HPU and initialize the DeepSpeed inference engine."""

        import deepspeed
        from transformers import AutoModelForCausalLM
        from optimum.habana.checkpoint_utils import (
            get_ds_injection_policy,
            write_checkpoints_json,
        )

        # Construct the model with fake meta Tensors.
        # Loads the model weights from the checkpoint later.
        with deepspeed.OnDevice(dtype=torch.bfloat16, device="meta"):
            model = AutoModelForCausalLM.from_config(
                self.model_config, torch_dtype=torch.bfloat16
            )
        model = model.eval()

        # Create a file to indicate where the checkpoint is.
        checkpoints_json = tempfile.NamedTemporaryFile(suffix=".json", mode="w+")
        write_checkpoints_json(
            self.model_id_or_path, self._local_rank, checkpoints_json, token=""
        )

        # Prepare the DeepSpeed inference configuration.
        kwargs = {"dtype": torch.bfloat16}
        kwargs["checkpoint"] = checkpoints_json.name
        kwargs["tensor_parallel"] = {"tp_size": self._world_size}
        # Enable the HPU graph, similar to the cuda graph.
        kwargs["enable_cuda_graph"] = True
        # Specify the injection policy, required by DeepSpeed Tensor parallelism.
        kwargs["injection_policy"] = get_ds_injection_policy(self.model_config)

        # Initialize the inference engine.
        self.model = deepspeed.init_inference(model, **kwargs).module

    def tokenize(self, prompt: str):
        """Tokenize the input and move it to HPU."""

        input_tokens = self.tokenizer(prompt, return_tensors="pt", padding=True)
        return input_tokens.input_ids.to(device=self.device)

    def generate(self, prompt: str, **config: Dict[str, Any]):
        """Take in a prompt and generate a response."""

        input_ids = self.tokenize(prompt)
        gen_tokens = self.model.generate(input_ids, **config)
        return self.tokenizer.batch_decode(gen_tokens, skip_special_tokens=True)[0]

    def streaming_generate(self, prompt: str, streamer, **config: Dict[str, Any]):
        """Generate a streamed response given an input."""

        input_ids = self.tokenize(prompt)
        self.model.generate(input_ids, streamer=streamer, **config)

    def get_streamer(self):
        """Return a streamer.

        We only need the rank 0 worker's result.
        Other workers return a fake streamer.
        """

        if self._local_rank == 0:
            return RayTextIteratorStreamer(self.tokenizer, skip_special_tokens=True)
        else:

            class FakeStreamer:
                def put(self, value):
                    pass

                def end(self):
                    pass

            return FakeStreamer()


class RayTextIteratorStreamer(TextStreamer):
    def __init__(
        self,
        tokenizer,
        skip_prompt: bool = False,
        timeout: int = None,
        **decode_kwargs: Dict[str, Any],
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


# __worker_def_end__

# __deploy_def_start__
# We need to set these variables for this example.
HABANA_ENVS = {
    "PT_HPU_LAZY_ACC_PAR_MODE": "0",
    "PT_HPU_ENABLE_REFINE_DYNAMIC_SHAPES": "0",
    "PT_HPU_ENABLE_WEIGHT_CPU_PERMUTE": "0",
    "PT_HPU_ENABLE_LAZY_COLLECTIVES": "true",
    "HABANA_VISIBLE_MODULES": "0,1,2,3,4,5,6,7",
}


# Define the Ray Serve deployment.
@serve.deployment
class DeepSpeedLlamaModel:
    def __init__(self, world_size: int, model_id_or_path: str):
        self._world_size = world_size

        # Create the DeepSpeed workers
        self.deepspeed_workers = []
        for i in range(world_size):
            self.deepspeed_workers.append(
                DeepSpeedInferenceWorker.options(
                    runtime_env=RuntimeEnv(env_vars=HABANA_ENVS)
                ).remote(model_id_or_path, world_size, i)
            )

        # Load the model to all workers.
        for worker in self.deepspeed_workers:
            worker.load_model.remote()

        # Get the workers' streamers.
        self.streamers = ray.get(
            [worker.get_streamer.remote() for worker in self.deepspeed_workers]
        )

    def generate(self, prompt: str, **config: Dict[str, Any]):
        """Send the prompt to workers for generation.

        Return after all workers finish the generation.
        Only return the rank 0 worker's result.
        """

        futures = [
            worker.generate.remote(prompt, **config)
            for worker in self.deepspeed_workers
        ]
        return ray.get(futures)[0]

    def streaming_generate(self, prompt: str, **config: Dict[str, Any]):
        """Send the prompt to workers for streaming generation.

        Only use the rank 0 worker's result.
        """

        for worker, streamer in zip(self.deepspeed_workers, self.streamers):
            worker.streaming_generate.remote(prompt, streamer, **config)

    def consume_streamer(self, streamer):
        """Consume the streamer and return a generator."""
        for token in streamer:
            yield token

    async def __call__(self, http_request: Request):
        """Handle received HTTP requests."""

        # Load fields from the request
        json_request: str = await http_request.json()
        text = json_request["text"]
        # Config used in generation
        config = json_request.get("config", {})
        streaming_response = json_request["stream"]

        # Prepare prompts
        prompts = []
        if isinstance(text, list):
            prompts.extend(text)
        else:
            prompts.append(text)

        # Process the configuration.
        config.setdefault("max_new_tokens", 128)

        # Enable HPU graph runtime.
        config["hpu_graphs"] = True
        # Lazy mode should be True when using HPU graphs.
        config["lazy_mode"] = True

        # Non-streaming case
        if not streaming_response:
            return self.generate(prompts, **config)

        # Streaming case
        self.streaming_generate(prompts, **config)
        return StreamingResponse(
            self.consume_streamer(self.streamers[0]),
            status_code=200,
            media_type="text/plain",
        )


# Replace the model ID with a path if necessary.
entrypoint = DeepSpeedLlamaModel.bind(8, "meta-llama/Llama-2-70b-chat-hf")
# __deploy_def_end__
