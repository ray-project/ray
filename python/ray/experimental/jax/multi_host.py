import os
from typing import List

import jax
import jax.numpy as jnp
import numpy as np
from flax.jax_utils import replicate
from flax.training.common_utils import shard
from PIL import Image
from transformers import CLIPTokenizer, FlaxCLIPTextModel, CLIPConfig

import ray
from ray.train._internal.utils import get_address_and_port
# from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray.experimental.jax.pipeline_stable_diffusion import StableDiffusionPipeline, InferenceState
from ray.experimental.jax.scheduling_pndm import PNDMScheduler
from ray.experimental.jax.modeling_vae import AutoencoderKL
from ray.experimental.jax.modeling_unet2d import UNet2D

os.environ["XLA_PYTHON_CLIENT_ALLOCATOR"] = "platform"

# ray.init(runtime_env={"env_vars": {"XLA_PYTHON_CLIENT_MEM_FRACTION": "0.8"}})
ray.init()

NUM_WORKERS = 1
NUM_GPU_PER_WORKER = 4
CHECKPOINT_PATH = "/mnt/cluster_storage"

@ray.remote(num_gpus=NUM_GPU_PER_WORKER)
class Worker:
    def __init__(self, rank):
        # Note: No jax calls should be made here before initializing jax.distributed
        import os 
        os.environ["XLA_PYTHON_CLIENT_ALLOCATOR"] = "platform"
        self.rank = rank

    def init_jax_distributed_group(self, coordinator_address, world_size):
        print(
            f"initialize worker with rank: {self.rank}, "
            f"world_size: {world_size}, "
            f"coordinator_address: {coordinator_address}"
        )
        jax.distributed.initialize(
            coordinator_address=coordinator_address,
            num_processes=world_size,
            process_id=self.rank,
        )
        return True

    def get_coordinator_address(self) -> str:
        assert self.rank == 0, "Should only use rank 0 to get address and port"
        address, port = get_address_and_port()
        return f"{address}:{port}"

    def get_device_count(self):
        return f"Worker[{self.rank}]: {jax.device_count()}"

    def get_local_device_count(self):
        return f"Worker[{self.rank}]: {jax.local_device_count()}"

    def get_devices(self):
        return f"Worker[{self.rank}]: {str(jax.devices())}"


    def init_stable_diffusion(self, fx_path):
        # inference with jax
        dtype = jnp.bfloat16
        clip_model, clip_params = FlaxCLIPTextModel.from_pretrained(
            "openai/clip-vit-large-patch14", _do_init=False, dtype=dtype
        )
        unet, unet_params = UNet2D.from_pretrained(f"{fx_path}/unet", _do_init=False, dtype=dtype)
        vae, vae_params = AutoencoderKL.from_pretrained(f"{fx_path}/vae", _do_init=False, dtype=dtype)

        # config = CLIPConfig.from_pretrained("openai/clip-vit-large-patch14")
        self.tokenizer = CLIPTokenizer.from_pretrained("openai/clip-vit-large-patch14")
        scheduler = PNDMScheduler()

        # create inference state and replicate it across all GPU devices
        inference_state = InferenceState(
            text_encoder_params=clip_params,
            unet_params=unet_params,
            vae_params=vae_params
        )
        self.inference_state = replicate(inference_state)

        # create pipeline
        self.pipe = StableDiffusionPipeline(
            text_encoder=clip_model, tokenizer=self.tokenizer,
            unet=unet, scheduler=scheduler, vae=vae
        )
        return True
    
    def shard_input_tokens(self, text_prompt_list: List[str]):
        """Should only be done by the coordinator.

        Given all text prompts, tokenize and padding each prompt, then shard the ids 
        across number of hosts available in jax cluster. 
        """
        num_global_host_count = jax.host_count()

        input_ids = self.tokenizer(
            text_prompt_list, padding="max_length",
            truncation=True, max_length=40, return_tensors="jax"
        ).input_ids
        uncond_input_ids = self.tokenizer(
            text_prompt_list, padding="max_length",
            truncation=True, max_length=40, return_tensors="jax"
        ).input_ids

        host_sharded_input_ids_list = jax.tree_map(lambda x: x.reshape((num_global_host_count, -1) + x.shape[1:]), input_ids)
        host_sharded_uncond_input_ids_list = jax.tree_map(lambda x: x.reshape((num_global_host_count, -1) + x.shape[1:]), uncond_input_ids)

        print(f">>>> host_sharded_input_ids_list: {host_sharded_input_ids_list.shape}")
        print(f">>>> host_sharded_uncond_input_ids_list: {host_sharded_uncond_input_ids_list.shape}")

        return host_sharded_input_ids_list, host_sharded_uncond_input_ids_list


    def run_stable_diffusion(self, host_sharded_input_ids, host_sharded_uncond_input_ids, num_inference_steps = 100, guidance_scale = 1.0):
        """Given host level tokenized ids, shard across all devices on host and apply pmap inference.
        """
        num_local_devices = jax.local_device_count()
        num_samples = len(host_sharded_input_ids)
        print(f">>>> num_samples: {num_samples}")
    
        prng_seed = jax.random.PRNGKey(42)
        # shard inputs and rng
        input_ids = shard(host_sharded_input_ids)
        uncond_input_ids = shard(host_sharded_uncond_input_ids)
        # https://github.com/google/jax/issues/10864 Assigning a random key on JAX costs 3900 MB on GPU
        # prng_seed = jax.random.split(prng_seed, num_local_devices)
        prng_seed = jax.random.split(prng_seed, num_local_devices)

        # pmap the sample function
        sample = jax.pmap(self.pipe.sample, static_broadcasted_argnums=(4, 5))

        print(f">>> input_ids: {input_ids.shape}")
        print(f">>> uncond_input_ids: {uncond_input_ids.shape}")
        print(f">>> prng_seed: {prng_seed.shape}")
        # sample images
        images = sample(
            input_ids,
            uncond_input_ids,
            prng_seed,
            self.inference_state,
            num_inference_steps,
            guidance_scale,
        )

        # convert images to PIL images
        images = images / 2 + 0.5
        images = jnp.clip(images, 0, 1)
        images = (images * 255).round().astype("uint8")
        images = np.asarray(images).reshape((num_samples, 512, 512, 3))

        pil_images = [Image.fromarray(image) for image in images]
        return pil_images

workers = [
    Worker.options(
        # scheduling_strategy=NodeAffinitySchedulingStrategy(
        #     node_id=ray.get_runtime_context().node_id,
        #     soft=False,
        # )
    ).remote(rank)
    for rank in range(NUM_WORKERS)
]
coordinator = workers[0]
coordinator_address = ray.get(coordinator.get_coordinator_address.remote())
ray.get(
    [
        w.init_jax_distributed_group.remote(coordinator_address, NUM_WORKERS)
        for w in workers
    ]
)

print(
    f"Device count: {ray.get([w.get_device_count.remote() for w in workers])} \n"
)
print(
    f"Local device count: {ray.get([w.get_local_device_count.remote() for w in workers])} \n"
)
print(f"Get devices: {ray.get(coordinator.get_devices.remote())} \n")

text_prompts = ["skull made of bubbles in a glass tank lit by the sun detailed aquamarine cerulean indigo teal coral reef bubbles skull water vibrant minimalist"] * 4

print(f" >>> Stable diffusion init -- {ray.get([w.init_stable_diffusion.remote(CHECKPOINT_PATH) for w in workers])}")

host_sharded_input_ids_list, host_sharded_uncond_input_ids_list = ray.get(coordinator.shard_input_tokens.remote(text_prompts))

rst = ray.get([
    worker.run_stable_diffusion.remote(host_sharded_input_ids_list[idx], host_sharded_uncond_input_ids_list[idx])
    for idx, worker in enumerate(workers)
])

# images = ray.get(coordinator.run_stable_diffusion.remote(["hi"]))

import ipdb
ipdb.set_trace()

# for idx, image in enumerate(images):
#     image.save(f"/mnt/shared_storage/avnish_cade_jiao_hackathon/multi_host_pmap_{idx}.jpg")


