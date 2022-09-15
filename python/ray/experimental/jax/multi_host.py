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

ray.init()

NUM_WORKERS = 1
NUM_GPU_PER_WORKER = 4
CHECKPOINT_PATH = "/mnt/cluster_storage"


@ray.remote(num_gpus=NUM_GPU_PER_WORKER)
class Worker:
    def __init__(self, rank):
        # Note: No jax calls should be made here before initializing jax.distributed
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

    def init_data(self):
        n_devices = jax.local_device_count()
        self.w = jnp.array([2.0, 3.0, 4.0])
        self.xs = jnp.arange(5 * n_devices).reshape(-1, 5)
        self.ws = jnp.stack([self.w] * n_devices)

    def convolution(self, x, w):
        output = []
        for i in range(1, len(x) - 1):
            output.append(jnp.dot(x[i - 1 : i + 2], w))
        return jnp.array(output)

    def convolution_vmap(self):
        return jax.vmap(self.convolution)(self.xs, self.ws)

    def convolution_pmap(self):
        # Replicated self.w via broadcasting
        return jax.pmap(self.convolution, in_axes=(0, None))(self.xs, self.w)

    def normalized_convolution(self, x, w):
        output = []
        for i in range(1, len(x)-1):
            output.append(jnp.dot(x[i-1:i+2], w))
        output = jnp.array(output)
        return output / jax.lax.psum(output, axis_name="p")

    def pmap_with_psum(self):
        return jax.pmap(lambda x: jax.lax.psum(x, "i"), axis_name="i")(self.xs)

    def pmap_normalized_convolution_with_psum(self):
        return jax.pmap(self.normalized_convolution, in_axes=(0, None), axis_name="p")(self.xs, self.w)

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

        config = CLIPConfig.from_pretrained("openai/clip-vit-large-patch14")
        self.tokenizer = CLIPTokenizer.from_pretrained("openai/clip-vit-large-patch14")
        scheduler = PNDMScheduler()

        # create inference state and replicate it across all TPU devices
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

    def run_stable_diffusion(self, text_prompt: str, num_inference_steps = 50, guidance_scale = 1.0):
        # TODO: Change this number to match local visible devices
        # prepare inputs
        num_samples = 4
        # p = "a photo of an astronaut riding a horse on mars"
        p = text_prompt

        input_ids = self.tokenizer(
            [p] * num_samples, padding="max_length", 
            truncation=True, max_length=77, return_tensors="jax"
        ).input_ids
        uncond_input_ids = self.tokenizer(
            [""] * num_samples, padding="max_length", 
            truncation=True, max_length=77, return_tensors="jax"
        ).input_ids
        prng_seed = jax.random.PRNGKey(42)

        # shard inputs and rng
        input_ids = shard(input_ids)
        uncond_input_ids = shard(uncond_input_ids)
        # TODO: Change this number to match local visible devices
        prng_seed = jax.random.split(prng_seed, 4)

        # pmap the sample function
        sample = jax.pmap(self.pipe.sample, static_broadcasted_argnums=(4, 5))

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

# ray.get([w.init_data.remote() for w in workers])
# # With psum, all GPU devices on host are visible to the worker actor
# print(ray.get(workers[0].pmap_with_psum.remote()))
# print(ray.get(workers[0].pmap_normalized_convolution_with_psum.remote()))

print(f" >>> Stable diffusion init -- {ray.get(coordinator.init_stable_diffusion.remote(CHECKPOINT_PATH))}")
images = ray.get(coordinator.run_stable_diffusion.remote("Michael Jordan playing soccer with Leo Messi"))
print(images)

