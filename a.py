import logging
import time
from io import BytesIO
from typing import Dict

import torch
from diffusers import DiffusionPipeline
from PIL import Image

import ray
from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray import serve

# Number of inference steps (more steps means higher quality but longer runtime).
NUM_INFERENCE_STEPS = 40
# The split of steps to run on the base vs. refiner models.
DENOISING_SPLIT = 0.8

logger = logging.getLogger("ray.serve")

def _load_diffusion_pipeline(model_id: str) -> DiffusionPipeline:
    return DiffusionPipeline.from_pretrained(
        model_id,
        torch_dtype=torch.float16, variant="fp16", use_safetensors=True,
    )

def _convert_image_to_bytes(img: Image) -> bytes:
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

@ray.remote(num_gpus=1)
class BaseModelActor:
    def __init__(self):
        self._model = _load_diffusion_pipeline(
            "stabilityai/stable-diffusion-xl-base-1.0"
        ).to("cuda")

    def predict(self, prompt: str) -> torch.Tensor:
        return self._model(
            prompt=prompt,
            num_inference_steps=NUM_INFERENCE_STEPS,
            denoising_end=DENOISING_SPLIT,
            output_type="latent",
        ).images.to("cpu")


@ray.remote(num_gpus=1)
class RefinerModelActor:
    def __init__(self):
        self._model = _load_diffusion_pipeline(
            "stabilityai/stable-diffusion-xl-refiner-1.0"
        ).to("cuda")

    def predict(self, prompt: str, image: torch.Tensor) -> Image:
        return self._model(
            prompt=prompt,
            image=image.to("cuda"),
            num_inference_steps=NUM_INFERENCE_STEPS,
            denoising_start=DENOISING_SPLIT,
        ).images[0]

@serve.deployment
class SDXLModelParallel:
    def __init__(self):
        self._base_actor = BaseModelActor.remote()
        self._refiner_actor = RefinerModelActor.remote()

        with ray.dag.InputNode() as prompt:
            dag = self._refiner_actor.predict.bind(
                prompt,
                self._base_actor.predict.bind(
                    prompt,
                ).with_type_hint(TorchTensorType()),
            )

        self._adag = dag.experimental_compile(
            enable_asyncio=True,
            _execution_timeout=120,
        )

    async def __call__(self, request) -> bytes:
        prompt = (await request.body()).decode("utf-8")
        logger.info("Got prompt: %s", prompt)

        ref = await self._adag.execute_async(prompt)
        output_bytes = _convert_image_to_bytes(await ref)
        logger.info("Output image size: %s bytes", len(output_bytes))
        return output_bytes

def app(args: Dict) -> serve.Application:
    # See ray.util.accelerators for available accelerator types.
    base_model_resources = {"CPU": 1, "GPU": 1}
    base_model_accelerator_type = args.get("base_model_accelerator_type", None)
    if base_model_accelerator_type is not None:
        logger.info("Using accelerator_type='%s' for base model", base_model_accelerator_type)
        base_model_resources["accelerator_type"] = base_model_accelerator_type
    else:
        logger.info("No accelerator_type specified for base model")

    refiner_model_resources = {"CPU": 1, "GPU": 1}
    refiner_model_accelerator_type = args.get("refiner_model_accelerator_type", None)
    if refiner_model_accelerator_type is not None:
        logger.info("Using accelerator_type='%s' for refiner model", refiner_model_accelerator_type)
        refiner_model_resources["accelerator_type"] = refiner_model_accelerator_type
    else:
        logger.info("No accelerator_type specified for refiner model")

    return SDXLModelParallel.options(
        # Define resource requirements for the replica actor.
        ray_actor_options={"num_cpus": 1},
        # Define resource requirements for actors managed by the replica actor.
        placement_group_bundles=[
            {"CPU": 1}, # For the replica actor, must match resources above.
            base_model_resources,
            refiner_model_resources,
        ],
    ).bind()
