from contextlib import contextmanager

# __example_code_start__

from io import BytesIO
from fastapi import FastAPI
from fastapi.responses import Response
import torch

from ray import serve


app = FastAPI()


@serve.deployment(num_replicas=1, route_prefix="/")
@serve.ingress(app)
class APIIngress:
    def __init__(self, diffusion_model_handle) -> None:
        self.handle = diffusion_model_handle

    @app.get(
        "/imagine",
        responses={200: {"content": {"image/png": {}}}},
        response_class=Response,
    )
    async def generate(self, prompt: str, img_size: int = 512):
        assert len(prompt), "prompt parameter cannot be empty"

        image_ref = await self.handle.generate.remote(prompt, img_size=img_size)
        image = await image_ref
        file_stream = BytesIO()
        image.save(file_stream, "PNG")
        return Response(content=file_stream.getvalue(), media_type="image/png")


@serve.deployment(
    ray_actor_options={"num_gpus": 1},
    autoscaling_config={"min_replicas": 0, "max_replicas": 2},
)
class StableDiffusionV2:
    def __init__(self):
        from diffusers import EulerDiscreteScheduler, StableDiffusionPipeline

        model_id = "stabilityai/stable-diffusion-2"

        scheduler = EulerDiscreteScheduler.from_pretrained(
            model_id, subfolder="scheduler"
        )
        self.pipe = StableDiffusionPipeline.from_pretrained(
            model_id, scheduler=scheduler, revision="fp16", torch_dtype=torch.float16
        )
        self.pipe = self.pipe.to("cuda")

    def generate(self, prompt: str, img_size: int = 512):
        assert len(prompt), "prompt parameter cannot be empty"

        with torch.autocast("cuda"):
            image = self.pipe(prompt, height=img_size, width=img_size).images[0]
            return image


entrypoint = APIIngress.bind(StableDiffusionV2.bind())

# __example_code_end__


@contextmanager
def serve_session(deployment):
    handle = serve.run(deployment)
    try:
        yield handle
    finally:
        serve.shutdown()


if __name__ == "__main__":
    import ray
    import os

    ray.init(
        runtime_env={
            "pip": [
                "diffusers==0.14.0",
                "transformers==4.25.1",
                "accelerate==0.17.1",
            ]
        }
    )

    with serve_session(entrypoint) as handle:
        ray.get(handle.generate.remote("hi"))
        # __example_client_start__
        import requests

        prompt = "a cute cat is dancing on the grass."
        prompt_query = "%20".join(prompt.split(" "))
        resp = requests.get(f"http://127.0.0.1:8000/imagine?prompt={prompt_query}")

        with open("output.png", "wb") as f:
            f.write(resp.content)
        # __example_client_end__

        assert os.path.exists("output.png")
        os.remove("output.png")
