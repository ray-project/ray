# __example_code_start__

from io import BytesIO
from fastapi import FastAPI
from fastapi.responses import Response
import torch

from ray import serve
from ray.serve.handle import DeploymentHandle


app = FastAPI()


@serve.deployment(num_replicas=1)
@serve.ingress(app)
class APIIngress:
    def __init__(self, diffusion_model_handle: DeploymentHandle) -> None:
        self.handle = diffusion_model_handle

    @app.get(
        "/imagine",
        responses={200: {"content": {"image/png": {}}}},
        response_class=Response,
    )
    async def generate(self, prompt: str, img_size: int = 512):
        assert len(prompt), "prompt parameter cannot be empty"

        image = await self.handle.generate.remote(prompt, img_size=img_size)
        file_stream = BytesIO()
        image.save(file_stream, "PNG")
        return Response(content=file_stream.getvalue(), media_type="image/png")


@serve.deployment(
    ray_actor_options={"num_gpus": 1},
    autoscaling_config={"min_replicas": 0, "max_replicas": 2},
)
class StableDiffusionXL:
    def __init__(self):
        from diffusers import DiffusionPipeline

        model_id = "stabilityai/stable-diffusion-xl-base-1.0"

        self.pipe = DiffusionPipeline.from_pretrained(
            model_id, torch_dtype=torch.float16, variant="fp16", use_safetensors=True
        )
        self.pipe = self.pipe.to("cuda")

    def generate(self, prompt: str, img_size: int = 512):
        assert len(prompt), "prompt parameter cannot be empty"

        with torch.autocast("cuda"):
            image = self.pipe(prompt, height=img_size, width=img_size).images[0]
            return image


entrypoint = APIIngress.bind(StableDiffusionXL.bind())

# __example_code_end__


if __name__ == "__main__":
    import ray
    import os
    import requests

    ray.init(
        runtime_env={
            "pip": [
                "diffusers==0.33.1",
                "transformers==4.51.3",
            ]
        }
    )

    handle = serve.run(entrypoint)
    handle.generate.remote("hi").result()

    prompt = "a cute cat is dancing on the grass."
    prompt_query = "%20".join(prompt.split(" "))
    resp = requests.get(f"http://127.0.0.1:8000/imagine?prompt={prompt_query}")

    with open("output.png", "wb") as f:
        f.write(resp.content)

    assert os.path.exists("output.png")
    os.remove("output.png")
