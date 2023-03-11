import argparse
from fastapi import FastAPI
from fastapi.responses import Response
from io import BytesIO
import time

from ray import serve

try:
    import torch
    from diffusers import EulerDiscreteScheduler, StableDiffusionPipeline
except ImportError as e:
    raise RuntimeError(
        "Did you install requirements with `pip install -r requirements.txt`?"
    ) from e


parser = argparse.ArgumentParser()
parser.add_argument("--num-replicas", type=int, default=1)
args = parser.parse_args()

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

        image = await (await self.handle.generate.remote(prompt, img_size=img_size))

        file_stream = BytesIO()
        image.save(file_stream, "PNG")
        return Response(content=file_stream.getvalue(), media_type="image/png")


@serve.deployment(
    ray_actor_options={"num_gpus": 1},
    autoscaling_config={
        "min_replicas": args.num_replicas,
        "max_replicas": args.num_replicas,
    },
)
class StableDiffusionV2:
    def __init__(self):
        # Delay imports, since packages will only be installed on the replicas

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

        image = self.pipe(prompt, height=img_size, width=img_size).images[0]

        return image


entrypoint = APIIngress.bind(StableDiffusionV2.bind())

# Run the script with `serve run app:entrypoint` to start the serve application
if __name__ == "__main__":
    serve.run(entrypoint, port=8000, name="serving_stable_diffusion_template")
    while True:
        time.sleep(0.5)
