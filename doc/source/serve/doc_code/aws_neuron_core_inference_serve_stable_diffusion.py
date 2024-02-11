# __neuron_serve_code_start__
from io import BytesIO
from fastapi import FastAPI
from fastapi.responses import Response
from ray import serve


app = FastAPI()

neuron_cores = 2

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
    async def generate(self, prompt: str):

        image_ref = await self.handle.generate.remote(prompt)
        image = image_ref
        file_stream = BytesIO()
        image.save(file_stream, "PNG")
        return Response(content=file_stream.getvalue(), media_type="image/png")


@serve.deployment(
    ray_actor_options={
        "resources": {"neuron_cores": neuron_cores}
    },
    autoscaling_config={"min_replicas": 1, "max_replicas": 2},
)
class StableDiffusionV2:
    def __init__(self):
        from optimum.neuron import NeuronStableDiffusionXLPipeline
        compiled_model_id = "aws-neuron/stable-diffusion-xl-base-1-0-1024x1024"
        self.pipe = NeuronStableDiffusionXLPipeline.from_pretrained(compiled_model_id, device_ids=[0, 1])

    async def generate(self, prompt: str):

        assert len(prompt), "prompt parameter cannot be empty"
        image = self.pipe(prompt).images[0]
        return image


entrypoint = APIIngress.bind(StableDiffusionV2.bind())

# __neuron_serve_code_end__