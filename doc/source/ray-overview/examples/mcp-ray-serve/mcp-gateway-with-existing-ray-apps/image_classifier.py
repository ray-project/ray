import requests
import starlette
from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment(
    name="image_downloader",
    num_replicas=2,
    ray_actor_options={
        "num_cpus": 0.3,
        "runtime_env": {                 
            "pip": [
                "pillow"
                ]
            }
        },
)
def downloader(image_url: str):
    from io import BytesIO
    from PIL import Image
    image_bytes = requests.get(image_url).content
    image = Image.open(BytesIO(image_bytes)).convert("RGB")
    return image


@serve.deployment(
    name="image_classifier",
    num_replicas=2,
    ray_actor_options={
        "num_gpus": 0.5,
        "runtime_env": {                 
            "pip": [
                "transformers",
                "torch",
                "pillow",
                "hf_xet"
                ]
            }
        },
)
class ImageClassifier:
    def __init__(self, downloader: DeploymentHandle):
        from transformers import pipeline
        self.downloader = downloader
        self.model = pipeline(
            "image-classification", model="google/vit-base-patch16-224"
        )

    async def classify(self, image_url: str) -> str:
        image = await self.downloader.remote(image_url)
        results = self.model(image)
        return results[0]["label"]

    async def __call__(self, req: starlette.requests.Request):
        req = await req.json()
        return await self.classify(req["image_url"])


app = ImageClassifier.bind(downloader.bind())