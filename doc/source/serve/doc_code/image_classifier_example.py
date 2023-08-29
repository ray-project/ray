# __serve_example_begin__
import requests
import starlette

from transformers import pipeline
from io import BytesIO
from PIL import Image

from ray import serve


@serve.deployment
def downloader(image_url: str):
    image_bytes = requests.get(image_url).content
    image = Image.open(BytesIO(image_bytes)).convert("RGB")
    return image


@serve.deployment
class ImageClassifier:
    def __init__(self, downloader):
        self.downloader = downloader.options(use_new_handle_api=True)
        self.model = pipeline(
            "image-classification", model="google/vit-base-patch16-224"
        )

    async def classify(self, image_url: str) -> str:
        image = await self.downloader.remote(image_url)
        results = self.model(image)
        return results[0]["label"]

    # async def __call__(self, req: starlette.requests.Request):
    #     req = await req.json()
    #     return await self.classify(req["image_url"])
    async def __call__(self, req: starlette.requests.Request):
        req = await req.json()
        result = await self.classify(req["image_url"])

        if req.get("should_translate") is True:
            handle = serve.get_app_handle("translator")
            return handle.translate.remote(result)

        return result


app = ImageClassifier.options(route_prefix="/classify").bind(downloader.bind())
# __serve_example_end__
