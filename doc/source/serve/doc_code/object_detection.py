from contextlib import contextmanager

# __example_code_start__
import torch
from PIL import Image
import numpy as np
from io import BytesIO
from fastapi.responses import Response
from fastapi import FastAPI

from ray import serve


app = FastAPI()


@serve.deployment(num_replicas=1, route_prefix="/")
@serve.ingress(app)
class APIIngress:
    def __init__(self, object_detection_handle) -> None:
        self.handle = object_detection_handle

    @app.get(
        "/detect",
        responses={200: {"content": {"image/jpeg": {}}}},
        response_class=Response,
    )
    async def detect(self, image_url: str):
        image_ref = await self.handle.detect.remote(image_url)
        image = await image_ref
        file_stream = BytesIO()
        image.save(file_stream, "jpeg")
        return Response(content=file_stream.getvalue(), media_type="image/jpeg")


@serve.deployment(
    ray_actor_options={"num_gpus": 1},
    autoscaling_config={"min_replicas": 1, "max_replicas": 2},
)
class ObjectDetection:
    def __init__(self):
        self.model = torch.hub.load("ultralytics/yolov5", "yolov5s")
        self.model.cuda()

    def detect(self, image_url: str):
        result_im = self.model(image_url)
        return Image.fromarray(result_im.render()[0].astype(np.uint8))


entrypoint = APIIngress.bind(ObjectDetection.bind())


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
    import requests
    import os

    ray.init(runtime_env={"pip": ["seaborn", "ultralytics"]})

    with serve_session(entrypoint):
        image_url = "https://ultralytics.com/images/zidane.jpg"
        resp = requests.get(f"http://127.0.0.1:8000/detect?image_url={image_url}")

        with open("output.jpeg", "wb") as f:
            f.write(resp.content)

        assert os.path.exists("output.jpeg")
