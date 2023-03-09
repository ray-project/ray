import torch
from PIL import Image
import numpy as np
from io import BytesIO
from fastapi.responses import Response
from ray import serve
from fastapi import FastAPI

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
class ObjectDection:
    def __init__(self):
        self.model = torch.hub.load("ultralytics/yolov5", "yolov5s")
        self.model.cuda()

    def detect(self, image_url: str):
        result_im = self.model(image_url)
        return Image.fromarray(result_im.render()[0].astype(np.uint8))


entrypoint = APIIngress.bind(ObjectDection.bind())
