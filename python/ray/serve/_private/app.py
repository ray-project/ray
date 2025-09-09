import gc
import io
import pickle

import numpy as np
from fastapi import FastAPI, Response, UploadFile
from PIL import Image

from ray import serve

app = FastAPI()


DEFAULT_CPU_DEPLOYMENT_OPTIONS = {
    "ray_actor_options": {"num_cpus": 1},
    "max_ongoing_requests": 4,
    "graceful_shutdown_timeout_s": 60,
    "max_replicas_per_node": 35,
    "autoscaling_config": {
        "initial_replicas": 1,
        "target_ongoing_requests": 2,
        "min_replicas": 1,
        "max_replicas": 16,
        "upscale_delay_s": 5,
        "downscale_delay_s": 30,
    },
}


def load_binary_data(binary_data, is_rgb=False):
    bio = io.BytesIO(binary_data)
    pil_im = Image.open(bio)
    if is_rgb:
        pil_im = pil_im.convert("RGB")
    return np.array(pil_im)


@serve.deployment(**DEFAULT_CPU_DEPLOYMENT_OPTIONS)
@serve.ingress(app)
class SimpleRepro:
    @app.post("/test")
    async def test_large_binary_io(
        self,
        image_data: UploadFile,
        depth_data: UploadFile,
    ) -> Response:
        try:
            image_binary_data = await image_data.read()
            depth_binary_data = await depth_data.read()
            image = load_binary_data(image_binary_data, True)
            depth = load_binary_data(depth_binary_data)[..., None]
            output = np.concatenate([image, depth], axis=-1)
            output = np.repeat(output, 10, axis=-1)
            output_binary = pickle.dumps(output)
        finally:
            await image_data.close()
            await depth_data.close()
            # Explicitly call garbage collection to free up memory
            # after closing UploadFile objects, in case they hold large buffers.
            gc.collect()

        return Response(output_binary)


deployment = SimpleRepro.bind()
