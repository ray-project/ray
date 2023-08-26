# fmt: off
# __doc_import_begin__
from ray import serve

from contextlib import contextmanager
from io import BytesIO
from PIL import Image
from starlette.requests import Request
from typing import Dict

import torch
from torchvision import transforms
from torchvision.models import resnet18
# __doc_import_end__
# fmt: on


# __doc_define_servable_begin__
@serve.deployment
class ImageModel:
    def __init__(self):
        self.model = resnet18(pretrained=True).eval()
        self.preprocessor = transforms.Compose(
            [
                transforms.Resize(224),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Lambda(lambda t: t[:3, ...]),  # remove alpha channel
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )

    async def __call__(self, starlette_request: Request) -> Dict:
        image_payload_bytes = await starlette_request.body()
        pil_image = Image.open(BytesIO(image_payload_bytes))
        print("[1/3] Parsed image data: {}".format(pil_image))

        pil_images = [pil_image]  # Our current batch size is one
        input_tensor = torch.cat(
            [self.preprocessor(i).unsqueeze(0) for i in pil_images]
        )
        print("[2/3] Images transformed, tensor shape {}".format(input_tensor.shape))

        with torch.no_grad():
            output_tensor = self.model(input_tensor)
        print("[3/3] Inference done!")
        return {"class_index": int(torch.argmax(output_tensor[0]))}
        # __doc_define_servable_end__


# __doc_deploy_begin__
image_model = ImageModel.bind()
# __doc_deploy_end__


@contextmanager
def serve_session(deployment):
    handle = serve.run(deployment)
    try:
        yield handle
    finally:
        serve.shutdown()


if __name__ == "__main__":
    import ray

    ray.init(
        runtime_env={
            "pip": [
                "torch==2.0.0",
                "torchvision==0.14.0",
            ]
        }
    )

    with serve_session(image_model):
        # __example_client_start__
        import requests

        ray_logo_bytes = requests.get(
            "https://raw.githubusercontent.com/ray-project/"
            "ray/master/doc/source/images/ray_header_logo.png"
        ).content

        resp = requests.post("http://localhost:8000/", data=ray_logo_bytes)
        print(resp.json())
        # __example_client_end__
