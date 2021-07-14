from os import name
import ray
from ray import serve

from io import BytesIO
from PIL import Image
import requests

import torch
from torchvision import transforms
from torchvision.models import resnet50


class Preprocessor:
    def __init__(self):
        self.torch_transform = transforms.Compose(
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

    def __call__(self, img_bytes):
        try:
            img = Image.open(BytesIO(img_bytes)).convert("RGB")
            tensor = self.torch_transform(img)
            return tensor
        except Exception as e:
            # PIL.UnidentifiedImageError: cannot identify image file <_io.BytesIO object at 0x7fb24feaa830>
            print(img_bytes[:100])
            raise e


class ImageModel:
    def __init__(self):
        self.model = resnet50(pretrained=True).eval().half().cuda()

    def __call__(self, request):
        input_tensor = torch.from_numpy(request.data).half().cuda()
        with torch.no_grad():
            output_tensor = self.model(input_tensor)
            result = torch.argmax(output_tensor, dim=1).cpu()
        return result.numpy()


if __name__ == "__main__":
    # Quick benchmark
    import numpy as np
    import time
    from tqdm import tqdm
    from collections import namedtuple

    # Bench just the model
    model = ImageModel()
    Request = namedtuple("Request", "data")
    arr = np.zeros((128, 3, 224, 224), dtype=np.float16)
    r = Request(data=arr)
    start = time.perf_counter()
    for _ in tqdm(range(100)):
        model(r)
    end = time.perf_counter()
    print("Throughput (img/s)", 100 * 128 / (end - start))

    # Quick testing with serve
    # client = serve.start()
    # client.create_backend(
    #     "resnet18:v0",
    #     ImageModel,
    #     config={"max_concurrent_queries": 16, "num_replicas": 5},
    #     ray_actor_options={"num_gpus": 0.2},
    # )
    # client.create_endpoint("predictor", backend="resnet18:v0")
    # handle = client.get_handle("predictor")

    # arr = np.zeros((128, 3, 224, 224), dtype=np.float16)
    # start = time.perf_counter()
    # refs = [handle.remote(arr) for _ in range(100)]
    # for r in tqdm(refs):
    #     ray.get(r)
    # end = time.perf_counter()
    # print("Throughput (img/s)", 100 * 128 / (end - start))
