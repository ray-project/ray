from concurrent.futures import ThreadPoolExecutor, TimeoutError
from io import BytesIO
import PIL
from PIL import Image
import requests
import starlette.requests
import torch
import torchvision.models as models
from torchvision.models import ResNet50_Weights
from torchvision import transforms

from ray import serve


@serve.deployment
class Model:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.resnet50 = (
            models.resnet50(weights=ResNet50_Weights.DEFAULT).eval().to(self.device)
        )
        self.preprocess = transforms.Compose(
            [
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )
        with open("imagenet_classes.txt", "r") as f:
            self.categories = [s.strip() for s in f.readlines()]

        self.model_thread_pool = ThreadPoolExecutor(max_workers=5)

    async def __call__(self, request: starlette.requests.Request) -> str:
        uri = (await request.json())["uri"]

        try:
            image_bytes = requests.get(uri, timeout=5).content
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.Timeout,
        ):
            return

        try:
            image = Image.open(BytesIO(image_bytes)).convert("RGB")
        except PIL.UnidentifiedImageError:
            return

        images = [image]  # Batch size is 1

        def run_model():
            input_tensor = torch.cat(
                [self.preprocess(img).unsqueeze(0) for img in images]
            ).to(self.device)
            with torch.no_grad():
                output = self.resnet50(input_tensor)
                sm_output = torch.nn.functional.softmax(output[0], dim=0)
            return torch.argmax(sm_output)

        try:
            future = self.model_thread_pool.submit(run_model)
            ind = future.result(timeout=5)
            return self.categories[ind]
        except TimeoutError:
            return


app = Model.bind()
