# __serve_example_begin__
import requests
from io import BytesIO

from PIL import Image
import starlette.requests
import torch
from torchvision import transforms
import torchvision.models as models
from torchvision.models import ResNet50_Weights

from ray import serve


@serve.deployment(
    ray_actor_options={"num_cpus": 1},
    max_concurrent_queries=5,
    autoscaling_config={
        "target_num_ongoing_requests_per_replica": 1,
        "min_replicas": 0,
        "initial_replicas": 0,
        "max_replicas": 200,
    },
)
class Model:
    def __init__(self):
        self.resnet50 = (
            models.resnet50(weights=ResNet50_Weights.DEFAULT).eval().to("cpu")
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
        resp = requests.get(
            "https://raw.githubusercontent.com/pytorch/hub/master/imagenet_classes.txt"
        )
        self.categories = resp.content.decode("utf-8").split("\n")

    async def __call__(self, request: starlette.requests.Request) -> str:
        uri = (await request.json())["uri"]
        image_bytes = requests.get(uri).content
        image = Image.open(BytesIO(image_bytes)).convert("RGB")

        # Batch size is 1
        input_tensor = torch.cat([self.preprocess(image).unsqueeze(0)]).to("cpu")
        with torch.no_grad():
            output = self.resnet50(input_tensor)
            sm_output = torch.nn.functional.softmax(output[0], dim=0)
        ind = torch.argmax(sm_output)
        return self.categories[ind]


app = Model.bind()
# __serve_example_end__

if __name__ == "__main__":
    import requests  # noqa

    serve.run(app)
    resp = requests.post(
        "http://localhost:8000/",
        json={
            "uri": "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000019.jpeg"  # noqa
        },
    )  # noqa
    assert resp.text == "ox"
