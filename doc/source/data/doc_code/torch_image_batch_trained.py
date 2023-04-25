# flake8: noqa
# isort: skip_file
# fmt: off

# __pt_load_start__
import ray

data_url = "s3://anonymous@air-example-data-2/1G-image-data-synthetic-raw"  # <1>
dataset = ray.data.read_images(data_url).limit(1000)  # <2>
# __pt_load_end__

# __pt_preprocess_start__
from typing import Dict
import numpy as np
from torchvision import transforms
from torchvision.models import ResNet18_Weights

resnet_transforms = ResNet18_Weights.DEFAULT.transforms
transform = transforms.Compose([transforms.ToTensor(), resnet_transforms()])  # <1>

def preprocess_images(batch: Dict[str, np.ndarray]):  # <2>
    transformed_images = [transform(image) for image in batch["image"]]
    return transformed_images

dataset = dataset.map_batches(preprocess_images)  # <3>
# __pt_preprocess_end__


# __pt_model_start__
from typing import List
import torch
from torchvision.models import resnet18


class TorchPredictor:
    def __init__(self):  # <1>
        self.model = resnet18(pretrained=True).cuda()
        self.model.eval()

    def __call__(self, batch: List[torch.Tensor]):  # <2>
        torch_batch = torch.stack(batch).cuda()  # <3>
        with torch.inference_mode():
            prediction = self.model(torch_batch)
            return {"class": prediction.argmax(dim=1).detach().cpu().numpy()}  # <4>
# __pt_model_end__


# __pt_prediction_start__
predictions = dataset.map_batches(
    TorchPredictor,
    compute=ray.data.ActorPoolStrategy(4),  # <1>
    num_gpus=1,  # <2>
)

predictions.show(limit=1)
# {'class': 258}
# __pt_prediction_end__
# fmt: on
