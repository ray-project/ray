# flake8: noqa
# isort: skip_file

# __pt_load_start__
import ray
from torchvision.models import ResNet18_Weights
from torchvision import transforms


# 1G of ImageNet images.
data_url = "s3://anonymous@air-example-data-2/1G-image-data-synthetic-raw"
dataset = ray.data.read_images(data_url).limit(100)
# __pt_load_start__

# __pt_preprocess_start__
from typing import Dict
import numpy as np


# Preprocess the images using the saved transforms.
resnet_transforms = ResNet18_Weights.DEFAULT.transforms

transform = transforms.Compose([transforms.ToTensor(), resnet_transforms()])


def preprocess_images(batch: Dict[str, np.ndarray]):
    transformed_images = [transform(image) for image in batch["image"]]
    return transformed_images


dataset = dataset.map_batches(preprocess_images, batch_format="numpy")
# __pt_preprocess_end__


# __pt_model_start__
from typing import List
import torch
from torchvision.models import resnet18


class TorchPredictor:
    def __init__(self):
        self.model = resnet18(pretrained=True).cuda()
        self.model.eval()

    def __call__(self, batch: List[torch.Tensor]):
        torch_batch = torch.stack(batch).cuda()
        with torch.inference_mode():
            prediction = self.model(torch_batch)
            return {"class": prediction.argmax(dim=1).detach().cpu().numpy()}
# __pt_model_end__


# __pt_prediction_start__
predictions = dataset.map_batches(
    TorchPredictor,
    compute=ray.data.ActorPoolStrategy(size=2),
    num_gpus=1,  # Specify 1 GPU per worker.
)

# Call show on the output probabilities to trigger execution
predictions.show(limit=1)
# {'class': 258}
# __pt_prediction_end__
