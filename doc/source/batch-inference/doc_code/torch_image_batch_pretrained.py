from typing import Dict, List
import numpy as np

import torch
from torchvision import transforms
from torchvision.models import resnet18, ResNet18_Weights

import ray

# 1G of ImageNet images.
data_url = "s3://anonymous@air-example-data-2/1G-image-data-synthetic-raw"

print(f"Running GPU batch prediction with 1GB data from {data_url}")

dataset = ray.data.read_images(data_url).limit(10)

# Preprocess the images using the saved transforms.
pretrained_weight = ResNet18_Weights.DEFAULT
imagenet_transforms = pretrained_weight.transforms

transform = transforms.Compose([transforms.ToTensor(), imagenet_transforms()])


def preprocess_images(batch: Dict[str, np.ndarray]):
    transformed_images = [transform(image) for image in batch["image"]]
    return transformed_images


dataset = dataset.map_batches(preprocess_images, batch_format="numpy")


class TorchModel:
    def __init__(self):
        self.model = resnet18(pretrained=True).cuda()
        self.model.eval()

    def __call__(self, batch: List[torch.Tensor]):
        torch_batch = torch.stack(batch).cuda()
        with torch.inference_mode():
            prediction = self.model(torch_batch)
            return {"class": prediction.argmax(dim=1).detach().cpu().numpy()}


predictions = dataset.map_batches(
    TorchModel,
    compute=ray.data.ActorPoolStrategy(size=2),
    num_gpus=1,  # Specify 1 GPU per worker.
)

# Call show on the output probabilities to trigger execution
predictions.show(limit=1)
# {'class': 258}
