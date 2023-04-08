from typing import Dict, List
import numpy as np

import torch
from torchvision import transforms
from torchvision.models import resnet18

import ray
from ray.train.torch import TorchCheckpoint, TorchPredictor
from ray.train.batch_predictor import BatchPredictor
from ray.data.preprocessors import TorchVisionPreprocessor


data_url = "s3://anonymous@air-example-data-2/1G-image-data-synthetic-raw"
print(f"Running GPU batch prediction with 1GB data from {data_url}")
dataset = ray.data.read_images(data_url, size=(256, 256)).limit(10)

transform = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.CenterCrop(224),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ]
)

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
    num_gpus=1 # Specify 1 GPU per worker.
)

# Call show on the output probabilities to trigger execution
predictions.show(limit=1)
# {'class': 258}
