from typing import Dict

import numpy as np

import torch
from torchvision import transforms
from torchvision.models import resnet18

import ray
from ray.train.torch import TorchCheckpoint, TorchPredictor
from ray.train.batch_predictor import BatchPredictor
from ray.data.preprocessors import BatchMapper


def preprocess(image_batch: Dict[str, np.ndarray]) -> np.ndarray:
    """
    User PyTorch code to transform user image with outer dimension of batch size.
    """
    preprocess = transforms.Compose(
        [
            # Torchvision's ToTensor does not accept outer batch dimension
            transforms.CenterCrop(224),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    # Outer dimension is batch size such as (10, 256, 256, 3) -> (10, 3, 256, 256)
    transposed_torch_tensor = torch.Tensor(image_batch["image"].transpose(0, 3, 1, 2))
    return preprocess(transposed_torch_tensor).numpy()


data_url = "s3://anonymous@air-example-data-2/1G-image-data-synthetic-raw"
print(f"Running GPU batch prediction with 1GB data from {data_url}")
dataset = ray.data.read_images(data_url, size=(256, 256)).limit(10)

model = resnet18(pretrained=True)

preprocessor = BatchMapper(preprocess, batch_format="numpy")
ckpt = TorchCheckpoint.from_model(model=model, preprocessor=preprocessor)

predictor = BatchPredictor.from_checkpoint(ckpt, TorchPredictor)
predictor.predict(dataset, batch_size=80)
