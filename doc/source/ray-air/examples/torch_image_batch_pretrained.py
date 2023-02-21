from torchvision import transforms
from torchvision.models import resnet18

import ray
from ray.train.torch import TorchCheckpoint, TorchPredictor
from ray.train.batch_predictor import BatchPredictor
from ray.data.preprocessors import TorchVisionPreprocessor


data_url = "s3://anonymous@air-example-data-2/1G-image-data-synthetic-raw"
print(f"Running GPU batch prediction with 1GB data from {data_url}")
dataset = ray.data.read_images(data_url, size=(256, 256)).limit(10)

model = resnet18(pretrained=True)

transform = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.CenterCrop(224),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ]
)
preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)

ckpt = TorchCheckpoint.from_model(model=model, preprocessor=preprocessor)

predictor = BatchPredictor.from_checkpoint(ckpt, TorchPredictor)
predictor.predict(dataset, batch_size=80)
