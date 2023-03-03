import ray
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchCheckpoint, TorchPredictor
from torchvision import models
from torch import nn

# checkpoint = result.checkpoint
checkpoint = TorchCheckpoint.from_directory(
    "/Users/kai/ray_results/resnet-hackathon/TorchTrainer_28e60_00000_0_2023-03-03_11-18-21/checkpoint_000000"
)

inference_data = ray.data.read_images(
    f"s3://anonymous@air-example-data/food-101-tiny/valid", size=(256, 256), mode="RGB"
)


def initialize_model():
    # Load pretrained model params
    model = models.efficientnet_b0(pretrained=True)

    # Replace the original classifier with a new Linear layer
    num_features = model.classifier[1].in_features
    model.classifier[1] = nn.Linear(num_features, 10)

    for param in model.parameters():
        param.requires_grad = True
    return model


predictor = BatchPredictor.from_checkpoint(
    checkpoint, TorchPredictor, model=initialize_model()
)
predictions = predictor.predict(inference_data)
df = predictions.to_pandas()
print(df)
