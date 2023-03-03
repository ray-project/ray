import ray
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchCheckpoint, TorchPredictor
from torchvision import models
from torch import nn

# checkpoint = result.checkpoint
checkpoint = TorchCheckpoint.from_directory(
    "/tmp/pytorch-image.checkpoint"
)

# Read some
inference_data = ray.data.read_images(
    f"s3://anonymous@air-example-data/food-101-tiny/valid", size=(256, 256), mode="RGB"
)


# We have to specify our model definition again
def initialize_model():
    # Load pretrained model params
    model = models.efficientnet_b0(pretrained=True)

    # Replace the original classifier with a new Linear layer
    num_features = model.classifier[1].in_features
    model.classifier[1] = nn.Linear(num_features, 10)

    return model


# Create a batch predictor from the checkpointed model state dict,
# providing the model class as an input.
predictor = BatchPredictor.from_checkpoint(
    checkpoint, TorchPredictor, model=initialize_model()
)

# Actually run predictions
predictions = predictor.predict(inference_data)

# We can now process the results, e.g. inspect the dataframe
df = predictions.to_pandas()
print(df)
