from ray.train.torch import TorchCheckpoint, TorchPredictor
from torchvision import models
from torch import nn

from ray import serve
from ray.serve import PredictorDeployment
from ray.serve.http_adapters import pandas_read_json


checkpoint = TorchCheckpoint.from_directory("/tmp/pytorch-text.checkpoint")


# We have to specify our model definition again
def initialize_model():
    # Load pretrained model params
    model = models.efficientnet_b0(pretrained=True)

    # Replace the original classifier with a new Linear layer
    num_features = model.classifier[1].in_features
    model.classifier[1] = nn.Linear(num_features, 10)

    return model


serve.run(
    PredictorDeployment.options(name="TorchTextService").bind(
        TorchPredictor, checkpoint, http_adapter=pandas_read_json, model=initialize_model()
    )
)
