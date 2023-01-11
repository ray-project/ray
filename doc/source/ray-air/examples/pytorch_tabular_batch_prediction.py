import numpy as np
import torch.nn as nn

import ray
from ray.data.preprocessors import Concatenator
from ray.train.torch import TorchCheckpoint, TorchPredictor
from ray.train.batch_predictor import BatchPredictor


def create_model(input_features: int):
    return nn.Sequential(
        nn.Linear(in_features=input_features, out_features=16),
        nn.ReLU(),
        nn.Linear(16, 16),
        nn.ReLU(),
        nn.Linear(16, 1),
        nn.Sigmoid(),
    )


dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

# All columns are features except the target column.
num_features = len(dataset.schema().names) - 1

# Specify a preprocessor to concatenate all feature columns.
prep = Concatenator(
    output_column_name="concat_features", exclude=["target"], dtype=np.float32
)

checkpoint = TorchCheckpoint.from_model(
    model=create_model(num_features), preprocessor=prep
)
# You can also fetch a checkpoint from a Trainer
# checkpoint = best_result.checkpoint

batch_predictor = BatchPredictor.from_checkpoint(checkpoint, TorchPredictor)

# Predict on the features.
predicted_probabilities = batch_predictor.predict(
    dataset, feature_columns=["concat_features"]
)
predicted_probabilities.show()
# {'predictions': array([1.], dtype=float32)}
# {'predictions': array([0.], dtype=float32)}
