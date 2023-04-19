import numpy as np
import pandas as pd
import torch
import torch.nn as nn

import ray

dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
dataset = dataset.drop_columns("target")

# All columns are features.
num_features = len(dataset.schema().names)


# Concatenate the features to a single Numpy array.
def concatenate(batch: pd.DataFrame):
    concatenated_features = batch.to_numpy()
    return concatenated_features


# Specify "pandas" batch format.
dataset = dataset.map_batches(concatenate, batch_format="pandas")


# Define the model class for prediction.
# Use a simple 3 layer feed-forward neural network.
class TorchModel:
    def __init__(self):
        self.model = nn.Sequential(
            nn.Linear(in_features=num_features, out_features=16),
            nn.ReLU(),
            nn.Linear(16, 16),
            nn.ReLU(),
            nn.Linear(16, 1),
            nn.Sigmoid(),
        )
        self.model.eval()

    def __call__(self, batch: np.ndarray):
        tensor = torch.as_tensor(batch, dtype=torch.float32)
        with torch.inference_mode():
            return self.model(tensor).detach().numpy()


# Predict on the features using 2 inference workers.
predicted_probabilities = dataset.map_batches(
    TorchModel,
    # Increase `size` to scale out to more workers.
    compute=ray.data.ActorPoolStrategy(size=2),
    batch_format="numpy",
)

# Call show on the output probabilities to trigger execution.
predicted_probabilities.show(limit=4)
# [0.9909512]
# [0.97295094]
# [0.932291]
# [0.9823262]