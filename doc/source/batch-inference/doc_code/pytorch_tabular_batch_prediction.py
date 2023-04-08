from typing import Dict

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

dataset = dataset.map_batches(concatenate, batch_format="pandas")

# Define the model class for prediction.
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
        
    def __call__(self, batch: np.ndarray):
        tensor = torch.as_tensor(batch, dtype=torch.float32)
        return self.model(tensor).detach().numpy()
    
# Predict on the features.
predicted_probabilities = dataset.map_batches(
    TorchModel,
    compute=ray.data.ActorPoolStrategy(size=2),
    batch_format="numpy"
)

# Call show on the output probabilities to trigger execution.
predicted_probabilities.show(limit=4)
# [0.9909512]
# [0.97295094]
# [0.932291]
# [0.9823262]
