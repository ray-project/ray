import ray
import numpy as np
import torch
import torch.nn as nn


class TorchPredictor:
    def __init__(self):
        self.model = nn.Sequential(
            nn.Linear(in_features=100, out_features=1),
            nn.Sigmoid(),
        )
        self.model.eval()

    def __call__(self, batch):
        tensor = torch.as_tensor(batch, dtype=torch.float32)
        with torch.inference_mode():
            return self.model(tensor).detach().numpy()


dataset = ray.data.from_numpy(np.ones((1, 100)))
scale = ray.data.ActorPoolStrategy(2)

predictions = dataset.map_batches(TorchPredictor, compute=scale)
predictions.show(limit=1)
# [0.45092654]
