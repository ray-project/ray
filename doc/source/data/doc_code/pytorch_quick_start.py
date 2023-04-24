# flake8: noqa
# isort: skip_file

# __pt_quickstart_load_start__
import ray
import numpy as np


dataset = ray.data.from_numpy(np.ones((1, 100)))
# __pt_quickstart_load_end__


# __pt_quickstart_model_start__
class TorchPredictor:
    import torch
    import torch.nn as nn

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
# __pt_quickstart_model_end__


# __pt_quickstart_prediction_start__
scale = ray.data.ActorPoolStrategy(2)
predictions = dataset.map_batches(TorchPredictor, compute=scale)
predictions.show(limit=1)
# [0.45092654]
# __pt_quickstart_prediction_end__
