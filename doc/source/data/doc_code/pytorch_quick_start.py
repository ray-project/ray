# flake8: noqa
# isort: skip_file
# fmt: off

# __pt_quickstart_load_start__
import ray
import numpy as np
from typing import Dict


ds = ray.data.from_numpy(np.ones((1, 100)))
# __pt_quickstart_load_end__


# __pt_quickstart_model_start__
import torch
import torch.nn as nn

class TorchPredictor:

    def __init__(self):  # <1>
        self.model = nn.Sequential(
            nn.Linear(in_features=100, out_features=1),
            nn.Sigmoid(),
        )
        self.model.eval()

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict:  # <2>
        tensor = torch.as_tensor(batch["data"], dtype=torch.float32)
        with torch.inference_mode():
            return {"output": self.model(tensor).detach().numpy()}
# __pt_quickstart_model_end__


# __pt_quickstart_prediction_start__
tp = TorchPredictor()
batch = ds.take_batch(10)
test = tp(batch)

scale = ray.data.ActorPoolStrategy(size=2)
predictions = ds.map_batches(TorchPredictor, compute=scale)
predictions.show(limit=1)
# {'output': array([0.45092654])}
# __pt_quickstart_prediction_end__
# fmt: on
