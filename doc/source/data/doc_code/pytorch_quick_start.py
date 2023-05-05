# flake8: noqa
# isort: skip_file
# fmt: off

# __pt_super_quick_start__
import ray
import numpy as np
from typing import Dict
import torch
import torch.nn as nn

ds = ray.data.from_numpy(np.ones((1, 100)))

class TorchPredictor:

    def __init__(self):
        self.model = nn.Sequential(
            nn.Linear(in_features=100, out_features=1),
            nn.Sigmoid(),
        )
        self.model.eval()

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict:
        tensor = torch.as_tensor(batch["data"], dtype=torch.float32)
        with torch.inference_mode():
            return {"output": self.model(tensor).detach().numpy()}

scale = ray.data.ActorPoolStrategy(size=2)
predictions = ds.map_batches(TorchPredictor, compute=scale)
predictions.show(limit=1)
# {'output': array([0.45092654])}
# __pt_super_quick_end__


# __pt_no_ray_start__
import torch
import torch.nn as nn
import numpy as np
from typing import Dict

batches = {"data": np.ones((1, 100))}

model = nn.Sequential(
    nn.Linear(in_features=100, out_features=1),
    nn.Sigmoid(),
)
model.eval()

def transform(batch: Dict[str, np.ndarray]):
    tensor = torch.as_tensor(batch["data"], dtype=torch.float32)
    with torch.inference_mode():
        return {"output": model(tensor).detach().numpy()}

results = transform(batches)
# __pt_no_ray_end__


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


# __pt_quickstart_prediction_test_start__
tp = TorchPredictor()
batch = ds.take_batch(10)
test = tp(batch)
# __pt_quickstart_prediction_test_end__


# __pt_quickstart_prediction_start__
scale = ray.data.ActorPoolStrategy(size=2)
predictions = ds.map_batches(TorchPredictor, compute=scale)
predictions.show(limit=1)
# {'output': array([0.45092654])}
# __pt_quickstart_prediction_end__
# fmt: on
