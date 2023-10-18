import os
import torch
import torch.distributed as dist
import torch.nn as nn
from torch.nn.parallel import DistributedDataParallel as DDP
import torch.optim as optim
import torch_xla.core.xla_model as xm
import torch_xla.distributed.xla_multiprocessing as xmp
import torch_xla.distributed.xla_backend
import ray.train

class Model(nn.Module):
    def __init__(self):
        super(Model, self).__init__()
        self.net1 = nn.Linear(10, 10)
        self.relu = nn.ReLU()
        self.net2 = nn.Linear(10, 5)

    def forward(self, x):
        return self.net2(self.relu(self.net1(x)))

def train_func():
    device = xm.xla_device()
    rank = xm.get_ordinal()

    # Create the model and move to device
    model = Model().to(device)
    ddp_model = DDP(model, gradient_as_bucket_view=True)

    loss_fn = nn.MSELoss()
    optimizer = optim.SGD(ddp_model.parameters(), lr=0.001)
    for step in range(5):
        optimizer.zero_grad()
        outputs = ddp_model(torch.randn(20, 10).to(device))
        labels = torch.randn(20, 5).to(device)
        loss = loss_fn(outputs, labels)
        loss.backward()
        optimizer.step()
        xm.mark_step()
        if rank == 0:
            print(f"Loss after step {step}: {loss.cpu()}")

from ray.train.torch.xla import TorchXLAConfig
from ray.train.torch import TorchTrainer

trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    torch_config=TorchXLAConfig(),
    scaling_config=ray.train.ScalingConfig(num_workers=2, resources_per_worker={"neuron_cores": 1}),
)
result = trainer.fit()
print(result)
