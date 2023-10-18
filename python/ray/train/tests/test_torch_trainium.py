import pytest
import torch
import torch.nn as nn
import torch.optim as optim
import torch_xla.core.xla_model as xm
import torch_xla.distributed.xla_backend  # noqa: F401
from torch.nn.parallel import DistributedDataParallel as DDP

import ray
import ray.train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer
from ray.train.torch.xla import TorchXLAConfig


@pytest.fixture
def ray_start_2_neuron_cores():
    address_info = ray.init(resources={"neuron_cores": 2})
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_torch_e2e(ray_start_2_neuron_cores):
    class Model(nn.Module):
        def __init__(self):
            super(Model, self).__init__()
            self.net1 = nn.Linear(10, 10)
            self.relu = nn.ReLU()
            self.net2 = nn.Linear(10, 5)

        def forward(self, x):
            return self.net2(self.relu(self.net1(x)))

    def train_func():
        # Create the model and move to device
        device = xm.xla_device()
        model = Model().to(device)
        ddp_model = DDP(model, gradient_as_bucket_view=True)

        loss_fn = nn.MSELoss()
        optimizer = optim.SGD(ddp_model.parameters(), lr=0.001)
        for _ in range(5):
            optimizer.zero_grad()
            outputs = ddp_model(torch.randn(20, 10).to(device))
            labels = torch.randn(20, 5).to(device)
            loss = loss_fn(outputs, labels)
            loss.backward()
            optimizer.step()
            xm.mark_step()

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        torch_config=TorchXLAConfig(),
        scaling_config=ScalingConfig(
            num_workers=2, resources_per_worker={"neuron_cores": 1}
        ),
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
