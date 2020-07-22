from unittest.mock import patch
import numpy as np
import os
import pytest
import torch
import torch.distributed as dist
import torch.optim as optim
from torch.nn.parallel import DistributedDataParallel

import ray
from ray import tune
from ray.util.sgd.torch.torch_trainable import DistributedTrainableCreator
from ray.tune.examples.mnist_pytorch import (train, test, get_data_loaders,
                                             model_creator)


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Ensure that tests don't ALL fail
    if dist.is_initialized():
        dist.destroy_process_group()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Ensure that tests don't ALL fail
    if dist.is_initialized():
        dist.destroy_process_group()


def train_mnist(config, checkpoint=False):
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    train_loader, test_loader = get_data_loaders()
    model = model_creator(config).to(device)
    optimizer = optim.SGD(model.parameters(), lr=0.1)

    if checkpoint:
        with open(checkpoint) as f:
            model_state, optimizer_state = torch.load(f)

        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    model = DistributedDataParallel(model)

    for epoch in range(10):
        train(model, optimizer, train_loader, device)
        acc = test(model, test_loader, device)
        with ray.util.sgd.torch.create_checkpoint(step=epoch) as f:
            torch.save((model.state_dict(), optimizer.state_dict()), f)
        tune.report(mean_accuracy=acc)


def test_single_step(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(train_mnist, num_workers=2)
    trainer = trainable_cls()
    result = trainer.train()
    print(result)
    trainer.stop()


# def test_resize(ray_start_2_cpus):  # noqa: F811
#     trainer = TorchTrainer(
#         model_creator=model_creator,
#         data_creator=data_creator,
#         optimizer_creator=optimizer_creator,
#         loss_creator=lambda config: nn.MSELoss(),
#         num_workers=1)
#     trainer.train(num_steps=1)
#     trainer.max_replicas = 2
#     results = trainer.train(num_steps=1, reduce_results=False)
#     assert len(results) == 2

if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
