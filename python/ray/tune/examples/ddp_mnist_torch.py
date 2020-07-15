# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
import os
import numpy as np
import argparse
from filelock import FileLock
import logging
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms
from torch.nn.parallel import DistributedDataParallel
import torch.distributed as dist
from ray.util.sgd.torch.constants import NCCL_TIMEOUT_S

import ray
from ray import tune
from ray.tune.function_runner import wrap_function
from ray.tune.resources import Resources
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.examples.mnist_pytorch import train, test, get_data_loaders
from ray.util.sgd.utils import find_free_port
from datetime import timedelta

# Change these values if you want the training to run quicker or slower.
EPOCH_SIZE = 512
TEST_SIZE = 256

logger = logging.getLogger(__name__)

def setup_address():
    ip = ray.services.get_node_ip_address()
    port = find_free_port()
    return "tcp://{ip}:{port}".format(ip=ip, port=port)


def setup_pg(url, world_rank, world_size, timeout, backend="gloo"):
    """Connects the distributed PyTorch backend.

    Args:
        url (str): the URL used to connect to distributed PyTorch.
        world_rank (int): the index of the runner.
        world_size (int): the total number of runners.
    """
    logger.debug("Connecting to {} world_rank: {} world_size: {}".format(
        url, world_rank, world_size))
    logger.debug("using {}".format(backend))
    os.environ["NCCL_BLOCKING_WAIT"] = "1"
    dist.init_process_group(
        backend=backend,
        init_method=url,
        rank=world_rank,
        world_size=world_size,
        timeout=timeout)


class ConvNet(nn.Module):
    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)


class _TorchTrainable(tune.Trainable):
    _func = None
    _num_workers = None

    __slots__ = ["workers"]

    @classmethod
    def default_process_group_parameters(self):
        return dict(timeout=timedelta(NCCL_TIMEOUT_S), backend="gloo")

    def setup(self, config):
        assert self._func

        def logger_creator(log_config):
            os.makedirs(self.logdir, exist_ok=True)
            # Set the working dir in the remote process, for user file writes
            if not ray.worker._mode() == ray.worker.LOCAL_MODE:
                os.chdir(self.logdir)
            return NoopLogger(log_config, self.logdir)

        func_trainable = wrap_function(self.__class__._func)
        remote_trainable = ray.remote(func_trainable)
        address = setup_address()
        self.workers = [remote_trainable.remote(config, logger_creator) for i in range(self._num_workers)]
        ### Requires tune.function_runner.FunctionRunner to be extended with this
        ray.get([w.execute.remote(
            lambda s: setup_pg(address, i, self._num_workers, **self.default_process_group_parameters()))
            for i, w in enumerate(self.workers)]
        )
        # print("called execute?")

    def step(self):
        result = ray.get([w.step.remote() for w in self.workers])
        return result[0]

    def save_checkpoint(self):
        if self.workers[0].is_colocated(self):
            return ray.get(self.workers[0].save.remote())
        result = ray.get(self.workers[0].save_to_object.remote())
        tmpdir = tempfile.mkdtemp("checkpoint", dir=self.logdir)
        checkpoint_path = TrainableUtil.create_from_pickle(result, tmpdir)
        return checkpoint_path



def train_mnist(config, checkpoint=False):
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    train_loader, test_loader = get_data_loaders()
    model = ConvNet().to(device)
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

def to_distributed(func,
            use_gpu=False,
            num_workers=1,
            num_cpus_per_worker=1,
            backend="auto",
            timeout_s=NCCL_TIMEOUT_S):

    class WrappedTorchTrainable(_TorchTrainable):
        _func = func
        _num_workers = num_workers

        @classmethod
        def default_process_group_parameters(self):
            return dict(timeout=timedelta(timeout_s), backend=backend)

        @classmethod
        def default_resource_request(cls, config):
            num_workers_ = int(config.get("num_workers", num_workers))
            num_cpus = int(config.get("num_cpus_per_worker", num_cpus_per_worker))
            use_gpu_ = config.get("use_gpu", use_gpu)

            return Resources(
                cpu=0,
                gpu=0,
                extra_cpu=num_cpus * num_workers_,
                extra_gpu=num_workers_ if use_gpu_ else 0)

    return WrappedTorchTrainable

trainable = to_distributed(train_mnist, num_workers=4)
analysis = tune.run(trainable, fail_fast=True)