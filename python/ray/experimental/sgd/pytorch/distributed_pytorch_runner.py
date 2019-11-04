from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import torch.distributed as dist
import torch.utils.data

from ray.experimental.sgd.pytorch.pytorch_runner import PyTorchRunner

logger = logging.getLogger(__name__)


class DistributedPyTorchRunner(PyTorchRunner):
    """Manages a distributed PyTorch model replica."""

    def __init__(self, *args, backend="gloo", **kwargs):
        """Initializes the runner.

        Args:
            args: Arguments for the PyTorchRunner.
            kwargs: Keyword arguments for the PyTorchRunner.
            backend (string): backend used by distributed PyTorch.
        """
        super(DistributedPyTorchRunner, self).__init__(*args, **kwargs)
        self.backend = backend

    def setup(self, url, world_rank, world_size):
        """Connects to the distributed PyTorch backend and initializes the model.

        Args:
            url (str): the URL used to connect to distributed PyTorch.
            world_rank (int): the index of the runner.
            world_size (int): the total number of runners.
        """
        self._setup_distributed_pytorch(url, world_rank, world_size)
        self._setup_training()

    def _setup_distributed_pytorch(self, url, world_rank, world_size):
        with self._timers["setup_proc"]:
            self.world_rank = world_rank
            logger.debug(
                "Connecting to {} world_rank: {} world_size: {}".format(
                    url, world_rank, world_size))
            logger.debug("using {}".format(self.backend))
            dist.init_process_group(
                backend=self.backend,
                init_method=url,
                rank=world_rank,
                world_size=world_size)

    def _setup_training(self):
        logger.debug("Creating model")
        self.model = self.model_creator(self.config)
        if torch.cuda.is_available():
            self.model = self.model.cuda()
        self.model = torch.nn.parallel.DistributedDataParallel(self.model)

        logger.debug("Creating optimizer.")
        self.optimizer = self.optimizer_creator(self.model, self.config)
        self.criterion = self.loss_creator(self.config)
        if torch.cuda.is_available():
            self.criterion = self.criterion.cuda()

        logger.debug("Creating dataset")
        self.train_loader, self.validation_loader = self.data_creator(
            self.batch_size, self.config)

    def step(self):
        """Runs a training epoch and updates the model parameters.

        Automatically sets epoch of sampler if possible.
        """
        logger.debug("Starting step")
        if hasattr(self.train_loader.sampler, "set_epoch"):
            self.train_loader.sampler.set_epoch(self.epoch)
        return super(DistributedPyTorchRunner, self).step()

    def get_state(self):
        """Returns the state of the runner."""
        return {
            "epoch": self.epoch,
            "model": self.model.module.cpu().state_dict(),
            "optimizer": self.optimizer.state_dict(),
            "stats": self.stats()
        }

    def set_state(self, state):
        """Sets the state of the model."""
        # TODO: restore timer stats
        self.model.module.load_state_dict(state["model"])
        self.optimizer.load_state_dict(state["optimizer"])
        self.epoch = state["stats"]["epoch"]

    def shutdown(self):
        """Attempts to shut down the worker."""
        super(DistributedPyTorchRunner, self).shutdown()
        dist.destroy_process_group()
