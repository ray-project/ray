from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import torch.distributed as dist
import torch.utils.data

from ray.experimental.sgd.pytorch.pytorch_runner import TensorFlow2Runner

logger = logging.getLogger(__name__)


class DistributedTensorFlow2Runner(TensorFlow2Runner):
    """Manages a distributed TensorFlow2 model replica."""

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 config=None,
                 batch_size=16):
        """Initializes the runner.

        Args:
            model_creator (dict -> torch.nn.Module): see pytorch_trainer.py.
            data_creator (dict -> Dataset, Dataset):  see pytorch_trainer.py.
            optimizer_creator (torch.nn.Module, dict -> loss, optimizer):
                see pytorch_trainer.py.
            config (dict):  see pytorch_trainer.py.
            batch_size (int): batch size used by one replica for an update.
            backend (string):  see pytorch_trainer.py.
        """

        super(DistributedTensorFlow2Runner, self).__init__(
            model_creator, data_creator, optimizer_creator, config, batch_size)
        self.backend = backend

    def setup(self, url, world_rank, world_size):
        """Connects to the distributed TensorFlow2 backend and initializes the model.

        Args:
            url (str): the URL used to connect to distributed TensorFlow2.
            world_rank (int): the index of the runner.
            world_size (int): the total number of runners.
        """
        self._setup_distributed_pytorch(url, world_rank, world_size)
        self._setup_training()

    def _setup_distributed_pytorch(self, url, world_rank, world_size):
        os.environ["CUDA_LAUNCH_BLOCKING"] = "1"
        os.environ["TF_CONFIG"] = json.dumps()


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
            self.model = torch.nn.parallel.DistributedDataParallel(
                self.model.cuda())
        else:
            self.model = torch.nn.parallel.DistributedDataParallelCPU(
                self.model)

        logger.debug("Creating optimizer")
        self.criterion, self.optimizer = self.optimizer_creator(
            self.model, self.config)
        if torch.cuda.is_available():
            self.criterion = self.criterion.cuda()

        logger.debug("Creating dataset")
        self.training_set, self.validation_set = self.data_creator(self.config)

        # TODO: make num_workers configurable
        self.train_sampler = torch.utils.data.distributed.DistributedSampler(
            self.training_set)
        self.train_loader = torch.utils.data.DataLoader(
            self.training_set,
            batch_size=self.batch_size,
            shuffle=(self.train_sampler is None),
            num_workers=2,
            pin_memory=False,
            sampler=self.train_sampler)

        self.validation_sampler = (
            torch.utils.data.distributed.DistributedSampler(
                self.validation_set))
        self.validation_loader = torch.utils.data.DataLoader(
            self.validation_set,
            batch_size=self.batch_size,
            shuffle=(self.validation_sampler is None),
            num_workers=2,
            pin_memory=False,
            sampler=self.validation_sampler)

    def step(self):
        """Runs a training epoch and updates the model parameters."""
        logger.debug("Starting step")
        self.train_sampler.set_epoch(self.epoch)
        return super(DistributedTensorFlow2Runner, self).step()

    def get_state(self):
        """Returns the state of the runner."""
        return {
            "epoch": self.epoch,
            "model": self.model.module.state_dict(),
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
        super(DistributedTensorFlow2Runner, self).shutdown()
        dist.destroy_process_group()
