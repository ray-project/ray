from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import torch
import torch.distributed as dist
import torch.utils.data

import ray
from ray.experimental.sgd.pytorch import utils

logger = logging.getLogger(__name__)


class PyTorchRunner(object):
    """Manages a distributed PyTorch model replica"""

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 config=None,
                 batch_size=16,
                 backend="gloo"):
        """Initializes the runner.

        Args:
            model_creator (dict -> torch.nn.Module): creates the model using
                the config.
            data_creator (dict -> Dataset, Dataset): creates the training and
                validation data sets using the config.
            optimizer_creator (torch.nn.Module, dict -> loss, optimizer):
                creates the loss and optimizer using the model and the config.
            config (dict): configuration passed to 'model_creator',
                'data_creator', and 'optimizer_creator'.
            batch_size (int): batch size used in an update.
            backend (string): backend used by distributed PyTorch.
        """

        self.model_creator = model_creator
        self.data_creator = data_creator
        self.optimizer_creator = optimizer_creator
        self.config = {} if config is None else config
        self.batch_size = batch_size
        self.backend = backend
        self.verbose = True

        self.epoch = 0
        self._timers = {
            k: utils.TimerStat(window_size=1)
            for k in [
                "setup_proc", "setup_model", "get_state", "set_state",
                "validation", "training"
            ]
        }

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
        os.environ["CUDA_LAUNCH_BLOCKING"] = "1"
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

    def get_node_ip(self):
        """Returns the IP address of the current node"""
        return ray.services.get_node_ip_address()

    def step(self):
        """Runs a training epoch and updates the model parameters"""
        logger.debug("Starting step")
        self.train_sampler.set_epoch(self.epoch)

        logger.debug("Begin Training Epoch {}".format(self.epoch + 1))
        with self._timers["training"]:
            train_stats = utils.train(self.train_loader, self.model,
                                      self.criterion, self.optimizer)
            train_stats["epoch"] = self.epoch

        self.epoch += 1

        train_stats.update(self.stats())
        return train_stats

    def validate(self):
        """Evaluates the model on the validation data set"""
        with self._timers["validation"]:
            validation_stats = utils.validate(self.validation_loader,
                                              self.model, self.criterion)

        validation_stats.update(self.stats())
        return validation_stats

    def stats(self):
        """Returns a dictionary of statistics collected"""
        stats = {"epoch": self.epoch}
        for k, t in self._timers.items():
            stats[k + "_time_mean"] = t.mean
            stats[k + "_time_total"] = t.sum
            t.reset()
        return stats

    def get_state(self):
        """Returns the state of the runner"""
        return {
            "epoch": self.epoch,
            "model": self.model.state_dict(),
            "optimizer": self.optimizer.state_dict(),
            "stats": self.stats()
        }

    def set_state(self, state):
        """Sets the state of the model"""
        # TODO: restore timer stats
        self.model.load_state_dict(state["model"])
        self.optimizer.load_state_dict(state["optimizer"])
        self.epoch = state["stats"]["epoch"]

    def shutdown(self):
        """Attempts to shut down the worker"""
        dist.destroy_process_group()
