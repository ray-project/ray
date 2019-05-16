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
    """Manages a distributed PyTorch model replica.

    Args:
        model_creator (dict -> torch.nn.Module): creates the model using the
            config.
        data_creator (dict -> Dataset, Dataset): creates the training and
            validation data sets using the config.
        optimizer_creator (model, dict -> loss, optimizer): creates the loss
            and optimizer using the config.
        config (dict): configuration passed to 'model_creator', 'data_creator',
            and 'optimizer_creator'.
        batch_size (int): batch size used for SGD.
        backend (string): backend used for distributed SGD. "gloo" or "nccl".
    """

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 config=None,
                 batch_size=16,
                 backend="gloo"):

        self.model_creator = model_creator
        self.data_creator = data_creator
        self.optimizer_creator = optimizer_creator
        self.config = {} if config is None else config
        self.batch_size = batch_size
        self.backend = backend
        self.verbose = True

        self.local_rank = None
        if os.environ.get("CUDA_VISIBLE_DEVICES", None):
            # TODO: might break if multiple GPUs requested
            self.local_rank = int(os.environ["CUDA_VISIBLE_DEVICES"])
        self.epoch = 0
        self._timers = {
            "setup_proc": utils.TimerStat(window_size=1),
            "setup_model": utils.TimerStat(window_size=1),
            "get_state": utils.TimerStat(window_size=1),
            "set_state": utils.TimerStat(window_size=1),
            "validation": utils.TimerStat(window_size=1),
            "training": utils.TimerStat(window_size=1)
        }

    def setup(self, url, world_rank, world_size):
        self.setup_distributed_pytorch(url, world_rank, world_size)
        self.setup_training()

    def setup_distributed_pytorch(self, url, world_rank, world_size):
        os.environ["CUDA_LAUNCH_BLOCKING"] = "1"
        with self._timers["setup_proc"]:
            self.world_rank = world_rank
            logger.debug(
                "Connecting to {} world_rank: {} world_size: {}".format(
                    url, world_rank, world_size))
            logger.debug("using {}".format(self.backend))
            dist.init_process_group(backend=self.backend,
                                    init_method=url,
                                    rank=world_rank,
                                    world_size=world_size)

            # This is a hack because set_devices fails otherwise
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
                [str(i) for i in range(ray.services._autodetect_num_gpus())])

            if torch.cuda.is_available():
                torch.cuda.set_device(self.local_rank)
            if self.verbose:
                logger.info("Device Set.")

    def setup_training(self):
        logger.debug("Creating model")
        self.model = self.model_creator(self.config)
        if self.local_rank is None:
            self.model = torch.nn.parallel.DistributedDataParallelCPU(
                self.model)
        else:
            self.model = torch.nn.parallel.DistributedDataParallel(
                self.model,
                device_ids=[self.local_rank],
                output_device=self.local_rank)

        logger.debug("Creating optimizer")
        self.criterion, self.optimizer = self.optimizer_creator(
            self.model, self.config)

        logger.debug("Creating dataset")
        self.training_set, self.validation_set = self.data_creator(self.config)

        self.train_sampler = torch.utils.data.distributed.DistributedSampler(
            self.training_set)
        self.train_loader = torch.utils.data.DataLoader(
            self.training_set,
            batch_size=self.batch_size,
            shuffle=(self.train_sampler is None),
            num_workers=2,
            pin_memory=False,
            sampler=self.train_sampler)
        self._train_iterator = iter(self.train_loader)

        # TODO: set up validation dataset

    def get_node_ip(self):
        return ray.services.get_node_ip_address()

    def step(self):
        logger.debug("Starting step")
        self.train_sampler.set_epoch(self.epoch)

        logger.debug("Begin Training Epoch {}".format(self.epoch + 1))
        with self._timers["training"]:
            train_stats = utils.train(self._train_iterator, self.model,
                                      self.criterion, self.optimizer)
            train_stats["epoch"] = self.epoch

        # TODO: validation?

        self.epoch += 1
        self._train_iterator = iter(self.train_loader)

        # train_stats.update(val_stats)
        train_stats.update(self.stats())
        return train_stats

    def stats(self):
        stats = {}
        for k, t in self._timers.items():
            stats[k + "_time_mean"] = t.mean
            stats[k + "_time_total"] = t.sum
            t.reset()
        return stats

    def get_state(self):
        return self.model.state_dict()

    def set_state(self, state_dict):
        self.model.load_state_dict(state_dict)

    def shutdown(self):
        logger.debug("Stopping worker.")
        try:
            dist.destroy_process_group()
        except Exception:
            logger.exception("Stop failed.")
