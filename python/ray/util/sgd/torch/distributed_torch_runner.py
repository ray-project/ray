import collections
from filelock import FileLock
import logging
import os
import torch.nn as nn
import torch.distributed as dist
import torch.utils.data
from torch.nn.parallel import DistributedDataParallel

from ray.util.sgd.pytorch.pytorch_runner import TorchRunner

logger = logging.getLogger(__name__)


class DistributedTorchRunner(TorchRunner):
    """Manages a distributed PyTorch model replica.


    Args:
        args: Arguments for TorchRunner.
        backend (string): backend used by distributed PyTorch.
        kwargs: Keyword arguments for TorchRunner.

    """

    def __init__(self, *args, backend="gloo", **kwargs):
        super(DistributedTorchRunner, self).__init__(*args, **kwargs)
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
        self.models = self.model_creator(self.config)
        if not isinstance(self.models, collections.Iterable):
            self.models = [self.models]
        assert all(isinstance(model, nn.Module) for model in self.models), (
            "All models must be PyTorch models: {}.".format(self.models))
        if torch.cuda.is_available():
            self.models = [model.cuda() for model in self.models]

        logger.debug("Creating optimizer.")
        self.optimizers = self.optimizer_creator(self.given_models,
                                                 self.config)
        if not isinstance(self.optimizers, collections.Iterable):
            self.optimizers = [self.optimizers]

        self._create_schedulers_if_available()

        self._try_setup_apex()

        # This needs to happen after apex
        self.models = [DistributedDataParallel(model) for model in self.models]

        logger.debug("Creating loss.")
        self._create_loss()

        logger.debug("Creating dataset.")
        with FileLock(os.path.expanduser("~/.ray_data.lock")):
            datasets = self.data_creator(self.config)
            train_set, val_set = self._validate_datasets(datasets)

        train_loader_config = self.dataloader_config.copy()
        train_loader_config.update(
            sampler=torch.utils.data.distributed.DistributedSampler(train_set),
            shuffle=False)

        self.train_loader = torch.utils.data.DataLoader(
            train_set, batch_size=self.batch_size, **train_loader_config)

        self.validation_loader = None
        if val_set:
            self.validation_loader = torch.utils.data.DataLoader(
                val_set, batch_size=self.batch_size, **self.dataloader_config)

        self.training_operator = self.training_operator_cls(
            self.config,
            models=self.models,
            optimizers=self.optimizers,
            criterion=self.criterion,
            schedulers=self.schedulers,
            use_fp16=self.use_fp16)

    def train_epoch(self, **kwargs):
        """Runs a training epoch and updates the model parameters.

        Automatically sets epoch of sampler if possible.
        """
        if hasattr(self.train_loader.sampler, "set_epoch"):
            self.train_loader.sampler.set_epoch(self.epochs)
        return super(DistributedTorchRunner, self).train_epoch(**kwargs)

    def _get_model_state_dicts(self):
        """Fetch state from ``model.module`` instead of ``model``.

        This is needed for PyTorch DistributedDataParallel models.
        """
        cpu_state_dicts = []
        for model in self.models:
            state_dict = model.module.state_dict()
            # This is so that we create a duplicate of weights into CPU rather
            # than move the model weights out of the GPU so that we can
            # resume training while saving intermediate checkpoints.
            cpu_state_dicts += [{k: v.cpu() for k, v in state_dict.items()}]
        return cpu_state_dicts

    def _set_model_state_dicts(self, model_state_dicts):
        for model, model_state_dict in zip(self.models, model_state_dicts):
            model.module.load_state_dict(model_state_dict)

    # def shutdown(self):
        """Attempts to shut down the worker."""
        # super(DistributedTorchRunner, self).shutdown()
        # TODO: Temporarily removing since it causes hangs on MacOSX.
        # However, it seems to be harmless to remove permanently
        # since the processes are shutdown anyways. This comment can be
        # removed in a future release if it is still not documented
        # the stable Pytorch docs.
        # dist.destroy_process_group()
