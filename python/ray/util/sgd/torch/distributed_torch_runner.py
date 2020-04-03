from datetime import timedelta
import collections
import logging
import os

import torch
import torch.nn as nn
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
from ray.util.sgd.torch.constants import NCCL_TIMEOUT_IN_SECONDS

import ray
from ray.util.sgd.torch.torch_runner import TorchRunner, _remind_gpu_usage

logger = logging.getLogger(__name__)


class DistributedTorchRunner(TorchRunner):
    """Manages a distributed PyTorch model replica.


    Args:
        args: Arguments for TorchRunner.
        backend (str): Backend used by distributed PyTorch.
        add_dist_sampler (bool): Whether to automatically add a
            DistributedSampler to all created dataloaders.
        kwargs: Keyword arguments for TorchRunner.

    """

    def __init__(self, *args, backend="gloo", add_dist_sampler=True, **kwargs):
        super(DistributedTorchRunner, self).__init__(*args, **kwargs)
        if backend not in ("gloo", "nccl"):
            raise ValueError("Backend must be one of 'gloo' or 'nccl'.")
        self.backend = backend
        self.add_dist_sampler = add_dist_sampler

    def setup(self, url, world_rank, world_size):
        """Connects to the distributed PyTorch backend and initializes the model.

        Args:
            url (str): the URL used to connect to distributed PyTorch.
            world_rank (int): the index of the runner.
            world_size (int): the total number of runners.
        """
        _remind_gpu_usage(self.use_gpu, is_chief=world_rank == 0)
        self._setup_distributed_pytorch(url, world_rank, world_size)
        self._setup_training()

    def _setup_distributed_pytorch(self, url, world_rank, world_size):
        self.world_rank = world_rank
        logger.debug("Connecting to {} world_rank: {} world_size: {}".format(
            url, world_rank, world_size))
        logger.debug("using {}".format(self.backend))

        if self.backend == "nccl" and "NCCL_BLOCKING_WAIT" not in os.environ:
            logger.debug(
                "Setting NCCL_BLOCKING_WAIT for detecting node failure. "
                "To override this behavior, you can set NCCL_BLOCKING_WAIT=0.")
            os.environ["NCCL_BLOCKING_WAIT"] = "1"

        timeout = timedelta(seconds=NCCL_TIMEOUT_IN_SECONDS)
        dist.init_process_group(
            backend=self.backend,
            init_method=url,
            rank=world_rank,
            world_size=world_size,
            timeout=timeout)

        self.device_ids = None

        if self.use_gpu and torch.cuda.is_available():
            # https://github.com/allenai/allennlp/issues/1090
            self._set_cuda_device_id()

    def _set_cuda_device_id(self):
        self.device_ids = [0]

    def _setup_training(self):
        logger.debug("Loading data.")
        self._initialize_dataloaders()
        logger.debug("Creating model")
        self.models = self.model_creator(self.config)
        if not isinstance(self.models, collections.Iterable):
            self.models = [self.models]
        assert all(isinstance(model, nn.Module) for model in self.models), (
            "All models must be PyTorch models: {}.".format(self.models))
        if self.use_gpu and torch.cuda.is_available():
            self.models = [model.cuda() for model in self.models]

        logger.debug("Creating optimizer.")
        self.optimizers = self.optimizer_creator(self.given_models,
                                                 self.config)
        if not isinstance(self.optimizers, collections.Iterable):
            self.optimizers = [self.optimizers]

        self._create_schedulers_if_available()
        self._try_setup_apex()

        # This needs to happen after apex
        self.models = [
            DistributedDataParallel(model, device_ids=self.device_ids)
            for model in self.models
        ]

        self._create_loss()

        self.training_operator = self.training_operator_cls(
            self.config,
            models=self.models,
            optimizers=self.optimizers,
            criterion=self.criterion,
            train_loader=self.train_loader,
            validation_loader=self.validation_loader,
            world_rank=self.world_rank,
            schedulers=self.schedulers,
            use_gpu=self.use_gpu,
            use_fp16=self.use_fp16,
            use_tqdm=self.use_tqdm)

    def _initialize_dataloaders(self):
        super(DistributedTorchRunner, self)._initialize_dataloaders()

        def with_sampler(loader):
            # Automatically set the DistributedSampler
            data_loader_args = {
                "dataset": loader.dataset,
                "batch_size": loader.batch_size,
                "shuffle": False,
                "num_workers": loader.num_workers,
                "collate_fn": loader.collate_fn,
                "pin_memory": loader.pin_memory,
                "drop_last": loader.drop_last,
                "timeout": loader.timeout,
                "worker_init_fn": loader.worker_init_fn,
                "sampler": DistributedSampler(loader.dataset)
            }
            return DataLoader(**data_loader_args)

        if isinstance(self.train_loader, DataLoader):
            if self.add_dist_sampler:
                self.train_loader = with_sampler(self.train_loader)

        if self.validation_loader and isinstance(self.validation_loader,
                                                 DataLoader):
            if self.add_dist_sampler:
                self.validation_loader = with_sampler(self.validation_loader)

    def train_epoch(self, **kwargs):
        """Runs a training epoch and updates the model parameters.

        Automatically sets epoch of sampler if possible.
        """
        if hasattr(self.train_loader, "sampler") and hasattr(
                self.train_loader.sampler, "set_epoch"):
            self.train_loader.sampler.set_epoch(self.epochs)
        return super(DistributedTorchRunner, self).train_epoch(**kwargs)

    def _get_model_state_dicts(self):
        """Fetch state from ``model.module`` instead of ``model``.

        This is needed for PyTorch DistributedDataParallel models.
        """
        return [model.module.state_dict() for model in self.models]

    def _set_model_state_dicts(self, model_state_dicts):
        for model, model_state_dict in zip(self.models, model_state_dicts):
            model.module.load_state_dict(model_state_dict)

    def shutdown(self):
        """Attempts to shut down the worker."""
        # However, it seems to be harmless to remove permanently
        # since the processes are shutdown anyways. This comment can be
        # removed in a future release if it is still not documented
        # the stable Pytorch docs.
        dist.destroy_process_group()
        super(DistributedTorchRunner, self).shutdown()


class _DummyActor:
    def cuda_devices(self):
        return os.environ["CUDA_VISIBLE_DEVICES"]


# This is a bit of a hack. It prevents the reassignment of CUDA_VISIBLE_DEVICES
# during a trainer resize. We won't need this if we don't shutdown
# all the actors.
_dummy_actor = None


class LocalDistributedRunner(DistributedTorchRunner):
    """A wrapper for running a distributed Runner on the driver.

    A dummy actor is used to reserve resources on the driver node,
    as specified by `num_cpus` and `num_gpus`. If the Trainer is already
    in an actor, we will ignore this resource request.
    """

    def __init__(self, *args, num_cpus=None, num_gpus=None, **kwargs):
        ip = ray.services.get_node_ip_address()

        # Reserve a local GPU or CPU for the local worker
        # TODO: we should make sure this NEVER dies.

        global _dummy_actor
        if not self.is_actor() and _dummy_actor is None:
            _dummy_actor = ray.remote(
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                resources={"node:" + ip: 0.1})(_DummyActor).remote()

            self.local_device = ray.get(_dummy_actor.cuda_devices.remote())

            # This is a pretty annoying workaround. To enable SyncBatchNorm,
            # we need to signify that we are using only 1 CUDA device (via
            # the DDP constructor). However, on the local worker,
            # we set the CUDA_VISIBLE_DEVICES at runtime rather at process
            # start. This means that we have to make sure that DDP knows which
            # specific device we are using.
            os.environ["CUDA_VISIBLE_DEVICES"] = self.local_device
            if self.local_device:
                torch.cuda.set_device(int(self.local_device))
        super(LocalDistributedRunner, self).__init__(*args, **kwargs)

    def _set_cuda_device_id(self):
        self.device_ids = [int(self.local_device)]

    def shutdown(self, cleanup=True):
        super(LocalDistributedRunner, self).shutdown()
        global _dummy_actor
        if cleanup and _dummy_actor:
            assert not self.is_actor(), "Actor shouldn't have a dummy actor."
            ray.kill(_dummy_actor)
            _dummy_actor = None

    def is_actor(self):
        actor_id = ray.worker.global_worker.actor_id
        return actor_id != actor_id.nil()


class DeactivatedRunner:
    def __getattr__(self, *args, **kwargs):
        raise RuntimeError(
            "This TorchTrainer is not active (it is likely shutdown already). "
            "Create a new TorchTrainer.")
