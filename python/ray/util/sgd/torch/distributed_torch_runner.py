from datetime import timedelta
import logging
import io
import os

import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler
from ray.util.sgd.torch.constants import NCCL_TIMEOUT_S

import ray
from ray.util.sgd.torch.torch_runner import TorchRunner

logger = logging.getLogger(__name__)


class DistributedTorchRunner(TorchRunner):
    """Manages a distributed PyTorch model replica.


    Args:
        args: Arguments for TorchRunner.
        backend (str): Backend used by distributed PyTorch.
        add_dist_sampler (bool): Whether to automatically add a
            DistributedSampler to all created dataloaders.
        wrap_ddp (bool): Whether to automatically wrap DistributedDataParallel
            over each model. If False, you are expected to call it yourself.
        kwargs: Keyword arguments for TorchRunner.

    """

    def __init__(self,
                 *args,
                 backend="gloo",
                 add_dist_sampler=True,
                 wrap_ddp=False,
                 **kwargs):
        super(DistributedTorchRunner, self).__init__(*args, **kwargs)
        if backend not in ("gloo", "nccl"):
            raise ValueError("Backend must be one of 'gloo' or 'nccl'.")
        self.backend = backend
        self.wrap_ddp = wrap_ddp
        self.add_dist_sampler = add_dist_sampler
        self.world_rank = None

    def setup_process_group(self, url, world_rank, world_size):
        """Connects the distributed PyTorch backend.

        Args:
            url (str): the URL used to connect to distributed PyTorch.
            world_rank (int): the index of the runner.
            world_size (int): the total number of runners.
        """
        self.world_rank = world_rank
        logger.debug("Connecting to {} world_rank: {} world_size: {}".format(
            url, world_rank, world_size))
        logger.debug("using {}".format(self.backend))
        if self.backend == "nccl" and "NCCL_BLOCKING_WAIT" not in os.environ:
            logger.debug(
                "Setting NCCL_BLOCKING_WAIT for detecting node failure. "
                "To override this behavior, you can set NCCL_BLOCKING_WAIT=0.")
            os.environ["NCCL_BLOCKING_WAIT"] = "1"

        timeout = timedelta(seconds=NCCL_TIMEOUT_S)
        dist.init_process_group(
            backend=self.backend,
            init_method=url,
            rank=world_rank,
            world_size=world_size,
            timeout=timeout)

    def setup_ddp_and_operator(self):
        """Runs distributed coordination components.

        This helps avoid timeouts due to creator functions (perhaps
        downloading data or models).
        """
        device_ids = None
        if self.use_gpu and torch.cuda.is_available():
            device_ids = self.get_device_ids()

        # Wrap dataloaders
        self._wrap_dataloaders()

        training_models = self.models
        if self.wrap_ddp:
            # This needs to happen after apex
            training_models = [
                DistributedDataParallel(model, device_ids=device_ids)
                for model in self.models
            ]
        self.training_operator = self.training_operator_cls(
            self.config,
            models=training_models,
            optimizers=self.optimizers,
            criterion=self.criterion,
            train_loader=self.train_loader,
            validation_loader=self.validation_loader,
            world_rank=self.world_rank,
            schedulers=self.schedulers,
            device_ids=device_ids,
            use_gpu=self.use_gpu,
            use_fp16=self.use_fp16,
            use_tqdm=self.use_tqdm)

    def get_device_ids(self):
        """Needed for SyncBatchNorm, which needs 1 GPU per process."""
        return [0]

    def load_state_stream(self, byte_obj):
        """Loads a bytes object the training state dict.

        This is needed because we don't want to deserialize the tensor
        onto the same device (which is from the driver process). We want to
        map it onto the actor's specific device.

        From: github.com/pytorch/pytorch/issues/10622#issuecomment-474733769
        """
        _buffer = io.BytesIO(byte_obj)
        to_gpu = self.use_gpu and torch.cuda.is_available()
        state_dict = torch.load(
            _buffer,
            map_location=("cpu" if not to_gpu else
                          lambda storage, loc: storage.cuda()))
        return self.load_state_dict(state_dict)

    def _wrap_dataloaders(self):
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

    def shutdown(self):
        """Attempts to shut down the worker."""
        # However, it seems to be harmless to remove permanently
        # since the processes are shutdown anyways. This comment can be
        # removed in a future release if it is still not documented
        # the stable Pytorch docs.
        dist.destroy_process_group()
        super(DistributedTorchRunner, self).shutdown()


def _init_cuda_context():
    # Force cuda initialization
    # Inspired by https://github.com/pytorch/pytorch/blob/
    # f050b16dd95b2bcce9853882fd3fb07a6fd80378/torch/testing/
    # _internal/common_cuda.py
    torch.cuda.is_available()


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
        self.local_device = "0"
        global _dummy_actor
        if not self.is_actor():
            if _dummy_actor is None:
                _dummy_actor = ray.remote(
                    num_cpus=num_cpus,
                    num_gpus=num_gpus,
                    resources={"node:" + ip: 0.1})(_DummyActor).remote()

            self.local_device = ray.get(_dummy_actor.cuda_devices.remote())

            # This is a pretty annoying workaround. To enable SyncBatchNorm,
            # we need to signify that we are using only 1 CUDA device (via
            # the DDP constructor).
            # However, on the local worker, we have to set the
            # CUDA_VISIBLE_DEVICES at runtime rather at process start.

            # You can only call setdevice(int > 0) after you've interacted with
            # torch.cuda. But you can't guarantee that you _haven't_ interacted
            # with it (user can do arbitrary things), so we force an
            # interaction.
            _init_cuda_context()
            os.environ["CUDA_VISIBLE_DEVICES"] = self.local_device

            if self.local_device:
                try:
                    torch.cuda.set_device(int(self.local_device))
                except RuntimeError:
                    logger.error("This happens if cuda is not initialized.")
                    raise

        super(LocalDistributedRunner, self).__init__(*args, **kwargs)

    def get_device_ids(self):
        return [int(self.local_device)]

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
