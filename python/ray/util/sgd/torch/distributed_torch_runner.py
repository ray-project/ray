import logging
import os

import torch
import torch.distributed as dist
from ray.util.sgd.torch.utils import setup_process_group

import ray
from ray.util.sgd.torch.torch_runner import TorchRunner

from ray.util.sgd.torch.utils import setup_address

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

    def __init__(
        self, *args, backend="gloo", add_dist_sampler=True, wrap_ddp=False, **kwargs
    ):
        super(DistributedTorchRunner, self).__init__(*args, **kwargs)
        if backend not in ("gloo", "nccl"):
            raise ValueError("Backend must be one of 'gloo' or 'nccl'.")
        self.backend = backend
        self.wrap_ddp = wrap_ddp
        self.add_dist_sampler = add_dist_sampler
        self.world_rank = None
        self.local_rank = None

    def setup_address(self):
        return setup_address()

    def setup_process_group(self, url, world_rank, world_size, timeout):
        """Connects the distributed PyTorch backend.

        Args:
            url (str): the URL used to connect to distributed PyTorch.
            world_rank (int): the index of the runner.
            world_size (int): the total number of runners.
            timeout (timedelta): Seconds for process group
                operations to timeout.
        """
        logger.info(f"Setting up process group for: {url} [rank={world_rank}]")
        self.world_rank = world_rank
        setup_process_group(url, world_rank, world_size, timeout, backend=self.backend)

    def set_local_rank(self, local_rank):
        """Sets the local rank of this runner.

        Args:
            local_rank (int): the index of the runner on its node.
        """
        self.local_rank = local_rank

    def setup_operator(self):
        """Runs distributed coordination components.

        This helps avoid timeouts due to creator functions (perhaps
        downloading data or models).
        """
        device = torch.device("cpu")
        if self.use_gpu and torch.cuda.is_available():
            device = self.get_device()

        self.training_operator = self.training_operator_cls(
            self.config,
            world_rank=self.world_rank,
            local_rank=self.local_rank,
            is_distributed=True,
            device=device,
            use_gpu=self.use_gpu,
            use_fp16=self.use_fp16,
            use_tqdm=self.use_tqdm,
            wrap_ddp=self.wrap_ddp,
            add_dist_sampler=self.add_dist_sampler,
            scheduler_step_freq=self.scheduler_step_freq,
        )

    def get_device(self):
        """Needed for SyncBatchNorm, which needs 1 GPU per process."""
        return torch.device("cuda:0")

    def train_epoch(self, num_steps=None, profile=False, info=None, iterator=None):
        """Runs a training epoch and updates the model parameters.

        Automatically sets epoch of sampler if possible.
        """
        if (
            iterator is None
            and hasattr(self.train_loader, "sampler")
            and hasattr(self.train_loader.sampler, "set_epoch")
        ):
            self.train_loader.sampler.set_epoch(self.epochs)
        return super(DistributedTorchRunner, self).train_epoch(
            num_steps=num_steps, profile=profile, info=info, iterator=iterator
        )

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

    def get(self):
        # in order to verify the actor has created
        return 1


# This is a bit of a hack. It prevents the reassignment of CUDA_VISIBLE_DEVICES
# during a trainer resize. We won't need this if we don't shutdown
# all the actors.
_dummy_cuda_actor = None
# used to reserve CPU resources
_dummy_cpu_actor = None


def clear_dummy_actor():
    global _dummy_cuda_actor
    if _dummy_cuda_actor:
        try:
            _dummy_cuda_actor.__ray_terminate__.remote()
        except Exception as exc:
            logger.info("Tried to clear dummy actor: %s", str(exc))

    _dummy_cuda_actor = None

    global _dummy_cpu_actor
    if _dummy_cpu_actor:
        try:
            _dummy_cpu_actor.__ray_terminate__.remote()
        except Exception as exc:
            logger.info("Tried to clear dummy actor: %s", str(exc))

    _dummy_cpu_actor = None


def reserve_resources(num_cpus, num_gpus, retries=20):
    ip = ray.util.get_node_ip_address()

    reserved_cuda_device = None

    if num_cpus > 0:
        global _dummy_cpu_actor
        if _dummy_cpu_actor is None:
            _dummy_cpu_actor = ray.remote(
                num_cpus=num_cpus, resources={"node:" + ip: 0.1}
            )(_DummyActor).remote()
            assert ray.get(_dummy_cpu_actor.get.remote()) == 1

    if num_gpus == 0:
        return reserved_cuda_device

    cuda_devices = os.environ.get("CUDA_VISIBLE_DEVICES")
    cuda_device_set = {}
    match_devices = bool(cuda_devices)
    if match_devices:
        logger.debug(f"Found set devices: {cuda_devices}")
        assert isinstance(cuda_devices, str)
        cuda_device_set = set(cuda_devices.split(","))

    global _dummy_cuda_actor
    unused_actors = []

    success = False
    for _ in range(retries):
        if _dummy_cuda_actor is None:
            _dummy_cuda_actor = ray.remote(
                num_cpus=0, num_gpus=num_gpus, resources={"node:" + ip: 0.1}
            )(_DummyActor).remote()

        reserved_cuda_device = ray.get(_dummy_cuda_actor.cuda_devices.remote())

        if match_devices and reserved_cuda_device not in cuda_device_set:
            logger.debug(
                "Device %s not in list of visible devices %s",
                reserved_cuda_device,
                cuda_device_set,
            )
            unused_actors.append(_dummy_cuda_actor)
            _dummy_cuda_actor = None
        else:
            logger.debug("Found matching device %s", reserved_cuda_device)
            success = True
            for actor in unused_actors:
                actor.__ray_terminate__.remote()
            break

    if not success:
        raise RuntimeError(
            "Unable to reserve the set CUDA VISIBLE DEVICES on Ray. Please "
            "make sure that Ray has access to all the visible devices: "
            "{}".format(os.environ.get("CUDA_VISIBLE_DEVICES"))
        )

    return reserved_cuda_device


class LocalDistributedRunner(DistributedTorchRunner):
    """A wrapper for running a distributed Runner on the driver.

    A dummy actor is used to reserve resources on the driver node,
    as specified by `num_cpus` and `num_gpus`. If the Trainer is already
    in an actor, we will ignore this resource request.
    """

    def __init__(self, *args, num_cpus=None, num_gpus=None, **kwargs):

        # Reserve a local GPU or CPU for the local worker
        # TODO: we should make sure this NEVER dies.
        self.local_cuda_device = "0"
        self._is_set = False
        if num_gpus:
            assert num_gpus == 1, "Does not support multi-gpu workers"

        if num_cpus is None:
            num_cpus = 0

        if num_gpus is None:
            num_gpus = 0

        if not self.is_actor() and (num_cpus > 0 or num_gpus > 0):
            self._try_reserve_and_set_resources(num_cpus, num_gpus)

        super(LocalDistributedRunner, self).__init__(*args, **kwargs)

    def _try_reserve_and_set_resources(self, num_cpus, num_gpus):
        visible_cuda_devices = os.environ.get("CUDA_VISIBLE_DEVICES")
        reserved_cuda_device = reserve_resources(num_cpus, num_gpus)
        if num_gpus == 0:
            return
        # This needs to be set even if torch.cuda is already
        # initialized because the env var is used later when
        # starting the DDP setup.
        os.environ["CUDA_VISIBLE_DEVICES"] = reserved_cuda_device
        if visible_cuda_devices:
            # We want to set the index on the visible devices list.
            if reserved_cuda_device not in visible_cuda_devices:
                raise RuntimeError(
                    "TorchTrainer reserved a device {} that was not in the "
                    "CUDA_VISIBLE_DEVICES {}. This may be because the "
                    "Ray cluster is not set with the right env vars. "
                    "If that is not the issue, please raise a Github "
                    "issue.".format(reserved_cuda_device, visible_cuda_devices)
                )
            devices = visible_cuda_devices.split(",")
            scoped_index = devices.index(reserved_cuda_device)
            self._set_cuda_device(str(scoped_index))
        else:
            # Once cuda is initialized, torch.device ignores the os.env
            # so we have to set the right actual device.
            self._set_cuda_device(reserved_cuda_device)

    def _set_cuda_device(self, device_str):
        """Sets the CUDA device for this current local worker."""
        if self._is_set:
            raise RuntimeError("CUDA devices already set.")
        self._is_set = True

        # This is idempotent. We need to call it
        # before we call 'set_device'.
        _init_cuda_context()
        assert isinstance(device_str, str)
        self.local_cuda_device = device_str
        logger.debug("Setting local CUDA device: %s", self.local_cuda_device)
        try:
            torch.cuda.set_device(int(self.local_cuda_device))
        except RuntimeError:
            logger.error("Failed to set local CUDA device.")
            raise

    def get_device(self):
        device_str = "cuda:" + self.local_cuda_device
        return torch.device(device_str)

    def shutdown(self, cleanup=True):
        super(LocalDistributedRunner, self).shutdown()
        global _dummy_cpu_actor
        global _dummy_cuda_actor
        if cleanup:
            if _dummy_cpu_actor or _dummy_cuda_actor:
                assert not self.is_actor(), "Actor shouldn't have a dummy actor."
            if _dummy_cpu_actor:
                ray.kill(_dummy_cpu_actor)
            if _dummy_cuda_actor:
                ray.kill(_dummy_cuda_actor)
            _dummy_cpu_actor = None
            _dummy_cuda_actor = None

    def is_actor(self):
        actor_id = ray.worker.global_worker.actor_id
        return actor_id != actor_id.nil()
