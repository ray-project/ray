import collections
import logging
import os
import random
import types
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import torch
from packaging.version import Version
from torch.cuda.amp import GradScaler, autocast
from torch.nn.parallel import DistributedDataParallel
from torch.optim import Optimizer
from torch.utils.data import (
    DataLoader,
    DistributedSampler,
    IterableDataset,
    RandomSampler,
    SequentialSampler,
)

from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.air._internal.device_manager import (
    get_torch_device_manager_by_context,
    get_torch_device_manager_by_device_type,
)
from ray.train._internal import session
from ray.train._internal.accelerator import Accelerator
from ray.train._internal.session import get_accelerator, set_accelerator
from ray.train.utils import _log_deprecation_warning
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="stable")
def get_device() -> torch.device:
    """Gets the correct torch device configured for this process.

    Returns the torch device for the current worker. If more than 1 GPU is
    requested per worker, returns the device with the minimal device index.

    .. note::

        If you requested multiple GPUs per worker, and want to get
        the full list of torch devices, please use
        :meth:`~ray.train.torch.get_devices`.

    Assumes that `CUDA_VISIBLE_DEVICES` is set and is a
    superset of the `ray.get_gpu_ids()`.

    Examples:

        Example: Launched 2 workers on the current node, each with 1 GPU

        .. testcode::
            :skipif: True

            os.environ["CUDA_VISIBLE_DEVICES"] = "2,3"
            ray.get_gpu_ids() == [2]
            torch.cuda.is_available() == True
            get_device() == torch.device("cuda:0")

        Example: Launched 4 workers on the current node, each with 1 GPU

        .. testcode::
            :skipif: True

            os.environ["CUDA_VISIBLE_DEVICES"] = "0,1,2,3"
            ray.get_gpu_ids() == [2]
            torch.cuda.is_available() == True
            get_device() == torch.device("cuda:2")

        Example: Launched 2 workers on the current node, each with 2 GPUs

        .. testcode::
            :skipif: True

            os.environ["CUDA_VISIBLE_DEVICES"] = "0,1,2,3"
            ray.get_gpu_ids() == [2,3]
            torch.cuda.is_available() == True
            get_device() == torch.device("cuda:2")


        You can move a model to device by:

        .. testcode::
            :skipif: True

            model.to(ray.train.torch.get_device())

        Instead of manually checking the device type:

        .. testcode::
            :skipif: True

            model.to("cuda" if torch.cuda.is_available() else "cpu")
    """
    from ray.air._internal import torch_utils

    record_extra_usage_tag(TagKey.TRAIN_TORCH_GET_DEVICE, "1")
    return torch_utils.get_devices()[0]


@PublicAPI(stability="beta")
def get_devices() -> List[torch.device]:
    """Gets the correct torch device list configured for this process.

    Assumes that `CUDA_VISIBLE_DEVICES` is set and is a
    superset of the `ray.get_gpu_ids()`.


    Examples:

        Example: Launched 2 workers on the current node, each with 1 GPU

        .. testcode::
            :skipif: True

            os.environ["CUDA_VISIBLE_DEVICES"] == "2,3"
            ray.get_gpu_ids() == [2]
            torch.cuda.is_available() == True
            get_devices() == [torch.device("cuda:0")]

        Example: Launched 4 workers on the current node, each with 1 GPU

        .. testcode::
            :skipif: True

            os.environ["CUDA_VISIBLE_DEVICES"] == "0,1,2,3"
            ray.get_gpu_ids() == [2]
            torch.cuda.is_available() == True
            get_devices() == [torch.device("cuda:2")]

        Example: Launched 2 workers on the current node, each with 2 GPUs

        .. testcode::
            :skipif: True

            os.environ["CUDA_VISIBLE_DEVICES"] == "0,1,2,3"
            ray.get_gpu_ids() == [2,3]
            torch.cuda.is_available() == True
            get_devices() == [torch.device("cuda:2"), torch.device("cuda:3")]
    """

    from ray.air._internal import torch_utils

    record_extra_usage_tag(TagKey.TRAIN_TORCH_GET_DEVICES, "1")
    return torch_utils.get_devices()


@PublicAPI(stability="stable")
def prepare_model(
    model: torch.nn.Module,
    move_to_device: Union[bool, torch.device] = True,
    parallel_strategy: Optional[str] = "ddp",
    parallel_strategy_kwargs: Optional[Dict[str, Any]] = None,
) -> torch.nn.Module:
    """Prepares the model for distributed execution.

    This allows you to use the same exact code regardless of number of
    workers or the device type being used (CPU, GPU).

    Args:
        model (torch.nn.Module): A torch model to prepare.
        move_to_device: Either a boolean indiciating whether to move
            the model to the correct device or an actual device to
            move the model to. If set to False, the model needs
            to manually be moved to the correct device.
        parallel_strategy ("ddp", "fsdp", or None): Whether to wrap models
            in ``DistributedDataParallel``, ``FullyShardedDataParallel``,
            or neither.
        parallel_strategy_kwargs (Dict[str, Any]): Args to pass into
            ``DistributedDataParallel`` or ``FullyShardedDataParallel``
            initialization if ``parallel_strategy`` is set to "ddp"
            or "fsdp", respectively.
    """
    if parallel_strategy == "fsdp" and Version(torch.__version__) < Version("1.11.0"):
        raise ImportError(
            "FullyShardedDataParallel requires torch>=1.11.0. "
            "Run `pip install 'torch>=1.11.0'` to use FullyShardedDataParallel."
        )

    record_extra_usage_tag(TagKey.TRAIN_TORCH_PREPARE_MODEL, "1")
    return get_accelerator(_TorchAccelerator).prepare_model(
        model,
        move_to_device=move_to_device,
        parallel_strategy=parallel_strategy,
        parallel_strategy_kwargs=parallel_strategy_kwargs,
    )


@PublicAPI(stability="stable")
def prepare_data_loader(
    data_loader: torch.utils.data.DataLoader,
    add_dist_sampler: bool = True,
    move_to_device: bool = True,
    auto_transfer: bool = True,
) -> torch.utils.data.DataLoader:
    """Prepares :class:`~torch.utils.data.DataLoader` for distributed execution.

    This allows you to use the same exact code regardless of number of
    workers or the device type being used (CPU, GPU).

    .. note::

        This method adds a `DistributedSampler` to the `DataLoader` if the
        number of training workers is greater than 1. If shuffling is
        enabled on the original `DataLoader`, then `shuffle=True` will also
        be passed into the `DistributedSampler` constructor. `shuffle=False`
        on the original `DataLoader` also means that shuffling is disabled
        on the sampler.

        With more than 1 worker, calling the `DistributedSampler.set_epoch` method
        at the beginning of each epoch before creating the DataLoader iterator
        is necessary to make shuffling work properly across multiple epochs.
        Otherwise, the same ordering will be always used.
        See: https://pytorch.org/docs/stable/data.html#torch.utils.data.distributed.DistributedSampler  # noqa: E501

    Example:

    .. testcode:
        :skipif: True

        import torch

        import ray.train.torch

        train_dataloader = torch.utils.data.DataLoader(
            ..., batch_size=..., shuffle=True
        )
        train_dataloader = ray.train.torch.prepare_data_loader(train_loader)

        for epoch in range(10):
            if ray.train.get_context().get_world_size() > 1:
                # Required for the distributed sampler to shuffle properly across epochs
                train_dataloader.sampler.set_epoch(epoch)

            for X, y in train_loader:
                # No need to move data to GPU, this is done by `prepare_data_loader`!
                # X, y = X.to("cuda"), y.to("cuda")
                ...

    Args:
        data_loader (torch.utils.data.DataLoader): The DataLoader to
            prepare.
        add_dist_sampler: Whether to add a DistributedSampler to
            the provided DataLoader.
        move_to_device: If set, automatically move the data
            returned by the data loader to the correct device.
        auto_transfer: If set and device is GPU, another CUDA stream
            is created to automatically copy data from host (CPU) memory
            to device (GPU) memory (the default CUDA stream still runs the
            training procedure). If device is CPU, it will be disabled
            regardless of the setting. This configuration will be ignored
            if ``move_to_device`` is False.
    """
    record_extra_usage_tag(TagKey.TRAIN_TORCH_PREPARE_DATALOADER, "1")
    return get_accelerator(_TorchAccelerator).prepare_data_loader(
        data_loader,
        add_dist_sampler=add_dist_sampler,
        move_to_device=move_to_device,
        auto_transfer=auto_transfer,
    )


def _log_amp_deprecation_warning():
    # Keep V2 imports out of top-level V1 imports.
    from ray.train.v2.torch.train_loop_utils import _TORCH_AMP_DEPRECATION_MESSAGE

    _log_deprecation_warning(_TORCH_AMP_DEPRECATION_MESSAGE)


@Deprecated
def accelerate(amp: bool = False) -> None:
    """[Deprecated] Enables training optimizations.

    Arguments:
        amp: If true, perform training with automatic mixed precision.
            Otherwise, use full precision.

    .. warning:: ``train.torch.accelerate`` cannot be called more than once, and it
       must be called before any other ``train.torch`` utility function.
    """
    _log_amp_deprecation_warning()
    try:
        set_accelerator(_TorchAccelerator(amp=amp))
    except RuntimeError:
        raise RuntimeError(
            "An accelerator has already been set. Make sure "
            "`train.torch.accelerate()` is not called multiple times, and is called "
            "before any of the prepare methods."
        )


@Deprecated
def prepare_optimizer(optimizer: torch.optim.Optimizer) -> torch.optim.Optimizer:
    """[Deprecated] Wraps optimizer to support automatic mixed precision.

    Args:
        optimizer (torch.optim.Optimizer): The DataLoader to prepare.

    Returns:
        A wrapped optimizer.
    """
    _log_amp_deprecation_warning()
    return get_accelerator(_TorchAccelerator).prepare_optimizer(optimizer)


@Deprecated
def backward(tensor: torch.Tensor) -> None:
    """[Deprecated] Computes the gradient of the specified tensor w.r.t. graph leaves.

    Args:
        tensor (torch.Tensor): Tensor of which the derivative will be computed.
    """
    _log_amp_deprecation_warning()
    get_accelerator(_TorchAccelerator).backward(tensor)


@PublicAPI(stability="stable")
def enable_reproducibility(seed: int = 0) -> None:
    """Limits sources of nondeterministic behavior.

    This function:

        * Seeds PyTorch, Python, and NumPy.
        * Disables CUDA convolution benchmarking.
        * Configures PyTorch to use determinstic algorithms.
        * Seeds workers spawned for multi-process data loading.

    Args:
        seed: The number to seed libraries and data workers with.

    .. warning:: ``train.torch.enable_reproducibility()`` can't guarantee
        completely reproducible results across executions. To learn more, read
        the `PyTorch notes on randomness
        <https://pytorch.org/docs/stable/notes/randomness.html>`_.
    """
    get_accelerator(_TorchAccelerator).enable_reproducibility(seed)


@Deprecated
class TorchWorkerProfiler:
    """Utility class for running PyTorch Profiler on a Train worker.

    Args:
        trace_dir (Optional[str]): The directory to store traces on the
           worker node. If ``None``, this will use a default temporary dir.
    """

    WORKER_TRACE_DIR_NAME = "pytorch_profiler_worker_traces"

    def __init__(self, trace_dir: Optional[str] = None):
        raise DeprecationWarning(
            "The `ray.train.torch.TorchWorkerProfiler` API is deprecated in Ray 2.0.",
        )


class _TorchAccelerator(Accelerator):
    """A utility that implements methods to accelerate PyTorch training.

    Arguments:
        amp: If true, perform training with automatic mixed precision.
            Otherwise, use full precision.
    """

    def __init__(self, amp: bool = False):
        self.amp_is_enabled = amp
        self.scaler = GradScaler() if amp else None
        self._seed = None
        self.device_manager = get_torch_device_manager_by_context()

    def prepare_model(
        self,
        model: torch.nn.Module,
        move_to_device: bool = True,
        parallel_strategy: Optional[str] = "ddp",
        parallel_strategy_kwargs: Optional[Dict[str, Any]] = None,
    ) -> torch.nn.Module:
        """Prepares the model for distributed execution.

        This allows you to use the same exact code regardless of number of
        workers or the device type being used (CPU, GPU).

        Args:
            model (torch.nn.Module): A torch model to prepare.
            move_to_device: Whether to move the model to the correct
                device. If set to False, the model needs to manually be moved
                to the correct device.
            parallel_strategy ("ddp", "fsdp", or None): Whether to wrap models
                in ``DistributedDataParallel``, ``FullyShardedDataParallel`` (
                Experimental), or neither.
            parallel_strategy_kwargs (Dict[str, Any]): Args to pass into
                ``DistributedDataParallel`` or ``FullyShardedDataParallel``
                initialization if ``parallel_strategy`` is set to "ddp"
                or "fsdp", respectively.
        """
        parallel_strategy_kwargs = parallel_strategy_kwargs or {}

        rank = session.get_local_rank()

        if isinstance(move_to_device, torch.device):
            device = move_to_device
        else:
            device = get_device()
            if isinstance(device, list):
                device = device[0]

        if self.device_manager.is_available():
            self.device_manager.set_device(device)

        if move_to_device:
            if rank == 0:
                logger.info(f"Moving model to device: {device}")
            else:
                logger.debug(f"Moving model to device: {device}")
            model = model.to(device)

        def model_get_state(self):
            # `__getstate__` is an special method that informs pickle which attributes
            # to serialize. This custom implementation ensures that the wrapped forward
            # method and custom `__getstate__` method aren't serialized.
            if hasattr(self, "_original_get_state"):
                state = self._original_get_state()
                state["__getstate__"] = state["_original_get_state"]
                del state["_original_get_state"]
            else:
                # If model does not have a `__getstate__` already defined, use default
                # implementation.
                state = self.__dict__.copy()
                del state["__getstate__"]
            state["forward"] = state["_unwrapped_forward"]
            del state["_unwrapped_forward"]

            return state

        if self.amp_is_enabled:
            # Pickle cannot serialize the wrapped forward method. As a workaround,
            # define a custom `__getstate__` method that unwraps the forward method.
            model._unwrapped_forward = model.forward
            model.forward = autocast()(model.forward)

            # TODO(amogkam): Replace below logic with a generic "unpack model" method.
            # Replacing the `model.forward` method makes the model no longer
            # serializable. When serializing the model, we have to override the
            # `__getstate__` method to set back the original forward method.
            if hasattr(model, "__getstate__"):
                model._original_get_state = model.__getstate__
            # `__getstate__` must be a bound method rather than an callable attribute.
            # See https://stackoverflow.com/questions/972/adding-a-method-to-an-existing-object-instance.  # noqa: E501
            model.__getstate__ = types.MethodType(model_get_state, model)

        world_size = session.get_world_size()

        if parallel_strategy and world_size > 1:
            if parallel_strategy == "ddp":
                DataParallel = DistributedDataParallel
                if self.device_manager.is_available() and device.type != "cpu":
                    parallel_strategy_kwargs = {
                        "device_ids": [device],
                        "output_device": device,
                        **parallel_strategy_kwargs,
                    }
            else:
                if not torch.cuda.is_available():
                    raise RuntimeError(
                        "FSDP is only available with GPU-enabled "
                        "training. Set "
                        "`use_gpu=True` in your Trainer to train with "
                        "GPUs."
                    )
                from torch.distributed.fsdp import FullyShardedDataParallel

                DataParallel = FullyShardedDataParallel
            if rank == 0:
                logger.info(f"Wrapping provided model in {DataParallel.__name__}.")
            else:
                logger.debug(f"Wrapping provided model in {DataParallel.__name__}.")
            model = DataParallel(model, **parallel_strategy_kwargs)

        return model

    def prepare_data_loader(
        self,
        data_loader: torch.utils.data.DataLoader,
        add_dist_sampler: bool = True,
        move_to_device: bool = True,
        auto_transfer: bool = False,
    ) -> torch.utils.data.DataLoader:
        """Prepares DataLoader for distributed execution.

        This allows you to use the same exact code regardless of number of
        workers or the device type being used (CPU, GPU).

        Args:
            data_loader (torch.utils.data.DataLoader): The DataLoader to
                prepare.
            add_dist_sampler: Whether to add a DistributedSampler to
                the provided DataLoader.
            move_to_device: If set, automatically move the data
                returned by the data loader to the correct device.
            auto_transfer: (Experimental) If set and device is GPU, another CUDA stream
                is created to automatically copy data from host (CPU) memory
                to device (GPU) memory (the default CUDA stream still runs the
                training procedure). If device is CPU, it will be disabled
                regardless of the setting. This configuration will be ignored
                if ``move_to_device`` is False.
        """

        world_size = session.get_world_size()
        world_rank = session.get_world_rank()

        # Only add Distributed Sampler if the following conditions hold:
        # 1. More than one training worker is being used.
        # 2. A DistributedSampler has not already been added by the user.
        # 3. The dataset is not an IterableDataset. Samplers do not worker with
        # IterableDatasets.
        if (
            world_size > 1
            and not isinstance(data_loader.sampler, DistributedSampler)
            and not (
                hasattr(data_loader, "dataset")
                and isinstance(data_loader.dataset, IterableDataset)
            )
            and add_dist_sampler
        ):

            def with_sampler(loader):
                # Automatically set the DistributedSampler

                # If you're using a sampler, the DataLoader shuffle flag must be set to
                # False. Shuffling is instead determined by the shuffle argument passed
                # to the DistributedSampler constructor.

                # If no sampler is passed to the DataLoader constructor, Torch
                # constructs a default sampler. The default sampler is a RandomSampler
                # if shuffling is enabled and a SequentialSampler otherwise. DataLoader
                # does not have a shuffle attribute, so we instead identify whether
                # shuffling is enabled by checking the default sampler type.
                shuffle = not isinstance(loader.sampler, SequentialSampler)

                def seeded_worker_init_fn(
                    worker_init_fn: Optional[Callable[[int], None]],
                ):
                    def wrapper(worker_id: int):
                        worker_seed = torch.initial_seed() % 2**32
                        np.random.seed(worker_seed)
                        random.seed(worker_seed)
                        if worker_init_fn:
                            worker_init_fn(worker_id)

                    return wrapper

                worker_init_fn: Optional[Callable[[int], None]] = loader.worker_init_fn
                generator: Optional[torch.Generator] = loader.generator
                if self._seed is not None:
                    worker_init_fn = seeded_worker_init_fn(worker_init_fn)
                    generator = torch.Generator()
                    generator.manual_seed(self._seed)

                using_default_sampler = isinstance(
                    loader.sampler, (SequentialSampler, RandomSampler)
                )
                if not using_default_sampler and world_rank == 0:
                    logger.warning(
                        f"The {loader.sampler.__class__.__name__} will be overwritten "
                        "with a DistributedSampler. You can disable this by setting "
                        "`with_sampler` to False in `prepare_data_loader`."
                    )

                data_loader_args = {
                    "dataset": loader.dataset,
                    "batch_size": loader.batch_size,
                    "shuffle": False,
                    "num_workers": loader.num_workers,
                    "collate_fn": loader.collate_fn,
                    "pin_memory": loader.pin_memory,
                    "drop_last": loader.drop_last,
                    "timeout": loader.timeout,
                    "worker_init_fn": worker_init_fn,
                    "generator": generator,
                    "sampler": DistributedSampler(loader.dataset, shuffle=shuffle),
                }
                return DataLoader(**data_loader_args)

            data_loader = with_sampler(data_loader)

        if move_to_device:
            device = get_device()
            data_loader = _WrappedDataLoader(data_loader, device, auto_transfer)

        return data_loader

    def prepare_optimizer(self, optimizer: Optimizer) -> Optimizer:
        """Wraps optimizer to support automatic mixed precision.

        Args:
            optimizer (torch.optim.Optimizer): The DataLoader to prepare.

        Returns:
            A wrapped optimizer.
        """
        return _WrappedOptimizer(optimizer, scaler=self.scaler)

    def backward(self, tensor: torch.Tensor) -> None:
        """Computes the gradient of the specified tensor w.r.t. graph leaves.

        Args:
            tensor (torch.Tensor): Tensor of which the derivative will be computed.
        """
        if self.amp_is_enabled:
            self.scaler.scale(tensor).backward()
        else:
            tensor.backward()

    def enable_reproducibility(self, seed: int = 0) -> None:
        """Limits sources of nondeterministic behavior."""
        self._seed = seed

        torch.manual_seed(seed)
        random.seed(seed)
        np.random.seed(seed)

        torch.use_deterministic_algorithms(True)
        torch.backends.cudnn.benchmark = False

        # If you want to use deterministic algorithms with CUDA, then you need to set
        # the CUBLAS_WORKSPACE_CONFIG environment variable; otherwise, Torch errors.
        # See https://docs.nvidia.com/cuda/cublas/index.html#cublasApi_reproducibility.
        os.environ["CUBLAS_WORKSPACE_CONFIG"] = ":4096:8"


class _WrappedDataLoader(DataLoader):
    def __init__(
        self, base_dataloader: DataLoader, device: torch.device, auto_transfer: bool
    ):
        self.__dict__.update(getattr(base_dataloader, "__dict__", {}))
        self._dataloader = base_dataloader
        self.dataloader_iter = None
        self.device = device

        self.device_manager = get_torch_device_manager_by_device_type(device.type)

        # disable auto transfer (host->device) if cpu is used
        if device.type != "cpu" and self.device_manager.supports_stream():
            self._auto_transfer = auto_transfer
        else:
            self._auto_transfer = False
        # create a new device stream to move data from host to device concurrently
        self._memcpy_stream = (
            self.device_manager.create_stream(device)
            if device.type != "cpu" and self._auto_transfer
            else None
        )
        self.next_batch = None

    def _move_to_device(self, item):
        if item is None:
            return None

        def try_move_device(i):
            try:
                i = i.to(self.device, non_blocking=self._auto_transfer)
            except AttributeError:
                logger.debug(f"Item {i} cannot be moved to device " f"{self.device}.")
            return i

        with self.device_manager.get_stream_context(self._memcpy_stream):
            if isinstance(item, collections.abc.Mapping):
                item_on_device = {k: self._move_to_device(v) for k, v in item.items()}
            elif isinstance(item, tuple):
                item_on_device = tuple(self._move_to_device(i) for i in item)
            elif isinstance(item, list):
                item_on_device = [self._move_to_device(i) for i in item]
            elif isinstance(item, torch.Tensor):
                item_on_device = try_move_device(item)
            else:
                logger.debug(
                    f"Data type {type(item)} doesn't support being moved to device."
                )
                item_on_device = item

            return item_on_device

    def _wait_for_batch(self, item):
        if self._memcpy_stream is None:
            return
        # Reference:
        # https://pytorch.org/docs/stable/generated/torch.Tensor.record_stream.html
        # The training stream (current) needs to wait until
        # the memory copy stream finishes.
        curr_stream = self.device_manager.get_current_stream()
        curr_stream.wait_stream(self._memcpy_stream)
        # When a tensor is used by CUDA streams different from
        # its original allocator, we need to call ``record_stream``
        # to inform the allocator of all these streams. Otherwise,
        # the tensor might be freed once it is no longer used by
        # the creator stream.
        for i in item:
            # The Pytorch DataLoader has no restrictions on what is outputted for
            # each batch. We should only ``record_stream`` if the item has the
            # ability to do so.
            try:
                i.record_stream(curr_stream)
            except AttributeError:
                pass

    def __len__(self):
        return len(self._dataloader)

    def _prefetch_next_batch(self):
        next_batch = next(self.dataloader_iter, None)
        self.next_batch = self._move_to_device(next_batch)

    def __iter__(self):
        self.dataloader_iter = iter(self._dataloader)
        self._prefetch_next_batch()
        return self

    def __next__(self):
        next_batch = self.next_batch
        if next_batch is None:
            raise StopIteration
        self._wait_for_batch(next_batch)
        self._prefetch_next_batch()
        return next_batch


class _WrappedOptimizer(Optimizer):
    def __init__(self, optimizer: Optimizer, scaler: Optional[GradScaler] = None):
        self.optimizer = optimizer
        self.scaler = scaler

    @property
    def state(self):
        return self.optimizer.state

    @state.setter
    def state(self, state):
        self.optimizer.state = state

    @property
    def param_groups(self):
        return self.optimizer.param_groups

    @param_groups.setter
    def param_groups(self, param_groups):
        self.optimizer.param_groups = param_groups

    @property
    def defaults(self):
        return self.optimizer.defaults

    @defaults.setter
    def defaults(self, defaults):
        self.optimizer.defaults = defaults

    def add_param_group(self, param_group):
        self.optimizer.add_param_group(param_group)

    def load_state_dict(self, state_dict):
        self.optimizer.load_state_dict(state_dict)

    def state_dict(self):
        return self.optimizer.state_dict()

    def zero_grad(self):
        self.optimizer.zero_grad()

    def step(self, closure=None):
        if self.scaler is not None:
            self.scaler.step(self.optimizer, closure)
            self.scaler.update()
        else:
            self.optimizer.step(closure)
