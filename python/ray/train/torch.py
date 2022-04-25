import tempfile
from dataclasses import dataclass
import functools
import io
import logging
import os
import random
import types

from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import ray
from ray import train
from ray.train.accelerator import Accelerator
from ray.train.backend import BackendConfig, Backend, EncodedData
from ray.train.constants import PYTORCH_PROFILER_KEY
from torch.optim import Optimizer
from ray.train.session import get_accelerator, set_accelerator
from ray.train.worker_group import WorkerGroup
from ray.train.utils import get_address_and_port
from ray.util import PublicAPI

import numpy as np

try:
    import torch
    from torch.cuda.amp import autocast, GradScaler
    import torch.distributed as dist
    from torch.nn.parallel import DistributedDataParallel
    from torch.utils.data import (
        DistributedSampler,
        DataLoader,
        IterableDataset,
        SequentialSampler,
        RandomSampler,
    )
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "PyTorch isn't installed. To install PyTorch, run 'pip install torch'"
    )

try:
    from torch.profiler import profile
except ImportError:
    profile = None

logger = logging.getLogger(__name__)


class TorchAccelerator(Accelerator):
    """A utility that implements methods to accelerate PyTorch training.

    Arguments:
        amp (bool): If true, perform training with automatic mixed precision.
            Otherwise, use full precision.
    """

    def __init__(self, amp: bool = False):
        self.amp_is_enabled = amp
        self.scaler = GradScaler() if amp else None
        self._seed = None

    def prepare_model(
        self,
        model: torch.nn.Module,
        move_to_device: bool = True,
        wrap_ddp: bool = True,
        ddp_kwargs: Optional[Dict[str, Any]] = None,
    ) -> torch.nn.Module:
        """Prepares the model for distributed execution.

        This allows you to use the same exact code regardless of number of
        workers or the device type being used (CPU, GPU).

        Args:
            model (torch.nn.Module): A torch model to prepare.
            move_to_device (bool): Whether to move the model to the correct
                device. If set to False, the model needs to manually be moved
                to the correct device.
            wrap_ddp (bool): Whether to wrap models in
                ``DistributedDataParallel``.
            ddp_kwargs (Dict[str, Any]): Args to pass into
                ``DistributedDataParallel`` initialization if ``wrap_ddp`` is
                set to True.
        """
        ddp_kwargs = ddp_kwargs or {}

        rank = train.local_rank()

        device = self.get_device()

        if torch.cuda.is_available():
            torch.cuda.set_device(device)

        if move_to_device:
            logger.info(f"Moving model to device: {device}")
            model = model.to(device)

        def wrap_forward(forward):
            @functools.wraps(forward)
            def wrapper(*args, **kwargs):
                with autocast():
                    outputs = forward(*args, **kwargs)
                assert isinstance(outputs, torch.Tensor)
                return outputs.float()

            return wrapper

        def model_get_state(self):
            # `__getstate__` is an special method that informs pickle which attributes
            # to serialize. This custom implementation ensures that the wrapped forward
            # method and custom `__getstate__` method aren't serialized.
            state = self.__dict__.copy()
            state["forward"] = state["_unwrapped_forward"]
            del state["_unwrapped_forward"]
            del state["__getstate__"]
            return state

        if self.amp_is_enabled:
            # Pickle cannot serialize the wrapped forward method. As a workaround,
            # define a custom `__getstate__` method that unwraps the forward method.
            model._unwrapped_forward = model.forward
            model.forward = wrap_forward(model.forward)
            # `__getstate__` must be a bound method rather than an callable attribute.
            # See https://stackoverflow.com/questions/972/adding-a-method-to-an-existing-object-instance.  # noqa: E501
            assert not hasattr(model, "__getstate__")
            model.__getstate__ = types.MethodType(model_get_state, model)

        if wrap_ddp and train.world_size() > 1:
            logger.info("Wrapping provided model in DDP.")
            if torch.cuda.is_available():
                model = DistributedDataParallel(
                    model, device_ids=[rank], output_device=rank, **ddp_kwargs
                )
            else:
                model = DistributedDataParallel(model, **ddp_kwargs)

        return model

    def prepare_data_loader(
        self,
        data_loader: torch.utils.data.DataLoader,
        add_dist_sampler: bool = True,
        move_to_device: bool = True,
        auto_transfer: bool = True,
    ) -> torch.utils.data.DataLoader:
        """Prepares DataLoader for distributed execution.

        This allows you to use the same exact code regardless of number of
        workers or the device type being used (CPU, GPU).

        Args:
            data_loader (torch.utils.data.DataLoader): The DataLoader to
                prepare.
            add_dist_sampler (bool): Whether to add a DistributedSampler to
                the provided DataLoader.
            move_to_device (bool): If set, automatically move the data
                returned by the data loader to the correct device.
            auto_transfer (bool): If set and device is GPU, another CUDA stream
                is created to automatically copy data from host (CPU) memory
                to device (GPU) memory (the default CUDA stream still runs the
                training procedure). If device is CPU, it will be disabled
                regardless of the setting. This configuration will be ignored
                if ``move_to_device`` is False.
        """

        # Only add Distributed Sampler if the following conditions hold:
        # 1. More than one training worker is being used.
        # 2. A DistributedSampler has not already been added by the user.
        # 3. The dataset is not an IterableDataset. Samplers do not worker with
        # IterableDatasets.
        if (
            train.world_size() > 1
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

                def seeded_worker_init_fn(worker_init_fn):
                    def wrapper(worker_id):
                        worker_seed = torch.initial_seed() % 2 ** 32
                        np.random.seed(worker_seed)
                        random.seed(worker_seed)
                        worker_init_fn(worker_id)

                    return wrapper

                worker_init_fn = loader.worker_init_fn
                generator = loader.generator
                if self._seed is not None:
                    worker_init_fn = seeded_worker_init_fn(loader.worker_init_fn)
                    generator = torch.Generator()
                    generator.manual_seed(self._seed)

                using_default_sampler = isinstance(
                    loader.sampler, (SequentialSampler, RandomSampler)
                )
                if not using_default_sampler and train.world_rank() == 0:
                    logger.warn(
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
            device = self.get_device()
            data_loader = _WrappedDataLoader(data_loader, device, auto_transfer)

        return data_loader

    def get_device(self) -> torch.device:
        """Gets the correct torch device to use for training."""
        if torch.cuda.is_available():
            gpu_ids = ray.get_gpu_ids()
            if len(gpu_ids) > 0:
                device_id = gpu_ids[0]
            else:
                # If called on the driver or outside of Ray Train, return the
                # 0th device.
                device_id = 0
            device = torch.device(f"cuda:{device_id}")
        else:
            device = torch.device("cpu")

        return device

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


@PublicAPI(stability="beta")
@dataclass
class TorchConfig(BackendConfig):
    """Configuration for torch process group setup.

    See https://pytorch.org/docs/stable/distributed.html for more info.

    Args:
        backend (str): The backend to use for training.
            See ``torch.distributed.init_process_group`` for more info and
            valid values.
            If set to None, nccl will be used if GPUs are requested, else gloo
            will be used.
        init_method (str): The initialization method to use. Either "env"
            for environment variable initialization or "tcp" for TCP
            initialization. Defaults to "env".
        timeout_s (int): Seconds for process group operations to timeout.
    """

    backend: Optional[str] = None
    init_method: str = "env"
    timeout_s: int = 1800

    @property
    def backend_cls(self):
        return TorchBackend


def setup_torch_process_group(
    backend: str,
    world_rank: int,
    world_size: int,
    init_method: str,
    timeout_s: int = 1800,
):
    """Connects the distributed PyTorch backend.

    Args:
        backend (str): The backend (nccl, gloo, etc.) to use for training.
        world_rank (int): Rank of the current worker.
        world_size (int): Number of workers participating in the job.
        init_method (str): URL specifying how to initialize the process group.
        timeout_s (timedelta): Seconds for process group operations to timeout.
    """
    logger.info(
        f"Setting up process group for: {init_method} [rank={world_rank}, "
        f"world_size={world_size}]"
    )
    logger.debug(f"using {backend}")

    if backend == "nccl" and "NCCL_BLOCKING_WAIT" not in os.environ:
        logger.debug(
            "Setting NCCL_BLOCKING_WAIT for detecting node failure. "
            "To override this behavior, you can set NCCL_BLOCKING_WAIT=0."
        )
        os.environ["NCCL_BLOCKING_WAIT"] = "1"

    dist.init_process_group(
        backend=backend,
        init_method=init_method,
        rank=world_rank,
        world_size=world_size,
        timeout=timedelta(seconds=timeout_s),
    )


def shutdown_torch(destroy_process_group=False):
    if destroy_process_group:
        dist.destroy_process_group()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


class TorchBackend(Backend):
    share_cuda_visible_devices: bool = True

    def on_start(self, worker_group: WorkerGroup, backend_config: TorchConfig):
        if dist.is_available():
            # Set the appropriate training backend.
            if backend_config.backend is None:
                if worker_group.num_gpus_per_worker > 0:
                    backend = "nccl"
                else:
                    backend = "gloo"
            else:
                backend = backend_config.backend

            master_addr, master_port = worker_group.execute_single(
                0, get_address_and_port
            )
            if backend_config.init_method == "env":

                def set_env_vars(addr, port):
                    os.environ["MASTER_ADDR"] = addr
                    os.environ["MASTER_PORT"] = str(port)

                worker_group.execute(set_env_vars, addr=master_addr, port=master_port)
                url = "env://"
            elif backend_config.init_method == "tcp":
                url = f"tcp://{master_addr}:{master_port}"
            else:
                raise ValueError(
                    f"The provided init_method ("
                    f"{backend_config.init_method}) is not supported. Must "
                    f"be either 'env' or 'tcp'."
                )

            setup_futures = []
            for i in range(len(worker_group)):
                setup_futures.append(
                    worker_group.execute_single_async(
                        i,
                        setup_torch_process_group,
                        backend=backend,
                        world_rank=i,
                        world_size=len(worker_group),
                        init_method=url,
                        timeout_s=backend_config.timeout_s,
                    )
                )
            ray.get(setup_futures)
        else:
            raise RuntimeError("Distributed torch is not available.")

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: TorchConfig):

        worker_group.execute(
            shutdown_torch, destroy_process_group=len(worker_group) > 1
        )

    @staticmethod
    def encode_data(data_dict: Dict) -> EncodedData:
        """Special handling for moving model from worker to driver."""

        # If model is being checkpointed and is wrapped in DDP, then extract
        # out the underlying module. If not, then deserialization will fail
        # since the torch process group is not initialized on the driver.

        for k, v in data_dict.items():
            if isinstance(v, DistributedDataParallel) and hasattr(v, "module"):
                data_dict[k] = v.module

        # Convert the checkpoint dict to bytes, so that any GPU tensors that
        # are in the checkpoint dict can be properly deserialized on the
        # driver side, even if the driver does not have access to a GPU device.
        _buffer = io.BytesIO()
        torch.save(data_dict, _buffer)
        return _buffer.getvalue()

    @staticmethod
    def decode_data(encoded_data: EncodedData) -> Dict:
        # When decoding the bytes on the driver side, always map to CPU.
        _buffer = io.BytesIO(encoded_data)
        checkpoint_dict = torch.load(_buffer, map_location="cpu")
        return checkpoint_dict


class _WrappedDataLoader(DataLoader):
    def __init__(
        self, base_dataloader: DataLoader, device: torch.device, auto_transfer: bool
    ):

        self.__dict__.update(getattr(base_dataloader, "__dict__", {}))
        self._dataloader = base_dataloader
        self.dataloader_iter = None
        self.device = device
        # disable auto transfer (host->device) if cpu is used
        self._auto_transfer = auto_transfer if device.type == "cuda" else False
        # create a new CUDA stream to move data from host to device concurrently
        self._memcpy_stream = (
            torch.cuda.Stream()
            if device.type == "cuda" and self._auto_transfer
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

        with torch.cuda.stream(self._memcpy_stream):
            return tuple(try_move_device(i) for i in item)

    def _wait_for_batch(self, item):
        if self._memcpy_stream is None:
            return
        # Reference:
        # https://pytorch.org/docs/stable/generated/torch.Tensor.record_stream.html
        # The training stream (current) needs to wait until
        # the memory copy stream finishes.
        curr_stream = torch.cuda.current_stream()
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


@PublicAPI(stability="beta")
def get_device() -> torch.device:
    """Gets the correct torch device to use for training."""
    return get_accelerator(TorchAccelerator).get_device()


@PublicAPI(stability="beta")
def prepare_model(
    model: torch.nn.Module,
    move_to_device: bool = True,
    wrap_ddp: bool = True,
    ddp_kwargs: Optional[Dict[str, Any]] = None,
) -> torch.nn.Module:
    """Prepares the model for distributed execution.

    This allows you to use the same exact code regardless of number of
    workers or the device type being used (CPU, GPU).

    Args:
        model (torch.nn.Module): A torch model to prepare.
        move_to_device (bool): Whether to move the model to the correct
            device. If set to False, the model needs to manually be moved
            to the correct device.
        wrap_ddp (bool): Whether to wrap models in
            ``DistributedDataParallel``.
        ddp_kwargs (Dict[str, Any]): Args to pass into
            ``DistributedDataParallel`` initialization if ``wrap_ddp`` is
            set to True.
    """
    return get_accelerator(TorchAccelerator).prepare_model(
        model,
        move_to_device=move_to_device,
        wrap_ddp=wrap_ddp,
        ddp_kwargs=ddp_kwargs,
    )


@PublicAPI(stability="beta")
def prepare_data_loader(
    data_loader: torch.utils.data.DataLoader,
    add_dist_sampler: bool = True,
    move_to_device: bool = True,
    auto_transfer: bool = True,
) -> torch.utils.data.DataLoader:
    """Prepares DataLoader for distributed execution.

    This allows you to use the same exact code regardless of number of
    workers or the device type being used (CPU, GPU).

    Args:
        data_loader (torch.utils.data.DataLoader): The DataLoader to
            prepare.
        add_dist_sampler (bool): Whether to add a DistributedSampler to
            the provided DataLoader.
        move_to_device (bool): If set, automatically move the data
            returned by the data loader to the correct device.
        auto_transfer (bool): If set and device is GPU, another CUDA stream
            is created to automatically copy data from host (CPU) memory
            to device (GPU) memory (the default CUDA stream still runs the
            training procedure). If device is CPU, it will be disabled
            regardless of the setting. This configuration will be ignored
            if ``move_to_device`` is False.
    """
    return get_accelerator(TorchAccelerator).prepare_data_loader(
        data_loader,
        add_dist_sampler=add_dist_sampler,
        move_to_device=move_to_device,
        auto_transfer=auto_transfer,
    )


@PublicAPI(stability="beta")
def accelerate(amp: bool = False) -> None:
    """Enables training optimizations.

    Arguments:
        amp (bool): If true, perform training with automatic mixed precision.
            Otherwise, use full precision.

    .. warning:: ``train.torch.accelerate`` cannot be called more than once, and it
       must be called before any other ``train.torch`` utility function.
    """
    try:
        set_accelerator(TorchAccelerator(amp=amp))
    except RuntimeError:
        raise RuntimeError(
            "An accelerator has already been set. Make sure "
            "`train.torch.accelerate()` is not called multiple times, and is called "
            "before any of the prepare methods."
        )


@PublicAPI(stability="beta")
def prepare_optimizer(optimizer: torch.optim.Optimizer) -> torch.optim.Optimizer:
    """Wraps optimizer to support automatic mixed precision.

    Args:
        optimizer (torch.optim.Optimizer): The DataLoader to prepare.

    Returns:
        A wrapped optimizer.
    """
    return get_accelerator(TorchAccelerator).prepare_optimizer(optimizer)


@PublicAPI(stability="beta")
def backward(tensor: torch.Tensor) -> None:
    """Computes the gradient of the specified tensor w.r.t. graph leaves.

    Args:
        tensor (torch.Tensor): Tensor of which the derivative will be computed.
    """
    get_accelerator(TorchAccelerator).backward(tensor)


def enable_reproducibility(seed: int = 0) -> None:
    """Limits sources of nondeterministic behavior.

    This function:

        * Seeds PyTorch, Python, and NumPy.
        * Disables CUDA convolution benchmarking.
        * Configures PyTorch to use determinstic algorithms.
        * Seeds workers spawned for multi-process data loading.

    Args:
        seed (int): The number to seed libraries and data workers with.

    .. warning:: ``train.torch.enable_reproducibility()`` can't guarantee
        completely reproducible results across executions. To learn more, read
        the `PyTorch notes on randomness
        <https://pytorch.org/docs/stable/notes/randomness.html>`_.
    """
    get_accelerator(TorchAccelerator).enable_reproducibility(seed)


WORKER_TRACE_DIR_NAME = "pytorch_profiler_worker_traces"


class TorchWorkerProfiler:
    """Utility class for running PyTorch Profiler on a Train worker.

    Args:
        trace_dir (Optional[str]): The directory to store traces on the
           worker node. If ``None``, this will use a default temporary dir.
    """

    def __init__(self, trace_dir: Optional[str] = None):
        if profile is None:
            raise ImportError(
                "Torch Profiler requires torch>=1.8.1. "
                "Run `pip install 'torch>=1.8.1'` to use TorchWorkerProfiler."
            )

        trace_dir = trace_dir or Path(tempfile.gettempdir()).joinpath(
            WORKER_TRACE_DIR_NAME
        )
        self.trace_dir = Path(trace_dir)
        self.trace_dir.mkdir(parents=True, exist_ok=True)
        # Accumulated traces.
        self.profiler_trace_filenames = []

    def trace_handler(self, p: profile):
        """A stateful PyTorch Profiler trace handler.

        This will the export chrome trace to a file on disk.

        These exported traces can then be fetched by calling
        ``get_and_clear_profile_traces``.

        Args:
            p (profile): A PyTorch Profiler profile.
        """
        trace_filename = f"worker_{train.world_rank()}_epoch_{p.step_num}.pt.trace.json"
        trace_path = self.trace_dir.joinpath(trace_filename)

        logger.debug(f"Writing worker trace to {trace_path}.")
        p.export_chrome_trace(str(trace_path))
        self.profiler_trace_filenames.append(trace_filename)

    def get_and_clear_profile_traces(self):
        """Reads unread Profiler traces from this worker.

        Returns:
            The traces in a format consumable by
            ``TorchTensorboardProfilerCallback``.
        """

        def get_trace(filename):
            trace_path = self.trace_dir.joinpath(filename)
            return trace_path.read_text()

        traces = [
            (trace_filename, get_trace(trace_filename))
            for trace_filename in self.profiler_trace_filenames
        ]

        self.profiler_trace_files = []
        return {PYTORCH_PROFILER_KEY: traces}
