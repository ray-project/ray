import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Dict

import torch
import torch.distributed as dist
from packaging.version import Version

import ray
from ray._common.network_utils import build_address
from ray.air._internal.device_manager import register_custom_torch_dist_backend
from ray.train._internal.utils import get_address_and_port
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


class TorchConfigContextManager:
    def __enter__(self):
        # Set default cuda device
        if torch.cuda.is_available():
            device = ray.train.torch.get_device()
            if device.type == "cuda":
                torch.cuda.set_device(device)
        # TODO(lehui): we will need to set the correct device for TPU training
        # Set default TPU device for TPU training
        elif hasattr(ray.train.torch, "get_device"):
            try:
                device = ray.train.torch.get_device()
                if device.type == "xla":
                    # For TPU training, the device is already set up in the backend
                    # Just log that we're using TPU
                    logger.info(f"XLA device already configured: {device}")
            except Exception:
                # If get_device fails, continue without setting device
                pass  

    def __exit__(self, type, value, traceback):
        # Propagate exceptions if any
        return False


@PublicAPI(stability="stable")
@dataclass
class TorchConfig(BackendConfig):
    """Configuration for torch process group setup.

    See https://pytorch.org/docs/stable/distributed.html for more info.

    Args:
        backend: The backend to use for training.
            See ``torch.distributed.init_process_group`` for more info and
            valid values.
            If set to None, nccl will be used if GPUs are requested, else gloo
            will be used.
        init_method: The initialization method to use. Either "env"
            for environment variable initialization or "tcp" for TCP
            initialization. Defaults to "env".
        timeout_s: Seconds for process group operations to timeout.
        use_tpu: If True, training will be done on TPUs using torch_xla backend.
            Defaults to False. This enables SPMD execution of the training workload.
        xla_spmd_config: Configuration for XLA SPMD execution when using TPUs.
            This includes settings for mesh partitioning, data parallelism, and
            model parallelism strategies.
    """

    backend: Optional[str] = None
    init_method: str = "env"
    timeout_s: int = 1800
    xla_spmd_config: Optional[Dict] = None

    @property
    def backend_cls(self):
        return _TorchBackend

    @property
    def train_func_context(self):
        return TorchConfigContextManager


def _setup_torch_process_group(
    backend: str,
    world_rank: int,
    world_size: int,
    init_method: str,
    timeout_s: int = 1800,
):
    """Connects the distributed PyTorch backend.

    Args:
        backend: The backend (nccl, gloo, etc.) to use for training.
        world_rank: Rank of the current worker.
        world_size: Number of workers participating in the job.
        init_method: URL specifying how to initialize the process group.
        timeout_s: Seconds for process group operations to timeout.
    """
    if world_rank == 0:
        logger.info(
            f"Setting up process group for: {init_method} [rank={world_rank}, "
            f"world_size={world_size}]"
        )
    else:
        logger.debug(
            f"Setting up process group for: {init_method} [rank={world_rank}, "
            f"world_size={world_size}]"
        )
    logger.debug(f"using {backend}")
    logger.info(f"using world_rank={world_rank}, world_size={world_size}, backend={backend}, init_method={init_method}")

     # Hard guard: if *anything* already initialized the default group, bail out.
    if dist.is_initialized():
        logger.info("Process group already initialized. Skipping.")
        return

    if backend == "nccl":
        # See https://github.com/pytorch/pytorch/blob/c263bd43e8e8502d4726643bc6fd046f0130ac0e/torch/distributed/distributed_c10d.py#L803-L823 # noqa: E501
        # We do not use TORCH_NCCL_BLOCKING_WAIT due to performance overhead.
        if Version(torch.__version__) < Version("2.2.0"):
            TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR = "NCCL_ASYNC_ERROR_HANDLING"
            TORCH_NCCL_BLOCKING_WAIT_ENV_VAR = "NCCL_BLOCKING_WAIT"
        else:
            TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR = "TORCH_NCCL_ASYNC_ERROR_HANDLING"
            TORCH_NCCL_BLOCKING_WAIT_ENV_VAR = "TORCH_NCCL_BLOCKING_WAIT"
        if (
            TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR not in os.environ
            and TORCH_NCCL_BLOCKING_WAIT_ENV_VAR not in os.environ
        ):
            logger.debug(
                f"Setting {TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR}=1 to fail if NCCL collective communication operations are timing out. "  # noqa: E501
                f"To override this behavior, you can set {TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR}=0."  # noqa: E501
            )
            os.environ[TORCH_NCCL_ASYNC_ERROR_HANDLING_ENV_VAR] = "1"
    elif backend == "hccl":
        register_custom_torch_dist_backend(backend)
    # elif backend == "xla":
        # # For XLA backend, use the XLA init method
        # import torch_xla.distributed.xla_backend
        # dist.init_process_group(
        #     backend="xla",
        #     init_method="xla://",  # Use XLA's native init method
        #     rank=world_rank,
        #     world_size=world_size,
        #     timeout=timedelta(seconds=timeout_s),
        # )

        # # Sanity logs
        # try:
        #     import torch_xla.runtime as xr
        #     logger.info(
        #         f">>> [XLA PG] dist rank/size=({dist.get_rank()}/{dist.get_world_size()}), "
        #         f"xr world_size={xr.world_size()}, "
        #         f"global_device_count={xr.global_device_count()}, "
        #         f"local_device_count={xr.local_device_count()}, "
        #         f"process_index={xr.process_index()}, "
        #         f"global_ordinal={xr.global_ordinal()}"
        #     )
        # except Exception as e:
        #     logger.warning(f"Failed to log XLA runtime info: {e}")

        # return



    dist.init_process_group(
        backend=backend,
        init_method=init_method,
        rank=world_rank,
        world_size=world_size,
        timeout=timedelta(seconds=timeout_s),
    )


def _shutdown_torch(destroy_process_group=False):
    from ray.air._internal.torch_utils import get_devices

    devices = get_devices()
    if destroy_process_group:
        dist.destroy_process_group()
    if torch.cuda.is_available():
        for device in devices:
            with torch.cuda.device(device):
                torch.cuda.empty_cache()


def _set_torch_distributed_env_vars():
    # Same env vars as in
    # https://pytorch.org/docs/stable/elastic/run.html#environment-variables
    from ray.train.torch import get_device

    context = ray.train.get_context()
    os.environ["LOCAL_RANK"] = str(context.get_local_rank())
    os.environ["RANK"] = str(context.get_world_rank())
    os.environ["LOCAL_WORLD_SIZE"] = str(context.get_local_world_size())
    os.environ["WORLD_SIZE"] = str(context.get_world_size())
    os.environ["NODE_RANK"] = str(context.get_node_rank())

    # Makes sure Hugging Face Accelerate uses the correct device
    device = get_device()
    os.environ["ACCELERATE_TORCH_DEVICE"] = str(device)


class _TorchBackend(Backend):
    share_cuda_visible_devices: bool = True

    def jls_extract_def(self):
        # Use a different port for clarity
        return 

    def on_start(self, worker_group: WorkerGroup, backend_config: TorchConfig):
        if not dist.is_available():
            raise RuntimeError("Distributed torch is not available.")


        if backend_config.backend == "xla":
            # XLA BACKEND INITIALIZATION PATH
            # This path does NOT use MASTER_ADDR/MASTER_PORT.
            # It relies exclusively on PJRT environment variables.

            # 1. Get a coordinator address from Ray's rank 0 worker.
            master_addr, master_port = worker_group.execute_single(0, get_address_and_port)
            pjrt_port = master_port + 123 
            coordinator = f"{master_addr}:{pjrt_port}"
            if backend_config.init_method == "env":
                print(">>> setting master addr port env vars")
                def set_env_vars(addr, port):
                    os.environ["MASTER_ADDR"] = addr
                    os.environ["MASTER_PORT"] = str(port)

                worker_group.execute(set_env_vars, addr=master_addr, port=master_port)
                
                print(">>> set up a gloo backend pg.")
                url = "env://"
                setup_futures = []
                for i in range(len(worker_group)):
                    setup_futures.append(
                        worker_group.execute_single_async(
                            i,
                            _setup_torch_process_group,
                            backend="gloo",
                            world_rank=i,
                            world_size=len(worker_group),
                            init_method=url,
                            timeout_s=backend_config.timeout_s,
                        )
                    )
                ray.get(setup_futures)
                
                print(">>> set up torch distributed env vars")
                worker_group.execute(_set_torch_distributed_env_vars)
    
            # 2. Define the function to set ONLY the necessary PJRT env vars.
            def _xla_worker_bootstrap(coord):
                context = ray.train.get_context()
                os.environ["PJRT_DEVICE"] = "CUDA"
                os.environ["XLA_DISABLE_FUNCTIONALIZATION"] = "1"
                # os.environ["PJRT_COORDINATOR_ADDRESS"] = coord
                # os.environ["PJRT_NUM_PROCESSES"] = str(context.get_world_size())
                # os.environ["PJRT_PROCESS_INDEX"] = str(context.get_world_rank())
                # os.environ['TPU_VISIBLE_CHIPS'] = "0"
                # os.environ['TPU_PROCESS_BOUNDS']="1,1,1"
                logger.info(
                    f"PJRT envs set for worker {context.get_world_rank()}: "
                    f"COORD={coord}, PROCS={context.get_world_size()}, INDEX={context.get_world_rank()}"
                )

                # 2) Enable SPMD & import SPMD APIs.
                import torch_xla.runtime as xr
                xr.use_spmd()  # SPMD must be enabled process-wide.

                import numpy as np
                import torch_xla.distributed.spmd as xs
                import torch_xla.core.xla_model as xm

                # 3) Build/infer mesh on the worker.
                num = xr.global_runtime_device_count()
                procs = max(1, xr.process_count())
                local = max(1, xr.addressable_runtime_device_count())
                if procs * local == num:
                    shape, axes = (procs, local), ("data", "model")
                else:
                    shape, axes = (num,), ("data",)   # fallback: 1D data mesh
                mesh = xs.Mesh(np.arange(num), shape, axes)

                logger.info(
                    f"PJRT envs set for worker {context.get_world_rank()}: "
                    f"COORD={coord}, PROCS={context.get_world_size()}, INDEX={context.get_world_rank()}, global runtime device = {xr.global_runtime_device_count()}"
                )
                
                # Store the mesh in the train context so it can be accessed by users
                # Use V2 context if available, otherwise fall back to V1
                try:
                    from ray.train.v2._internal.execution.context import get_train_context
                    context = get_train_context()
                    context.set_xla_mesh(mesh)
                except (ImportError, RuntimeError):
                    # Fall back to V1 context or skip if not available
                    logger.warning("Could not access Train V2 context for mesh storage")
                    pass


            # 3. Execute the setup function on all workers.
            worker_group.execute(_xla_worker_bootstrap, coord=coordinator)

        else:
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
                url = f"tcp://{build_address(master_addr, master_port)}"
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
                        _setup_torch_process_group,
                        backend=backend,
                        world_rank=i,
                        world_size=len(worker_group),
                        init_method=url,
                        timeout_s=backend_config.timeout_s,
                    )
                )
            ray.get(setup_futures)


    def on_shutdown(self, worker_group: WorkerGroup, backend_config: TorchConfig):
        worker_group.execute(
            _shutdown_torch,
            destroy_process_group=len(worker_group) > 1,
        )

    def on_training_start(
        self, worker_group: WorkerGroup, backend_config: BackendConfig
    ):
        if backend_config.backend == "xla":
            return
        worker_group.execute(_set_torch_distributed_env_vars)
