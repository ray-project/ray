import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

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
    use_tpu: bool = False
    # xla_spmd_config: Optional[Dict[str, Any]] = None

    @property
    def backend_cls(self):
        # if self.backend == "xla_tpu":
        #     return _TorchTPUBackend
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
    logger.info(f"using world_rank={world_rank}, world_size={world_size}, backend={backend}")

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
    elif backend == "xla":
        import torch_xla.distributed.xla_backend
        # import torch_xla.runtime as xr
        # assert xr.using_spmd(), "XLA_USE_SPMD must be 1 before PG init"
        # General backend setup for other backends (gloo, nccl, etc.)
        if dist.is_initialized():
            logger.warning("Process group already initialized. Skipping.")
            return
        print(">>> os.environ[\"XLA_USE_SPMD\"]", os.environ["XLA_USE_SPMD"])
        dist.init_process_group(
            backend='xla',
            init_method='xla://',
            rank=world_rank,
            world_size=world_size,
            timeout=timedelta(seconds=timeout_s),
        )
        # Sanity logs
        try:
            import torch_xla.runtime as xr
            logger.info(
                f">>> [XLA PG] dist rank/size=({dist.get_rank()}/{dist.get_world_size()}), "
                f"xr world_size={xr.world_size()}, "
                f"global_device_count={xr.global_device_count()}, "
                f"local_device_count={xr.local_device_count()}"
            )
        except Exception:
            pass
        return



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
            # if backend_config.backend == 'xla':
            #     import torch_xla.distributed.xla_backend
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

            if backend_config.backend == "xla":
                # set pjrt envs
                coordinator = f"{master_addr}:{master_port}"
                def _set_pjrt_envs(coord):
                    context = ray.train.get_context()
                    # Core PJRT settings
                    os.environ["PJRT_DEVICE"] = "CUDA"
                    # os.environ["PJRT_DIST_SERVICE_ADDR"] = coord


                    # PJRT multiprocess rendezvous (new-style names)
                    os.environ["PJRT_COORDINATOR_ADDRESS"] = coord
                    os.environ["PJRT_NUM_PROCESSES"] = str(context.get_world_size())
                    os.environ["PJRT_PROCESS_INDEX"] = str(context.get_world_rank())

                    # once again, turn on spmd
                    os.environ["XLA_USE_SPMD"] = "1"
                    
                    # CRITICAL: Use Ray's world size and rank for consistency
                    # os.environ["PJRT_WORLD_SIZE"] = str(context.get_world_size())
                    # os.environ["PJRT_LOCAL_PROCESS_RANK"] = str(context.get_world_rank())
                    
                    # For multi-node setups
                    # os.environ["PJRT_LOCAL_PROCESS_COUNT"] = str(context.get_local_world_size())
                    os.environ["PJRT_NODE_ID"] = str(context.get_node_rank())
                    
                    # Additional XLA settings for GPU
                    os.environ["GPU_NUM_DEVICES"] = "1"  # One GPU per Ray worker
                    
                    logger.info(f"PJRT env vars set for worker {context.get_world_rank()}: "
                                f"WORLD_SIZE={context.get_world_size()}, "
                                f"RANK={context.get_world_rank()}, "
                                f"COORDINATOR={coord}")

                worker_group.execute(_set_pjrt_envs, coord=coordinator)
                logger.info(f"PJRT environment configured with coordinator: {coordinator}")

                # Wait a moment for env vars to propagate
                import time
                time.sleep(1)

            # it might be related since we will need these env var before pjrt init and xla process group init
            worker_group.execute(_set_torch_distributed_env_vars)
            logger.info(f"set torch distributed envs for c10d")
            
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
        else:
            raise RuntimeError("Distributed torch is not available.")

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: TorchConfig):
        worker_group.execute(
            _shutdown_torch,
            destroy_process_group=len(worker_group) > 1,
        )

    def on_training_start(
        self, worker_group: WorkerGroup, backend_config: BackendConfig
    ):
        if backend_config.backend == 'xla':
            return 
        worker_group.execute(_set_torch_distributed_env_vars)


def _setup_xla_torch_process_group(world_rank: int, world_size: int):
    """
    Initialize PyTorch distributed process group with XLA backend for TPU/GPU training.

    This function calls dist.init_process_group("xla", init_method='xla://') to set up
    distributed training on TPU/GPU devices using the torch_xla backend.

    Args:
        world_rank: The rank of this worker in the distributed training setup
        world_size: The total number of workers in the distributed training setup
    """
    import torch.distributed as dist

    # Use the provided rank and world_size parameters
    rank = world_rank
    world_size = world_size

    # Log the rank and world size for debugging
    logger.info(
        f"Setting up XLA process group - rank: {rank}, world_size: {world_size}"
    )

    # Initialize the XLA distributed process group
    dist.init_process_group(
        backend="xla", init_method="env://", world_size=world_size, rank=rank
    )

    logger.info(
        f"Initialized XLA distributed process group: rank={rank}, world_size={world_size}"
    )


def _setup_xla_spmd_environment(spmd_config):
    """Set up XLA SPMD environment for distributed TPU training.

    This function configures the environment for XLA's Single Program Multiple Data
    execution, which is essential for efficient TPU training with data parallelism.
    """
    try:
        import torch_xla.core.xla_model as xm
        import torch_xla.runtime as xr

        # Set XLA SPMD environment variables
        os.environ["XLA_USE_SPMD"] = "1"
        os.environ["XLA_SPMD_PARTITIONING_MODE"] = "auto"

        # Configure mesh partitioning if specified
        if "mesh_shape" in spmd_config:
            mesh_shape = spmd_config["mesh_shape"]
            os.environ["XLA_MESH_SHAPE"] = ",".join(map(str, mesh_shape))
            logger.info(f"Set XLA mesh shape to {mesh_shape}")

        # Configure data parallelism
        if "data_parallel_size" in spmd_config:
            data_parallel_size = spmd_config["data_parallel_size"]
            os.environ["XLA_DATA_PARALLEL_SIZE"] = str(data_parallel_size)
            logger.info(f">>> Set XLA data parallel size to {data_parallel_size}")

        # Configure model parallelism
        if "model_parallel_size" in spmd_config:
            model_parallel_size = spmd_config["model_parallel_size"]
            os.environ["XLA_MODEL_PARALLEL_SIZE"] = str(model_parallel_size)
            logger.info(f"Set XLA model parallel size to {model_parallel_size}")

        # Set TPU-specific SPMD optimizations
        os.environ["XLA_TPU_SPMD_REDUCE_OPTIMIZATION"] = "1"
        os.environ["XLA_TPU_SPMD_OPTIMIZATION"] = "1"

        # Enable XLA graph optimization for SPMD
        os.environ["XLA_OPTIMIZATION_LEVEL"] = "2"

        logger.info("XLA SPMD environment configured for TPU training")

    except ImportError:
        logger.warning("torch_xla not available, skipping XLA SPMD configuration")
    except Exception as e:
        logger.warning(f"Failed to configure XLA SPMD environment: {e}")


def _shutdown_tpu_torch(destroy_process_group=False):
    """Clean up TPU/GPU resources."""
    if destroy_process_group:
        try:
            import torch.distributed as dist

            if dist.is_initialized():
                dist.destroy_process_group()
                logger.info("Successfully destroyed XLA process group")
        except Exception as e:
            logger.warning(f"Failed to destroy XLA process group: {e}")

    # Additional XLA-specific cleanup can be added here if needed
    try:
        import torch_xla.core.xla_model as xm

        # Force synchronization to ensure all pending operations complete
        xm.mark_step()
    except ImportError:
        pass  # torch_xla not available
    except Exception as e:
        logger.warning(f"Failed to synchronize XLA operations during shutdown: {e}")
