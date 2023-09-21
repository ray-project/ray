
import os
import uuid
from dataclasses import dataclass

import ray
from ray._private.ray_constants import NEURON_CORES
from ray.train._internal.utils import get_address_and_port
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend
from ray.train.torch import TorchConfig
from ray.util import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class TorchXLAConfig(TorchConfig):
    """
    Configuration for torch XLA setup.
    See https://pytorch.org/xla/release/1.13/index.html for more info.

    Args:
        runtime: Runtime to use for training. Supported values are "xrt", "pjrt".
            Currently, only "xrt" is supported.
        accelerator_type: The accelerator type used to differentiate the XLA backend.
            Currently, only "neuron_cores" is supported.

    """

    runtime: str = "xrt"
    accelerator_type: str = NEURON_CORES

    @property
    def backend_cls(self):
        if self.accelerator_type == NEURON_CORES:
            return _TorchAwsNeuronXLABackend
        else:
            raise NotImplementedError


def _kill_xrt_server():
    import subprocess

    subprocess.call(["pkill", "-f", "xrt_run_server"])


def _set_xla_env_vars():
    # https://pytorch.org/docs/1.13/elastic/run.html#environment-variables
    context = ray.train.get_context()

    os.environ["LOCAL_RANK"] = str(context.get_local_rank())
    os.environ["RANK"] = str(context.get_world_rank())
    os.environ["LOCAL_WORLD_SIZE"] = str(context.get_local_world_size())
    os.environ["WORLD_SIZE"] = str(context.get_world_size())
    os.environ["GROUP_RANK"] = str(context.get_node_rank())
    os.environ["GROUP_WORLD_SIZE"] = str(
        context.get_world_size() / context.get_local_world_size()
    )
    os.environ["ROLE_RANK"] = str(context.get_world_rank())
    os.environ["ROLE_WORLD_RANK"] = str(context.get_world_rank())
    os.environ["ROLE_WORLD_SIZE"] = str(context.get_world_size())

    # EFA and XLA setup
    # https://github.com/aws/libfabric/blob/master/prov/efa/src/rxr/rxr_init.c
    # https://github.com/aws-neuron/aws-neuron-samples/blob/master/torch-neuronx/training/dp_bert_hf_pretrain/run_dp_bert_large_hf_pretrain_bf16_s128.sh # noqa
    os.environ["FI_PROVIDER"] = "efa"
    os.environ["FI_EFA_USE_DEVICE_RDMA"] = "1"
    os.environ["FI_EFA_FORK_SAFE"] = "1"
    os.environ["XLA_TRANSFER_SEED_ASYNC"] = "1"
    os.environ["NCCL_ASYNC_ERROR_HANDLING"] = "1"


def _setup_xla_torch_process_group():
    try:
        import torch_xla.core.xla_model as xm  # noqa F401
        import torch_xla.distributed.xla_backend  # noqa F401
        import torch.distributed as dist

        dist.init_process_group("xla")
    except ImportError:
        raise ImportError("torch_xla must be installed to use torch_xla backend.")


class _TorchAwsNeuronXLABackend(Backend):
    unique_run_id: str = str(uuid.uuid4())

    def on_start(self, worker_group: WorkerGroup, backend_config: TorchXLAConfig):
        """Logic ran right before training is started."""

        if backend_config.runtime != "xrt":
            # pjrt is not yet supported in torch-neuronx.
            raise ValueError(
                f"Expected runtime to be 'xrt', but got '{backend_config.runtime}'."
            )

        # On previous worker failure, we don't run graceful shutdown on workers.
        # This would leak any running xrt server.
        worker_group.execute(_kill_xrt_server)

        # Get master address and port from the first worker.
        master_addr, master_port = worker_group.execute_single(0, get_address_and_port)

        def set_env_vars(addr, port):
            os.environ["MASTER_ADDR"] = addr
            os.environ["MASTER_PORT"] = str(port)
            # To trigger the xrt server
            os.environ["TORCHELASTIC_RUN_ID"] = self.unique_run_id

        # Set the env vars on all workers.
        worker_group.execute(set_env_vars, addr=master_addr, port=master_port)

    def on_training_start(
        self, worker_group: WorkerGroup, backend_config: TorchXLAConfig
    ):
        """
        Configure the environment variables for the worker group.
        And initialize the xla distributed process group.
        """
        worker_group.execute(_set_xla_env_vars)
        worker_group.execute(_setup_xla_torch_process_group)

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: TorchXLAConfig):
        """
        Logic ran right after training is finished.
        This is a sanity cleanup to kill xrt server.
        """
        worker_group.execute(_kill_xrt_server)
