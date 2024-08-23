import logging
import os
import re
import shutil
import uuid
from dataclasses import dataclass

import ray
from ray.train._internal.utils import get_address_and_port
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend
from ray.train.torch import TorchConfig
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
@dataclass
class TorchXLAConfig(TorchConfig):
    """
    Configuration for torch XLA setup.
    See https://pytorch.org/xla/release/1.13/index.html for more info.
    Currently, only "neuron_cores" accelerator (AwsNeuronXLABackend)
    is supported with xrt runtime.
    """

    neuron_parallel_compile: bool = False

    @property
    def backend_cls(self):
        return _TorchAwsNeuronXLABackend


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
        import torch.distributed as dist
        import torch_xla.core.xla_model as xm  # noqa F401
        import torch_xla.distributed.xla_backend  # noqa F401

        dist.init_process_group("xla")
    except ImportError:
        raise ImportError("torch_xla must be installed to use torch_xla backend.")


# The following env vars enable Neuron graph extraction for parallel compilation
#   Note: model outputs are invalid and should be ignored while these env vars are set
def _set_neuron_parallel_compile_env_vars():
    os.environ["NEURON_PARALLEL_COMPILE"] = "1"
    os.environ["NEURON_EXTRACT_GRAPHS_ONLY"] = "1"
    os.environ["NEURON_FALL_BACK_TO_NULL_NEFF"] = "1"


# Compile previously extracted Neuron graphs
def _neuron_compile_extracted_graphs():
    try:
        from libneuronxla.neuron_cc_cache import CacheUrl
        from libneuronxla.neuron_parallel_compile import parallel_compile
    except ImportError:
        raise ImportError(
            "libneuronxla must be installed to use Neuron parallel compilation."
        )

    # Only 1 worker per node should run parallel_compile()
    if os.environ.get("LOCAL_RANK") == "0":
        logger.info("Compiling extracted graphs on local rank0 worker")

        parallel_compile_workdir = (
            f"/tmp/{os.environ.get('USER','no-user')}/parallel_compile_workdir/"
        )
        if os.path.exists(parallel_compile_workdir):
            shutil.rmtree(parallel_compile_workdir)
        os.makedirs(parallel_compile_workdir, exist_ok=True)

        # Users can set the cache directory using --cache_dir in NEURON_CC_FLAGS or by
        # using NEURON_COMPILE_CACHE_URL. --cache_dir takes precedence.
        explicit_cache_dir = None
        if neuron_cc_flags := os.environ.get("NEURON_CC_FLAGS"):
            if s := re.search(r"--cache_dir[= ](\S+)", neuron_cc_flags):
                explicit_cache_dir = s.group(1)

        parallel_compile(
            parallel_compile_workdir,
            CacheUrl.get_cache_url(explicit_cache_dir),
        )


class _TorchAwsNeuronXLABackend(Backend):
    unique_run_id: str = str(uuid.uuid4())

    def on_start(self, worker_group: WorkerGroup, backend_config: TorchXLAConfig):
        """Logic ran right before training is started."""

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

        # Set up env vars for neuron parallel compilation graph extraction
        if backend_config.neuron_parallel_compile:
            logger.info("Extracting graphs for Neuron parallel compilation")
            worker_group.execute(_set_neuron_parallel_compile_env_vars)

    def on_training_start(
        self, worker_group: WorkerGroup, backend_config: TorchXLAConfig
    ):
        """
        Configure the environment variables for the worker group.
        And initialize the xla distributed process group.
        TODO: Current setup only supports homogenous cluster with
         neuron_cores accelerator and xrt runtime.
        """
        worker_group.execute(_set_xla_env_vars)
        worker_group.execute(_setup_xla_torch_process_group)

    def on_shutdown(self, worker_group: WorkerGroup, backend_config: TorchXLAConfig):
        """
        Logic ran right after training is finished.
        This is a sanity cleanup to kill xrt server, and to optionally
        run neuron parallel graph compilation
        """
        worker_group.execute(_kill_xrt_server)

        # Compile the extracted graphs. This must run at end of training.
        if backend_config.neuron_parallel_compile:
            worker_group.execute(_neuron_compile_extracted_graphs)
