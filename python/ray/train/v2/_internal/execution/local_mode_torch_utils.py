import logging
import os
from typing import Callable

import torch
import torch.distributed as dist

from ray.train import Result
from ray.train.v2._internal.execution.local_mode_utils import LocalController
from ray.train.v2._internal.execution.train_fn_utils import (
    LocalTrainFnUtils,
    get_train_fn_utils,
    set_train_fn_utils,
)

logger = logging.getLogger(__name__)


def is_torch_dist_env_set() -> bool:
    """Return True if this process has torch.distributed env vars set.

    For torch.distributed.init_process_group with init_method="env://", these variables are required:
    - RANK: The rank of the current process
    - LOCAL_RANK: The local rank of the current process
    - WORLD_SIZE: Total number of processes participating in the job
    - LOCAL_WORLD_SIZE: Total number of processes participating in the job on the current node
    - MASTER_ADDR: The IP address or hostname of the master node (rank 0)
    - MASTER_PORT: A free port on the master node for communication

    """
    torch_dist_required_vars = {
        "RANK",
        "LOCAL_RANK",
        "WORLD_SIZE",
        "LOCAL_WORLD_SIZE",
        "MASTER_ADDR",
        "MASTER_PORT",
    }

    return torch_dist_required_vars.issubset(os.environ.keys())


class LocalTorchController(LocalController):
    def run(self, train_func: Callable[[], None]) -> Result:
        world_size = 1
        world_rank = 0
        if is_torch_dist_env_set():
            assert not dist.is_initialized(), "torch.distributed is already initialized"
            torch.distributed.init_process_group(
                backend="nccl" if torch.cuda.is_available() else "gloo"
            )
            world_size = torch.distributed.get_world_size()
            world_rank = torch.distributed.get_rank()
            if torch.cuda.is_available():
                torch.cuda.set_device(world_rank)
            assert os.environ["LOCAL_WORLD_SIZE"] == str(
                world_size
            ), "Local mode only supports 1 node, LOCAL_WORLD_SIZE should be equal to WORLD_SIZE."
        if world_size != 1:
            assert (
                self.datasets is None or len(self.datasets) == 0
            ), "Local mode with multiple workers doesn't support ray data."
        set_train_fn_utils(
            LocalTrainFnUtils(
                experiment_name=self.experiment_name,
                world_size=world_size,
                world_rank=world_rank,
                dataset_shards=self.datasets,
            )
        )
        train_func()
        train_fn_utils = get_train_fn_utils()
        assert isinstance(train_fn_utils, LocalTrainFnUtils)
        result = Result(
            metrics=train_fn_utils._get_last_metrics(),
            checkpoint=train_fn_utils.get_checkpoint(),
            path=None,
            error=None,
        )
        if dist.is_initialized():
            dist.destroy_process_group()
        return result
