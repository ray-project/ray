import logging
import os
from typing import Callable

import torch
import torch.distributed as dist

from ray.train import Result
from ray.train.v2._internal.execution.local_mode.utils import LocalController
from ray.train.v2._internal.execution.train_fn_utils import (
    LocalTrainFnUtils,
    get_train_fn_utils,
    set_train_fn_utils,
)

logger = logging.getLogger(__name__)


def has_torchrun_env() -> bool:
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
    def _set_train_fn_utils(self) -> None:
        world_size = 1
        global_rank = 0
        local_rank = 0
        nproc_per_node = 1
        node_rank = 0
        if has_torchrun_env():
            assert not dist.is_initialized(), "torch.distributed is already initialized"
            torch.distributed.init_process_group(
                backend="nccl" if torch.cuda.is_available() else "gloo"
            )
            world_size = torch.distributed.get_world_size()
            global_rank = torch.distributed.get_rank()
            local_rank = int(os.environ["LOCAL_RANK"])
            if torch.cuda.is_available():
                torch.cuda.set_device(local_rank)
            nproc_per_node = int(os.environ.get("LOCAL_WORLD_SIZE"))
            node_rank = global_rank // nproc_per_node

        if world_size != 1:
            assert (
                self.datasets is None or len(self.datasets) == 0
            ), "Ray Data is not supported in local mode with multiple workers."
        set_train_fn_utils(
            LocalTrainFnUtils(
                experiment_name=self.experiment_name,
                world_size=world_size,
                world_rank=global_rank,
                local_rank=local_rank,
                local_world_size=nproc_per_node,
                node_rank=node_rank,
                dataset_shards=self.datasets,
            )
        )

    def run(self, train_func: Callable[[], None]) -> Result:
        self._set_train_fn_utils()
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
