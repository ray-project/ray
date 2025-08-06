import torch
import torch.distributed as dist
import asyncio
from ray.train.trainer import GenDataset
from ray.train.v2._internal.execution.local_running_utils import launched_by_torchrun, maybe_start_local_running_data_provider, get_dataset_shard
from ray.data import DataIterator
from typing import Dict, Optional, Callable
from ray.train.v2.api.context import LocalRunningTrainContext
from ray.train.v2.api.result import Result

class LocalTorchTrainer:
    def __init__(self, datasets: Optional[Dict[str, GenDataset]] = None,):
        self.launched_by_torchrun = launched_by_torchrun()
        if self.launched_by_torchrun:
            dist.init_process_group(backend="nccl" if torch.cuda.is_available() else "gloo")
            self.local_world_size = dist.get_world_size()
            self.local_rank = dist.get_rank()
            torch.cuda.set_device(self.local_rank)
        else:
            self.local_world_size = 1
            self.local_rank = 0

        self.datasets = datasets
        self.dataset_shards = asyncio.run(self._set_local_running_train_context_and_data_shard())
        self.context = LocalRunningTrainContext(
            experiment_name="local-training",
            local_world_size=self.local_world_size,
            local_rank=self.local_rank,
            dataset_shards=self.dataset_shards,
        )

    async def _set_local_running_train_context_and_data_shard(self) -> Dict[str, DataIterator]:
        if self.datasets is None or len(self.datasets) == 0:
            return {}
        datasets = {k: v() if callable(v) else v for k, v in self.datasets.items()}
        self.data_provider_actor = await maybe_start_local_running_data_provider(
            world_size=self.local_world_size, dataset=datasets, local_rank=self.local_rank
        )
        return await get_dataset_shard(self.data_provider_actor, self.local_rank)

    def fit(self, train_func: Callable[[], None]) -> Result:
        train_func()
        if self.launched_by_torchrun:
            dist.destroy_process_group()
        return Result(metrics={}, checkpoint=None, path=None, error=None)
