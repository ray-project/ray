from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Tuple

import torch
import ray.train
from ray.data import Dataset

from config import DataLoaderConfig, RayDataConfig


class BaseDataLoaderFactory(ABC):
    """Base class for creating and managing dataloaders."""

    def __init__(self, config: DataLoaderConfig):
        self.config = config

    @abstractmethod
    def get_train_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        pass

    @abstractmethod
    def get_val_dataloader(self) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        pass

    def get_metrics(self) -> Dict[str, Any]:
        """Return metrics about dataloader performance."""
        return {}

    def get_ray_datasets(self) -> Dict[str, Dataset]:
        """Get Ray datasets if this loader type uses Ray Data."""
        return {}


class RayDataLoaderFactory(BaseDataLoaderFactory):
    def __init__(self, config: RayDataConfig):
        super().__init__(config)
        self._ray_ds_iterators = {}

        assert isinstance(config, RayDataConfig), type(config)

    @abstractmethod
    def get_ray_datasets(self) -> Dict[str, Dataset]:
        """Get the Ray datasets for training and validation.

        Returns:
            Dict with "train" and "val" Dataset objects
        """
        pass

    @abstractmethod
    def collate_fn(self) -> Dict[str, Dataset]:
        """Get the collate function for the dataloader.

        Returns:
            A function that takes a batch and returns a tuple of tensors.
        """
        pass

    def get_train_dataloader(self):
        ds_iterator = self._ray_ds_iterators["train"] = ray.train.get_dataset_shard(
            "train"
        )
        return iter(
            ds_iterator.iter_torch_batches(
                batch_size=self.config.train_batch_size,
                local_shuffle_buffer_size=(
                    self.config.local_buffer_shuffle_size
                    if self.config.local_buffer_shuffle_size > 0
                    else None
                ),
                collate_fn=self.collate_fn,
            )
        )

    def get_val_dataloader(self):
        ds_iterator = self._ray_ds_iterators["val"] = ray.train.get_dataset_shard("val")
        return iter(
            ds_iterator.iter_torch_batches(
                batch_size=self.config.validation_batch_size,
                collate_fn=self.collate_fn,
            )
        )

    def get_metrics(self) -> Dict[str, Any]:
        metrics = {}
        for ds_key, ds_iterator in self._ray_ds_iterators.items():
            stats = ray.get(ds_iterator._coord_actor.stats.remote())
            summary = stats.to_summary()
            summary.iter_stats = ds_iterator._iter_stats.to_summary().iter_stats
            summary.iter_stats.streaming_split_coord_time.add(
                stats.streaming_split_coordinator_s.get()
            )

            if not summary.parents:
                continue

            # The split() operator has no metrics, so pull the stats
            # from the final dataset stage.
            ds_output_summary = summary.parents[0]
            ds_throughput = (
                ds_output_summary.operators_stats[-1].output_num_rows["sum"]
                / ds_output_summary.get_total_wall_time()
            )

            iter_stats = summary.iter_stats

            metrics[f"dataloader/{ds_key}"] = {
                "producer_throughput": ds_throughput,
                "iter_stats": {
                    "prefetch_block-avg": iter_stats.wait_time.avg(),
                    "prefetch_block-min": iter_stats.wait_time.min(),
                    "prefetch_block-max": iter_stats.wait_time.max(),
                    "prefetch_block-total": iter_stats.wait_time.get(),
                    "fetch_block-avg": iter_stats.get_time.avg(),
                    "fetch_block-min": iter_stats.get_time.min(),
                    "fetch_block-max": iter_stats.get_time.max(),
                    "fetch_block-total": iter_stats.get_time.get(),
                    "block_to_batch-avg": iter_stats.next_time.avg(),
                    "block_to_batch-min": iter_stats.next_time.min(),
                    "block_to_batch-max": iter_stats.next_time.max(),
                    "block_to_batch-total": iter_stats.next_time.get(),
                    "format_batch-avg": iter_stats.format_time.avg(),
                    "format_batch-min": iter_stats.format_time.min(),
                    "format_batch-max": iter_stats.format_time.max(),
                    "format_batch-total": iter_stats.format_time.get(),
                    "collate-avg": iter_stats.collate_time.avg(),
                    "collate-min": iter_stats.collate_time.min(),
                    "collate-max": iter_stats.collate_time.max(),
                    "collate-total": iter_stats.collate_time.get(),
                    "finalize-avg": iter_stats.finalize_batch_time.avg(),
                    "finalize-min": iter_stats.finalize_batch_time.min(),
                    "finalize-max": iter_stats.finalize_batch_time.max(),
                    "finalize-total": iter_stats.finalize_batch_time.get(),
                    "time_spent_blocked-avg": iter_stats.block_time.avg(),
                    "time_spent_blocked-min": iter_stats.block_time.min(),
                    "time_spent_blocked-max": iter_stats.block_time.max(),
                    "time_spent_blocked-total": iter_stats.block_time.get(),
                    "time_spent_training-avg": iter_stats.user_time.avg(),
                    "time_spent_training-min": iter_stats.user_time.min(),
                    "time_spent_training-max": iter_stats.user_time.max(),
                    "time_spent_training-total": iter_stats.user_time.get(),
                },
            }
        return metrics
