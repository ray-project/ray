from abc import abstractmethod
from typing import Any, Dict, Optional

import ray.train
from ray.data import Dataset
from ray.data.collate_fn import CollateFn

from constants import DatasetKey
from config import BenchmarkConfig, RayDataConfig
from dataloader_factory import BaseDataLoaderFactory


class RayDataLoaderFactory(BaseDataLoaderFactory):
    def __init__(self, benchmark_config: BenchmarkConfig) -> None:
        super().__init__(benchmark_config)
        self._ray_ds_iterators = {}

        dataloader_config = self.get_dataloader_config()
        assert isinstance(dataloader_config, RayDataConfig), type(dataloader_config)

        # Configure Ray Data settings.
        data_context = ray.data.DataContext.get_current()
        data_context.enable_operator_progress_bars = (
            dataloader_config.enable_operator_progress_bars
        )
        # Retry ACCESS_DENIED errors that sometimes show up
        # due to throttling during read operations.
        data_context.retried_io_errors.append("AWS Error ACCESS_DENIED")

        data_context.execution_options.locality_with_output = (
            dataloader_config.locality_with_output
        )
        data_context.execution_options.actor_locality_enabled = (
            dataloader_config.actor_locality_enabled
        )
        data_context.execution_options.preserve_order = dataloader_config.preserve_order

    @abstractmethod
    def get_ray_datasets(self) -> Dict[str, Dataset]:
        """Get Ray datasets."""
        raise NotImplementedError

    def _get_collate_fn(self) -> Optional[CollateFn]:
        """Return the collate function for the dataloader."""
        return None

    def get_ray_data_config(self) -> ray.train.DataConfig:
        return ray.train.DataConfig(
            enable_shard_locality=self.get_dataloader_config().enable_shard_locality,
        )

    def get_train_dataloader(self):
        """Get the training dataloader.

        Returns:
            Iterator of training batches
        """
        ds_iterator = ray.train.get_dataset_shard(DatasetKey.TRAIN)
        self._ray_ds_iterators[DatasetKey.TRAIN] = ds_iterator

        dataloader_config = self.get_dataloader_config()
        return iter(
            ds_iterator.iter_torch_batches(
                batch_size=dataloader_config.train_batch_size,
                local_shuffle_buffer_size=(
                    dataloader_config.local_buffer_shuffle_size
                    if dataloader_config.local_buffer_shuffle_size > 0
                    else None
                ),
                collate_fn=self._get_collate_fn(),
                prefetch_batches=dataloader_config.ray_data_prefetch_batches,
                drop_last=True,
                pin_memory=dataloader_config.ray_data_pin_memory,
            )
        )

    def get_val_dataloader(self):
        """Get the validation dataloader.

        Returns:
            Iterator of validation batches
        """
        ds_iterator = ray.train.get_dataset_shard(DatasetKey.VALID)
        self._ray_ds_iterators[DatasetKey.VALID] = ds_iterator

        dataloader_config = self.get_dataloader_config()
        return iter(
            ds_iterator.iter_torch_batches(
                batch_size=dataloader_config.validation_batch_size,
                collate_fn=self._get_collate_fn(),
                prefetch_batches=dataloader_config.ray_data_prefetch_batches,
                drop_last=True,
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
                    "get_ref_bundles-avg": iter_stats.get_ref_bundles_time.avg(),
                    "get_ref_bundles-min": iter_stats.get_ref_bundles_time.min(),
                    "get_ref_bundles-max": iter_stats.get_ref_bundles_time.max(),
                    "get_ref_bundles-total": iter_stats.get_ref_bundles_time.get(),
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
