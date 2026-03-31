import logging
import threading
import time
from abc import abstractmethod
from typing import Any, Dict, Optional

import ray
import ray.train
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.data import Dataset
from ray.data.collate_fn import CollateFn

from constants import DatasetKey
from config import BenchmarkConfig, RayDataConfig
from dataloader_factory import BaseDataLoaderFactory

logger = logging.getLogger(__name__)

SPILL_MONITOR_ACTOR_NAME = "spill_metrics_monitor"
SPILL_MONITOR_ACTOR_NAMESPACE = "_spill_metrics_monitor"


@ray.remote(num_cpus=0)
class SpillMetricsMonitor:
    """Actor that periodically polls object store spill metrics
    to compute peak and average spilling rates (GB/min).

    GB/min is used instead of GB/s because object store spilling rates are
    typically small fractions of a GB per second, making GB/s values hard to
    read and compare (e.g. 0.0021 GB/s vs 0.13 GB/min). GB/min produces
    more human-friendly numbers while still matching the 60-second poll
    interval naturally.

    A single instance is shared across all workers via a named actor.
    """

    def __init__(self, poll_interval_s: float = 60.0):
        self._poll_interval_s = poll_interval_s
        self._count = 0
        self._sum_gb_min = 0.0
        self._max_gb_min = 0.0
        self._lock = threading.Lock()

        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def _get_spilled_bytes(self) -> int:
        memory_info = get_memory_info_reply(
            get_state_from_address(ray.get_runtime_context().gcs_address)
        )
        return memory_info.store_stats.spilled_bytes_total

    def _poll_loop(self) -> None:
        prev_spilled_bytes: Optional[int] = None
        prev_time: Optional[float] = None

        while True:
            time.sleep(self._poll_interval_s)
            try:
                current_bytes = self._get_spilled_bytes()
                current_time = time.monotonic()

                if prev_spilled_bytes is not None and prev_time is not None:
                    delta_bytes = current_bytes - prev_spilled_bytes
                    delta_time = current_time - prev_time

                    if delta_time > 0 and delta_bytes >= 0:
                        rate_gb_min = (delta_bytes / (1024**3)) / delta_time * 60
                        with self._lock:
                            self._count += 1
                            self._sum_gb_min += rate_gb_min
                            self._max_gb_min = max(self._max_gb_min, rate_gb_min)

                prev_spilled_bytes = current_bytes
                prev_time = current_time
            except Exception as e:
                logger.warning(f"SpillMetricsMonitor: poll failed: {e}")

    def get_metrics(self) -> Dict[str, float]:
        with self._lock:
            count = self._count
            sum_gb_min = self._sum_gb_min
            max_gb_min = self._max_gb_min

        if count == 0:
            return {}

        return {
            "object_store_spilling_peak_gb_min": round(max_gb_min, 4),
            "object_store_spilling_avg_gb_min": round(sum_gb_min / count, 4),
        }


def get_or_create_spill_metrics_monitor(
    poll_interval_s: float = 60.0,
) -> ray.actor.ActorHandle:
    return SpillMetricsMonitor.options(
        name=SPILL_MONITOR_ACTOR_NAME,
        namespace=SPILL_MONITOR_ACTOR_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
    ).remote(poll_interval_s=poll_interval_s)


class ShuffleQualityTracker:
    """Tracks shuffle quality metrics by observing file source diversity in batches."""

    def __init__(self):
        self._unique_files_per_batch: list = []
        self._all_file_ids: list = []
        self._file_id_map: Dict[str, int] = {}

    def _path_to_id(self, path: str) -> int:
        if path not in self._file_id_map:
            self._file_id_map[path] = len(self._file_id_map)
        return self._file_id_map[path]

    def observe(self, batch) -> None:
        """Record shuffle quality stats from a batch containing a 'path' column."""
        if "path" not in batch.column_names:
            return
        paths = batch.column("path").to_pylist()
        self._unique_files_per_batch.append(len(set(paths)))
        self._all_file_ids.extend(self._path_to_id(p) for p in paths)

    def get_metrics(self) -> Dict[str, float]:
        """Return shuffle quality metrics accumulated across batches."""
        if not self._unique_files_per_batch:
            return {}
        total = sum(self._unique_files_per_batch)
        count = len(self._unique_files_per_batch)
        metrics = {
            "shuffle_quality/mean_unique_files_per_batch": total / count,
            "shuffle_quality/min_unique_files_per_batch": min(
                self._unique_files_per_batch
            ),
            "shuffle_quality/max_unique_files_per_batch": max(
                self._unique_files_per_batch
            ),
            "shuffle_quality/num_batches_measured": count,
        }

        # Compute unique files in sliding windows of ~2000 rows
        window_size = 2000
        ids = self._all_file_ids
        if len(ids) >= window_size:
            window_counts = []
            for start in range(0, len(ids) - window_size + 1, window_size):
                window = ids[start : start + window_size]
                window_counts.append(len(set(window)))
            metrics["shuffle_quality/mean_unique_files_per_2k_rows"] = sum(
                window_counts
            ) / len(window_counts)
            metrics["shuffle_quality/min_unique_files_per_2k_rows"] = min(window_counts)
            metrics["shuffle_quality/max_unique_files_per_2k_rows"] = max(window_counts)

        return metrics


class RayDataLoaderFactory(BaseDataLoaderFactory):
    def __init__(self, benchmark_config: BenchmarkConfig) -> None:
        super().__init__(benchmark_config)
        self._ray_ds_iterators = {}
        self._spill_monitor: Optional[ray.actor.ActorHandle] = None
        self._shuffle_tracker: Optional[ShuffleQualityTracker] = None

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
        # Get or create the shared spill monitor actor on first call.
        if self._spill_monitor is None:
            self._spill_monitor = get_or_create_spill_metrics_monitor()

        ds_iterator = ray.train.get_dataset_shard(DatasetKey.TRAIN)
        self._ray_ds_iterators[DatasetKey.TRAIN] = ds_iterator

        dataloader_config = self.get_dataloader_config()

        if dataloader_config.track_shuffle_quality:
            return self._train_dataloader_with_tracking()

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

    def _train_dataloader_with_tracking(self):
        """Get training dataloader with shuffle quality tracking.

        Iterates raw Arrow batches to observe the 'path' column for
        shuffle quality metrics before passing batches through the
        collate function. Subclasses can call this from
        get_train_dataloader() to opt in.
        """
        if self._spill_monitor is None:
            self._spill_monitor = get_or_create_spill_metrics_monitor()

        ds_iterator = ray.train.get_dataset_shard(DatasetKey.TRAIN)
        self._ray_ds_iterators[DatasetKey.TRAIN] = ds_iterator

        dataloader_config = self.get_dataloader_config()
        collate_fn = self._get_collate_fn()
        self._shuffle_tracker = ShuffleQualityTracker()

        arrow_batches = ds_iterator.iter_batches(
            batch_size=dataloader_config.train_batch_size,
            batch_format="pyarrow",
            local_shuffle_buffer_size=(
                dataloader_config.local_buffer_shuffle_size
                if dataloader_config.local_buffer_shuffle_size > 0
                else None
            ),
            prefetch_batches=dataloader_config.ray_data_prefetch_batches,
            drop_last=True,
        )

        def _tracking_iter():
            for batch in arrow_batches:
                self._shuffle_tracker.observe(batch)
                yield collate_fn(batch)

        return _tracking_iter()

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

        # Collect object store spill metrics.
        try:
            memory_info = get_memory_info_reply(
                get_state_from_address(ray.get_runtime_context().gcs_address)
            )
            spilled_bytes_total = memory_info.store_stats.spilled_bytes_total
            metrics["object_store_spilled_total_gb"] = round(
                spilled_bytes_total / (1024**3), 4
            )
        except Exception as e:
            logger.warning(
                f"Failed to collect object_store_spilled_total_gb metric: {e}"
            )

        # Collect peak and average spilling rate from the background monitor.
        if self._spill_monitor is not None:
            try:
                spill_metrics = ray.get(self._spill_monitor.get_metrics.remote())
                metrics.update(spill_metrics)
            except Exception as e:
                logger.warning(f"Failed to collect spill rate metrics: {e}")

        if self._shuffle_tracker is not None:
            metrics.update(self._shuffle_tracker.get_metrics())

        return metrics
