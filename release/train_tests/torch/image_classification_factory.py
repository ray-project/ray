import torch

import ray.train

from config import DataloaderType
from factory import BenchmarkFactory
from imagenet import get_preprocess_map_fn, IMAGENET_PARQUET_SPLIT_S3_DIRS


def mock_dataloader(num_batches: int = 64, batch_size: int = 32):
    device = ray.train.torch.get_device()

    images = torch.randn(batch_size, 3, 224, 224).to(device)
    labels = torch.randint(0, 1000, (batch_size,)).to(device)

    for _ in range(num_batches):
        yield images, labels


def collate_fn(batch):
    from ray.air._internal.torch_utils import (
        convert_ndarray_batch_to_torch_tensor_batch,
    )

    device = ray.train.torch.get_device()
    batch = convert_ndarray_batch_to_torch_tensor_batch(batch, device=device)

    return batch["image"], batch["label"]

class ImageClassificationFactory(BenchmarkFactory):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Populated if Ray Data is being used.
        self._ray_ds_iterators = {}

    def get_model(self):
        if self.benchmark_config.model_name == "resnet50":
            from torchvision.models import resnet50

            return resnet50(weights=None)
        else:
            raise ValueError(f"Model {self.benchmark_config.model_name} not supported")

    def get_train_dataloader(self):
        batch_size = self.benchmark_config.train_batch_size
        if self.benchmark_config.dataloader_type == DataloaderType.RAY_DATA:
            ds_iterator = self._ray_ds_iterators["train"] = ray.train.get_dataset_shard("train")
            return iter(ds_iterator.iter_torch_batches(
                batch_size=batch_size,
                local_shuffle_buffer_size=batch_size * 8,
                collate_fn=collate_fn,
            ))
        elif self.benchmark_config.dataloader_type == DataloaderType.MOCK:
            return mock_dataloader(num_batches=1024, batch_size=batch_size)
        else:
            raise ValueError(
                f"Dataloader type {self.benchmark_config.dataloader_type} not supported"
            )

    def get_val_dataloader(self):
        batch_size = self.benchmark_config.validation_batch_size
        if self.benchmark_config.dataloader_type == DataloaderType.RAY_DATA:
            ds_iterator = self._ray_ds_iterators["val"] = ray.train.get_dataset_shard("val")
            return iter(ds_iterator.iter_torch_batches(batch_size=batch_size, collate_fn=collate_fn))
        elif self.benchmark_config.dataloader_type == DataloaderType.MOCK:
            return mock_dataloader(num_batches=512, batch_size=batch_size)
        else:
            raise ValueError(
                f"Dataloader type {self.benchmark_config.dataloader_type} not supported"
            )

    def get_ray_datasets(self):
        if self.benchmark_config.dataloader_type != DataloaderType.RAY_DATA:
            return {}

        train_ds = ray.data.read_parquet(
            IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
        ).map(get_preprocess_map_fn(decode_image=True, random_transforms=True))

        # TODO: The validation parquet files do not have labels,
        # so just use a subset of the train dataset for now.
        val_ds = (
            ray.data.read_parquet(
                IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
            )
            .limit(50000)
            .map(get_preprocess_map_fn(decode_image=True, random_transforms=False))
        )

        return {"train": train_ds, "val": val_ds}

    def get_loss_fn(self):
        return torch.nn.CrossEntropyLoss()

    def get_dataloader_metrics(self):
        if self.benchmark_config.dataloader_type != DataloaderType.RAY_DATA:
            return {}

        def get_ds_iterator_stats_summary(ds_iterator):
            stats = ray.get(ds_iterator._coord_actor.stats.remote())
            summary = stats.to_summary()
            summary.iter_stats = ds_iterator._iter_stats.to_summary().iter_stats
            summary.iter_stats.streaming_split_coord_time.add(
                stats.streaming_split_coordinator_s.get()
            )
            return summary

        out = {}
        for ds_key, ds_iterator in self._ray_ds_iterators.items():
            stats_summary = get_ds_iterator_stats_summary(ds_iterator)

            if not stats_summary.parents:
                continue

            # The split() operator has no metrics, so pull the stats
            # from the final dataset stage.
            ds_output_summary = stats_summary.parents[0]
            ds_throughput = (
                ds_output_summary.operators_stats[-1].output_num_rows["sum"]
                / ds_output_summary.get_total_wall_time()
            )
            
            iter_stats = stats_summary.iter_stats

            # TODO: Make this raw data dict easier to access from the iterator.
            # The only way to access this through public API is a string representation.
            out[f"dataloader/{ds_key}"] = {
                # Data pipeline throughput:
                # This is the raw throughput of the data producer before being split
                # and fed to the training consumers.
                "producer_throughput": ds_throughput,

                # Training worker (consumer) iterator stats.
                "iter_stats": {
                    # Prefetch blocks to the training worker.
                    "prefetch_block-avg": iter_stats.wait_time.avg(),
                    "prefetch_block-min": iter_stats.wait_time.min(),
                    "prefetch_block-max": iter_stats.wait_time.max(),
                    "prefetch_block-total": iter_stats.wait_time.get(),

                    # Actually fetch the block to the training worker.
                    "fetch_block-avg": iter_stats.get_time.avg(),
                    "fetch_block-min": iter_stats.get_time.min(),
                    "fetch_block-max": iter_stats.get_time.max(),
                    "fetch_block-total": iter_stats.get_time.get(),

                    # Convert a block to a batch by taking a view of the block.
                    # (This may also do some operations to combine chunks of data
                    # to make a contiguous chunk memory.)
                    "block_to_batch-avg": iter_stats.next_time.avg(),
                    "block_to_batch-min": iter_stats.next_time.min(),
                    "block_to_batch-max": iter_stats.next_time.max(),
                    "block_to_batch-total": iter_stats.next_time.get(),

                    # Convert the block to the user-specified batch format (ex: numpy/pandas).
                    "format_batch-avg": iter_stats.format_time.avg(),
                    "format_batch-min": iter_stats.format_time.min(),
                    "format_batch-max": iter_stats.format_time.max(),
                    "format_batch-total": iter_stats.format_time.get(),

                    # UDF or default collate function converting numpy array -> torch.Tensor.
                    "collate-avg": iter_stats.collate_time.avg(),
                    "collate-min": iter_stats.collate_time.min(),
                    "collate-max": iter_stats.collate_time.max(),
                    "collate-total": iter_stats.collate_time.get(),

                    # Default finalize function moves torch.Tensor to the assigned train worker GPU.
                    "finalize-avg": iter_stats.finalize_batch_time.avg(),
                    "finalize-min": iter_stats.finalize_batch_time.min(),
                    "finalize-max": iter_stats.finalize_batch_time.max(),
                    "finalize-total": iter_stats.finalize_batch_time.get(),

                    # Training blocked time
                    "time_spent_blocked-avg": iter_stats.block_time.avg(),
                    "time_spent_blocked-min": iter_stats.block_time.min(),
                    "time_spent_blocked-max": iter_stats.block_time.max(),
                    "time_spent_blocked-total": iter_stats.block_time.get(),

                    # Training time
                    "time_spent_training-avg": iter_stats.user_time.avg(),
                    "time_spent_training-min": iter_stats.user_time.min(),
                    "time_spent_training-max": iter_stats.user_time.max(),
                    "time_spent_training-total": iter_stats.user_time.get(),        
                }
            }

        return out

