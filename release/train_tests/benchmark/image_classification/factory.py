from typing import Dict, Iterator, Tuple, Optional
import logging
import time

import torch
import torchvision
from torch.utils.data import IterableDataset

import ray.train

from config import DataloaderType, BenchmarkConfig
from factory import BenchmarkFactory
from dataloader_factory import (
    BaseDataLoaderFactory,
)
from ray_dataloader_factory import RayDataLoaderFactory
from torch_dataloader_factory import TorchDataLoaderFactory
from image_classification.imagenet import (
    get_preprocess_map_fn,
    IMAGENET_PARQUET_SPLIT_S3_DIRS,
)
from image_classification.torch_parquet_image_iterable_dataset import (
    S3Reader,
    S3ParquetImageIterableDataset,
)

logger = logging.getLogger(__name__)


def mock_dataloader(num_batches: int = 64, batch_size: int = 32):
    device = ray.train.torch.get_device()

    images = torch.randn(batch_size, 3, 224, 224).to(device)
    labels = torch.randint(0, 1000, (batch_size,)).to(device)

    for _ in range(num_batches):
        yield images, labels


class ImageClassificationMockDataLoaderFactory(BaseDataLoaderFactory):
    def get_train_dataloader(self):
        dataloader_config = self.get_dataloader_config()
        return mock_dataloader(
            num_batches=1024, batch_size=dataloader_config.train_batch_size
        )

    def get_val_dataloader(self):
        dataloader_config = self.get_dataloader_config()
        return mock_dataloader(
            num_batches=512, batch_size=dataloader_config.validation_batch_size
        )


class ImageClassificationRayDataLoaderFactory(RayDataLoaderFactory):
    def get_ray_datasets(self) -> Dict[str, ray.data.Dataset]:
        train_ds = (
            ray.data.read_parquet(
                IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
            )
            .limit(self.benchmark_config.limit_training_rows)
            .map(get_preprocess_map_fn(decode_image=True, random_transforms=True))
        )

        val_ds = (
            ray.data.read_parquet(
                IMAGENET_PARQUET_SPLIT_S3_DIRS["train"], columns=["image", "label"]
            )
            .limit(self.benchmark_config.limit_validation_rows)
            .map(get_preprocess_map_fn(decode_image=True, random_transforms=False))
        )

        if self.benchmark_config.validate_every_n_steps > 0:
            # TODO: This runs really slowly and needs to be tuned.
            # Maybe move this to the RayDataLoaderFactory.
            cpus_to_exclude = 16
            train_ds.context.execution_options.exclude_resources = (
                train_ds.context.execution_options.exclude_resources.add(
                    ray.data.ExecutionResources(cpu=cpus_to_exclude)
                )
            )
            val_ds.context.execution_options.resource_limits = (
                ray.data.ExecutionResources(cpu=cpus_to_exclude)
            )
            logger.info(
                f"[Dataloader] Reserving {cpus_to_exclude} CPUs for validation "
                "that happens concurrently with training every "
                f"{self.benchmark_config.validate_every_n_steps} steps. "
            )

        return {"train": train_ds, "val": val_ds}

    def collate_fn(self, batch):
        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        device = ray.train.torch.get_device()
        batch = convert_ndarray_batch_to_torch_tensor_batch(batch, device=device)

        return batch["image"], batch["label"]


class ImageClassificationTorchDataLoaderFactory(TorchDataLoaderFactory, S3Reader):
    """Factory for creating PyTorch DataLoaders for image classification tasks.

    This factory:
    1. Creates DataLoaders that read Parquet files from S3
    2. Distributes files among Ray workers using round-robin allocation
    3. Handles device transfer and error handling for batches
    4. Supports row limits per worker for controlled data processing
    """

    def __init__(self, benchmark_config: BenchmarkConfig):
        super().__init__(benchmark_config)
        S3Reader.__init__(self)  # Initialize S3Reader to set up _s3_client
        self.train_url = IMAGENET_PARQUET_SPLIT_S3_DIRS["train"]
        self.val_url = IMAGENET_PARQUET_SPLIT_S3_DIRS["train"]

    def calculate_rows_per_worker(
        self, total_rows: Optional[int], num_workers: int
    ) -> Optional[int]:
        """Calculate how many rows each worker should process.

        Args:
            total_rows: Total number of rows to process across all workers.
            num_workers: Total number of workers (Ray workers × Torch workers)

        Returns:
            Number of rows each worker should process, or None if no limit.
            If total_rows is less than num_workers, each worker will process at least 1 row.
        """
        if total_rows is None:
            return None

        if num_workers == 0:
            return total_rows

        return max(1, total_rows // num_workers)

    def get_iterable_datasets(self) -> Dict[str, IterableDataset]:
        """Get the train and validation datasets.

        Returns:
            A dictionary containing the train and validation datasets.
        """
        # Calculate row limits per worker for validation
        dataloader_config = self.get_dataloader_config()
        num_workers = max(1, dataloader_config.num_torch_workers)
        total_workers = self.benchmark_config.num_workers * num_workers

        limit_training_rows_per_worker = self.calculate_rows_per_worker(
            self.benchmark_config.limit_training_rows, total_workers
        )

        limit_validation_rows_per_worker = self.calculate_rows_per_worker(
            self.benchmark_config.limit_validation_rows, total_workers
        )

        # Calculate total rows to process
        total_training_rows = (
            self.benchmark_config.limit_training_rows
            if self.benchmark_config.limit_training_rows is not None
            else None
        )
        total_validation_rows = (
            self.benchmark_config.limit_validation_rows
            if self.benchmark_config.limit_validation_rows is not None
            else None
        )

        train_file_urls = self._get_file_urls(self.train_url)
        train_ds = S3ParquetImageIterableDataset(
            file_urls=train_file_urls,
            random_transforms=True,
            limit_rows_per_worker=limit_training_rows_per_worker,
        )

        # Report training dataset configuration
        ray.train.report(
            {
                "train_dataset": {
                    "file_urls": train_file_urls,
                    "random_transforms": True,
                    "limit_rows_per_worker": limit_training_rows_per_worker,
                    "total_rows": total_training_rows,
                    "worker_rank": ray.train.get_context().get_world_rank(),
                }
            }
        )

        val_file_urls = self._get_file_urls(self.val_url)
        val_ds = S3ParquetImageIterableDataset(
            file_urls=val_file_urls,
            random_transforms=False,
            limit_rows_per_worker=limit_validation_rows_per_worker,
        )

        # Report validation dataset configuration
        ray.train.report(
            {
                "validation_dataset": {
                    "file_urls": val_file_urls,
                    "random_transforms": False,
                    "limit_rows_per_worker": limit_validation_rows_per_worker,
                    "total_rows": total_validation_rows,
                    "worker_rank": ray.train.get_context().get_world_rank(),
                }
            }
        )

        return {"train": train_ds, "val": val_ds}

    def create_batch_iterator(
        self, dataloader: torch.utils.data.DataLoader, device: torch.device
    ) -> Iterator[Tuple[torch.Tensor, torch.Tensor]]:
        """Create a safe iterator that handles device transfer and error handling.

        Args:
            dataloader: The PyTorch DataLoader to iterate over
            device: The device to move tensors to

        Returns:
            An iterator that yields batches moved to the specified device
        """
        worker_rank = ray.train.get_context().get_world_rank()
        logger.info(
            f"[ImageClassification] Worker {worker_rank}: Starting batch iteration"
        )

        try:
            last_batch_time = time.time()
            for batch_idx, batch in enumerate(dataloader):
                try:
                    # Check for delays between batches
                    current_time = time.time()
                    time_since_last_batch = current_time - last_batch_time
                    if time_since_last_batch > 10:
                        logger.warning(
                            f"[ImageClassification] Worker {worker_rank}: "
                            f"Long delay ({time_since_last_batch:.2f}s) between batches {batch_idx-1} and {batch_idx}"
                        )

                    # Move batch to device
                    images, labels = batch
                    logger.info(
                        f"[ImageClassification] Worker {worker_rank}: Processing batch {batch_idx} "
                        f"(shape: {images.shape}, time since last: {time_since_last_batch:.2f}s)"
                    )

                    transfer_start = time.time()
                    dataloader_config = self.get_dataloader_config()
                    images = images.to(
                        device, non_blocking=dataloader_config.torch_non_blocking
                    )
                    labels = labels.to(
                        device, non_blocking=dataloader_config.torch_non_blocking
                    )
                    transfer_time = time.time() - transfer_start

                    if transfer_time > 5:
                        logger.warning(
                            f"[ImageClassification] Worker {worker_rank}: "
                            f"Slow device transfer ({transfer_time:.2f}s) for batch {batch_idx}"
                        )

                    logger.info(
                        f"[ImageClassification] Worker {worker_rank}: Completed device transfer "
                        f"for batch {batch_idx} in {transfer_time:.2f}s"
                    )

                    last_batch_time = time.time()
                    yield images, labels

                except Exception as e:
                    logger.error(
                        f"[ImageClassification] Worker {worker_rank}: Error processing batch {batch_idx}: {str(e)}",
                        exc_info=True,
                    )
                    raise

        except Exception as e:
            logger.error(
                f"[ImageClassification] Worker {worker_rank}: Error in batch iterator: {str(e)}",
                exc_info=True,
            )
            raise


class ImageClassificationFactory(BenchmarkFactory):
    def get_dataloader_factory(self) -> BaseDataLoaderFactory:
        data_factory_cls = {
            DataloaderType.MOCK: ImageClassificationMockDataLoaderFactory,
            DataloaderType.RAY_DATA: ImageClassificationRayDataLoaderFactory,
            DataloaderType.TORCH: ImageClassificationTorchDataLoaderFactory,
        }[self.benchmark_config.dataloader_type]

        return data_factory_cls(self.benchmark_config)

    def get_model(self) -> torch.nn.Module:
        return torchvision.models.resnet50(weights=None)

    def get_loss_fn(self) -> torch.nn.Module:
        return torch.nn.CrossEntropyLoss()
