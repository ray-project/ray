"""ImageNet dataset loading via S3 URL download with map_batches.

This module provides dataset loading that:
1. Lists JPEG files from S3 using boto3
2. Creates a Ray dataset from the file records
3. Uses map_batches to download and process images from S3

This approach separates file listing from image downloading, allowing for
efficient parallel downloads during map_batches execution.

NOTE: The current implementation downloads images sequentially within each
map_batches call, which is not optimal for I/O-bound S3 downloads. A potential
optimization would be to use concurrent downloads (e.g., ThreadPoolExecutor),
but this risks spawning too many threads when combined with Ray's parallelism.
For production workloads requiring higher throughput, consider using Ray Data's
native S3 reading capabilities with `ray.data.read_images()` instead.
"""

import logging
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import numpy as np
from PIL import Image
from torchvision.transforms.functional import pil_to_tensor

import ray.data

from constants import DatasetKey
from image_classification.imagenet import (
    get_transform,
    IMAGENET_WNID_TO_ID,
)
from s3_utils import parse_s3_url, create_s3_client, download_s3_object_as_buffer

logger = logging.getLogger(__name__)

# S3 configuration for ImageNet JPEG data
S3_ROOT = "s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC"
IMAGENET_S3_URL_SPLIT_DIRS = {
    DatasetKey.TRAIN: f"{S3_ROOT}/train",
    DatasetKey.VALID: f"{S3_ROOT}/val",
    DatasetKey.TEST: f"{S3_ROOT}/test",
}


def _get_class_labels(bucket: str, prefix: str) -> List[str]:
    """Get all class label directories from S3.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix path

    Returns:
        List of class label directory names
    """
    from typing import Set

    # Ensure prefix ends with /
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    # List directories using delimiter
    s3_client = create_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")

    # Use delimiter to get "directory" level
    labels: Set[str] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        # CommonPrefixes contains the "directories"
        for common_prefix in page.get("CommonPrefixes", []):
            prefix_path = common_prefix["Prefix"]
            # Extract the directory name
            label = prefix_path.rstrip("/").split("/")[-1]
            labels.add(label)

    return sorted(labels)


@ray.remote
def _list_files_for_label(
    bucket: str, prefix: str, label: str
) -> List[Tuple[str, str]]:
    """Ray task to list all image files for a specific label.

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix (parent directory)
        label: Class label (subdirectory name)

    Returns:
        List of tuples with (file_path, class_name)
    """
    s3_client = create_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")

    # Construct the full prefix for this label
    label_prefix = f"{prefix}/{label}/" if prefix else f"{label}/"

    file_records = []
    for page in paginator.paginate(Bucket=bucket, Prefix=label_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith((".jpg", ".jpeg")):
                file_path = f"s3://{bucket}/{key}"
                file_records.append((file_path, label))

    return file_records


@lru_cache(maxsize=8)
def _list_s3_image_files_cached(data_dir: str) -> Tuple[Tuple[str, str], ...]:
    """Cached implementation of S3 file listing using Ray tasks for parallelism.

    Returns a tuple of tuples for hashability (required by lru_cache).
    """
    logger.info(f"Listing JPEG files from {data_dir}...")

    # Parse S3 URL using shared utility
    bucket, prefix = parse_s3_url(data_dir)
    prefix = prefix.rstrip("/")

    # Get all class labels
    labels = _get_class_labels(bucket, prefix)
    logger.info(
        f"Found {len(labels)} class labels, launching Ray tasks for parallel listing..."
    )

    # Launch Ray tasks for each label
    futures = [_list_files_for_label.remote(bucket, prefix, label) for label in labels]

    # Wait for all tasks to complete and aggregate results
    results = ray.get(futures)

    # Flatten the list of lists
    file_records = []
    for records in results:
        file_records.extend(records)

    logger.info(f"Listed and cached {len(file_records)} JPEG files")
    return tuple(file_records)


def list_s3_image_files(data_dir: str) -> List[Dict[str, str]]:
    """List JPEG files from S3 with class labels extracted from path.

    Results are cached to avoid repeated S3 listings.

    Args:
        data_dir: S3 path to list files from (e.g., "s3://bucket/prefix")

    Returns:
        List of dicts with "path" (S3 URL) and "class" (WNID) keys
    """
    cached_records = _list_s3_image_files_cached(data_dir)
    return [{"path": path, "class": cls} for path, cls in cached_records]


def get_download_and_process_batch_fn(
    random_transforms: bool = True,
    label_to_id_map: Optional[Dict[str, int]] = None,
):
    """Get a map_batches function that downloads and processes images from S3.

    Args:
        random_transforms: Whether to use random transforms for training
        label_to_id_map: Mapping from WNID strings to integer IDs

    Returns:
        A function suitable for use with dataset.map_batches()
    """
    if label_to_id_map is None:
        label_to_id_map = IMAGENET_WNID_TO_ID

    transform = get_transform(
        to_torch_tensor=False, random_transforms=random_transforms
    )

    def download_and_process_batch(
        batch: Dict[str, np.ndarray],
    ) -> Dict[str, np.ndarray]:
        """Download images from S3 and apply preprocessing.

        Note: Images are downloaded sequentially within each batch. This is
        intentionally kept simple to avoid thread management complexity when
        running inside Ray tasks. Ray Data's parallelism across batches
        provides the primary throughput scaling.

        Args:
            batch: Dict with "path" and "class" arrays

        Returns:
            Dict with "image" (numpy array) and "label" (int) arrays
        """
        s3_client = create_s3_client()

        processed_images = []
        labels = []

        paths = list(batch["path"])
        classes = list(batch["class"])

        for s3_url, wnid in zip(paths, classes):
            # Download image from S3 using shared utility
            image_buffer = download_s3_object_as_buffer(s3_client, s3_url)

            # Decode and transform image
            image_pil = Image.open(image_buffer).convert("RGB")
            image_tensor = pil_to_tensor(image_pil) / 255.0
            processed_image = np.array(transform(image_tensor))
            processed_images.append(processed_image)

            # Convert label
            labels.append(label_to_id_map.get(wnid, -1))

        return {
            "image": np.stack(processed_images),
            "label": np.array(labels),
        }

    return download_and_process_batch


def create_s3_url_dataset(
    data_dir: str,
    random_transforms: bool = True,
    limit_rows: Optional[int] = None,
) -> ray.data.Dataset:
    """Create a Ray dataset that downloads images from S3 URLs.

    Args:
        data_dir: S3 path to the image directory
        random_transforms: Whether to use random transforms
        limit_rows: Optional row limit

    Returns:
        Ray dataset with "image" and "label" columns
    """
    # List files from S3
    file_records = list_s3_image_files(data_dir)

    # Create dataset from file records
    ds = ray.data.from_items(file_records)

    # Apply row limit if specified
    if limit_rows is not None and limit_rows > 0:
        ds = ds.limit(limit_rows)

    # Download and process images using map_batches
    download_and_process_fn = get_download_and_process_batch_fn(
        random_transforms=random_transforms
    )
    ds = ds.map_batches(download_and_process_fn)

    return ds
