"""ImageNet dataset loading via S3 URL download with Ray Data expressions.

This module provides dataset loading that:
1. Lists JPEG files from S3 using boto3 (parallelized via Ray tasks)
2. Creates a Ray dataset from the file records
3. Uses Ray Data expressions (alpha) to download image bytes efficiently
4. Uses map_batches to decode and process images

This approach leverages Ray Data's expressions API for optimized parallel I/O,
separating the download step from image processing for better throughput.
"""

import io
import logging
from functools import lru_cache
from typing import Callable, Dict, List, Optional, Tuple

import boto3
import numpy as np
from PIL import Image
from torchvision.transforms.functional import pil_to_tensor

import ray.data
from ray.data.expressions import download

from constants import DatasetKey
from image_classification.imagenet import (
    get_transform,
    IMAGENET_WNID_TO_ID,
)

logger = logging.getLogger(__name__)

# S3 configuration for ImageNet JPEG data
AWS_REGION = "us-west-2"
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
    s3_client = boto3.client("s3", region_name=AWS_REGION)
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
    s3_client = boto3.client("s3", region_name=AWS_REGION)
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

    # Parse S3 URL: s3://bucket/prefix
    s3_path = data_dir
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    parts = s3_path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].rstrip("/") if len(parts) > 1 else ""

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


def get_process_batch_fn(
    random_transforms: bool = True,
    label_to_id_map: Optional[Dict[str, int]] = None,
) -> Callable[[Dict[str, np.ndarray]], Dict[str, np.ndarray]]:
    """Get a map_batches function that processes pre-downloaded image bytes.

    This function expects image bytes to already be downloaded (via Ray Data
    expressions) and handles decoding and transformations.

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

    def process_batch(
        batch: Dict[str, np.ndarray],
    ) -> Dict[str, np.ndarray]:
        """Process pre-downloaded image bytes.

        Args:
            batch: Dict with "bytes" (image data) and "class" arrays

        Returns:
            Dict with "image" (numpy array) and "label" (int) arrays
        """
        processed_images = []
        labels = []

        image_bytes_list = list(batch["bytes"])
        classes = list(batch["class"])

        for data, wnid in zip(image_bytes_list, classes):
            # Decode and transform image
            image_pil = Image.open(io.BytesIO(data)).convert("RGB")
            image_tensor = pil_to_tensor(image_pil) / 255.0
            processed_image = np.array(transform(image_tensor))
            processed_images.append(processed_image)

            # Convert label
            labels.append(label_to_id_map[wnid])

        return {
            "image": np.stack(processed_images),
            "label": np.array(labels),
        }

    return process_batch


def create_s3_url_dataset(
    data_dir: str,
    random_transforms: bool = True,
    limit_rows: Optional[int] = None,
) -> ray.data.Dataset:
    """Create a Ray dataset that downloads images from S3 URLs.

    Uses Ray Data expressions (alpha) for efficient parallel downloads,
    then map_batches for image decoding and transformations.

    Args:
        data_dir: S3 path to the image directory
        random_transforms: Whether to use random transforms
        limit_rows: Optional row limit

    Returns:
        Ray dataset with "image" and "label" columns
    """
    file_records = list_s3_image_files(data_dir)

    ds = ray.data.from_items(file_records)

    if limit_rows is not None and limit_rows > 0:
        ds = ds.limit(limit_rows)

    # Download image bytes using Ray Data expressions (alpha)
    # This enables optimized parallel I/O managed by Ray Data
    ds = ds.with_column("bytes", download("path"))

    # Process downloaded bytes (decode and transform)
    process_fn = get_process_batch_fn(random_transforms=random_transforms)
    ds = ds.map_batches(process_fn)

    return ds
