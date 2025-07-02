import io
import numpy as np
from typing import Dict, Union, Callable
from PIL import Image
from torchvision.transforms.functional import pil_to_tensor

from constants import DatasetKey
from image_classification.imagenet import (
    get_transform,
    IMAGENET_WNID_TO_ID,
)

IMAGENET_PARQUET_SPLIT_S3_ROOT = (
    "s3://ray-benchmark-data-internal-us-west-2/imagenet/parquet_split"
)
IMAGENET_PARQUET_SPLIT_S3_DIRS = {
    DatasetKey.TRAIN: f"{IMAGENET_PARQUET_SPLIT_S3_ROOT}/train",
    DatasetKey.VALID: f"{IMAGENET_PARQUET_SPLIT_S3_ROOT}/val",
    DatasetKey.TEST: f"{IMAGENET_PARQUET_SPLIT_S3_ROOT}/test",
}


def get_preprocess_map_fn(
    decode_image: bool = True, random_transforms: bool = True
) -> Callable[[Dict[str, Union[bytes, str]]], Dict[str, Union[np.ndarray, int]]]:
    """Get a map function that transforms a row of the dataset to the format
    expected by the training loop.

    Args:
        decode_image: Whether to decode the image bytes into a tensor
        random_transforms: Whether to use random transforms for training

    Returns:
        A function that takes a row dict and returns a processed dict.
        Input row dict should have:
        - "image": bytes or tensor in CHW format
        - "label": WNID string

        Output dict has:
        - "image": np.array of the transformed, normalized image
        - "label": An integer index of the WNID
    """
    crop_resize_transform = get_transform(
        to_torch_tensor=False, random_transforms=random_transforms
    )

    def map_fn(row: Dict[str, Union[bytes, str]]) -> Dict[str, Union[np.ndarray, int]]:
        assert "image" in row and "label" in row, row.keys()

        if decode_image:
            row["image"] = pil_to_tensor(Image.open(io.BytesIO(row["image"]))) / 255.0

        row["image"] = np.array(crop_resize_transform(row["image"]))
        row["label"] = IMAGENET_WNID_TO_ID[row["label"]]

        return {"image": row["image"], "label": row["label"]}

    return map_fn
