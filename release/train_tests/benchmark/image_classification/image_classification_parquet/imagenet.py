import io
import torch
import numpy as np
from typing import Dict, List, Union, Callable
from PIL import Image
from torchvision.transforms.functional import pil_to_tensor
from image_classification.imagenet import (
    get_transform,
    IMAGENET_WNID_TO_ID,
)

IMAGENET_PARQUET_SPLIT_S3_ROOT = (
    "s3://ray-benchmark-data-internal/imagenet/parquet_split"
)
IMAGENET_PARQUET_SPLIT_S3_DIRS = {
    split: f"{IMAGENET_PARQUET_SPLIT_S3_ROOT}/{split}"
    for split in ["train", "val", "test"]
}


def get_preprocess_map_fn(
    decode_image: bool = True,
    random_transforms: bool = True,
) -> Callable[
    [
        Union[
            Dict[str, Union[bytes, str]],
            List[Dict[str, Union[bytes, str]]],
        ]
    ],
    Union[Dict[str, torch.Tensor], List[Dict[str, torch.Tensor]]],
]:
    """Get a map function that transforms a row or batch of rows to the format
    expected by the training loop.

    Args:
        decode_image: Whether to decode the image bytes into a tensor
        random_transforms: Whether to use random transforms for training

    Returns:
        A function that takes either a single row dict or a list of row dicts.
        Each row dict should have:
        - "image": bytes or tensor in CHW format
        - "label": WNID string

    The output is either:
    - For a single row: A dict with "image" and "label" keys
    - For a batch: A list of dicts, each with "image" and "label" keys
    """
    crop_resize_transform = get_transform(
        to_torch_tensor=False, random_transforms=random_transforms
    )

    def process_row(row: Dict[str, Union[bytes, str]]) -> Dict[str, torch.Tensor]:
        """Process a single row into the expected format.

        Args:
            row: Dict containing "image" and "label" keys

        Returns:
            Dict with "image" and "label" keys
        """
        assert "image" in row and "label" in row, row.keys()

        # Decode image if needed
        if decode_image:
            image = pil_to_tensor(Image.open(io.BytesIO(row["image"]))) / 255.0
        else:
            image = row["image"]

        # Apply transforms
        image = np.array(crop_resize_transform(image))
        # Convert label
        label = IMAGENET_WNID_TO_ID[row["label"]]
        return {"image": image, "label": label}

    def map_fn(
        row_or_batch: Union[
            Dict[str, Union[bytes, str]],
            List[Dict[str, Union[bytes, str]]],
        ]
    ) -> Union[Dict[str, torch.Tensor], List[Dict[str, torch.Tensor]]]:
        # Handle single row case
        if isinstance(row_or_batch, dict):
            return process_row(row_or_batch)

        # Handle batch case
        return [process_row(row) for row in row_or_batch]

    return map_fn
