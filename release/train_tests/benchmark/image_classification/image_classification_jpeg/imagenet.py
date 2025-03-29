import torch
import numpy as np
from typing import Dict, List, Union, Callable
from image_classification.imagenet import (
    get_transform,
    IMAGENET_WNID_TO_ID,
)


IMAGENET_JPEG_SPLIT_S3_ROOT = "s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC"
IMAGENET_JPEG_SPLIT_S3_DIRS = {
    split: f"{IMAGENET_JPEG_SPLIT_S3_ROOT}/{split}"
    for split in ["train", "val", "test"]
}


def get_preprocess_map_fn(
    random_transforms: bool = True,
) -> Callable[
    [
        Union[
            Dict[str, Union[np.ndarray, str]],
            List[Dict[str, Union[np.ndarray, str]]],
        ]
    ],
    Union[Dict[str, torch.Tensor], List[Dict[str, torch.Tensor]]],
]:
    """Get a map function that transforms a row or batch of rows to the format
    expected by the training loop.

    Args:
        random_transforms: Whether to use random transforms for training

    Returns:
        A function that takes either a single row dict or a list of row dicts.
        Each row dict should have:
        - "image": numpy array in HWC format
        - "class": WNID string

    The output is either:
    - For a single row: A dict with "image" and "label" keys
    - For a batch: A list of dicts, each with "image" and "label" keys
    """
    crop_resize_transform = get_transform(
        to_torch_tensor=False, random_transforms=random_transforms
    )

    def process_row(row: Dict[str, Union[np.ndarray, str]]) -> Dict[str, torch.Tensor]:
        """Process a single row into the expected format.

        Args:
            row: Dict containing "image" and "class" keys

        Returns:
            Dict with "image" and "label" keys
        """
        # Convert to CHW format and normalize
        image = torch.tensor(np.transpose(row["image"], axes=(2, 0, 1))) / 255.0
        # Apply transforms
        image = crop_resize_transform(image)
        # Convert label
        label = IMAGENET_WNID_TO_ID[row["class"]]
        return {"image": image, "label": label}

    def map_fn(
        row_or_batch: Union[
            Dict[str, Union[np.ndarray, str]],
            List[Dict[str, Union[np.ndarray, str]]],
        ]
    ) -> Union[Dict[str, torch.Tensor], List[Dict[str, torch.Tensor]]]:
        # Handle single row case
        if isinstance(row_or_batch, dict):
            return process_row(row_or_batch)

        # Handle batch case
        return [process_row(row) for row in row_or_batch]

    return map_fn
