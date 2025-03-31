import torch
import numpy as np
from typing import Dict, Union, Callable
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
) -> Callable[[Dict[str, Union[np.ndarray, str]]], Dict[str, torch.Tensor]]:
    """Get a map function that transforms a row to the format expected by the training loop.

    Args:
        random_transforms: Whether to use random transforms for training

    Returns:
        A function that takes a row dict with:
        - "image": numpy array in HWC format
        - "class": WNID string

    The output is a dict with "image" and "label" keys.
    """
    crop_resize_transform = get_transform(
        to_torch_tensor=False, random_transforms=random_transforms
    )

    def map_fn(row: Dict[str, Union[np.ndarray, str]]) -> Dict[str, torch.Tensor]:
        """Process a single row into the expected format.

        Args:
            row: Dict containing "image" and "class" keys

        Returns:
            Dict with "image" and "label" keys
        """
        # Convert to CHW format and normalize
        image = torch.from_numpy(row["image"].transpose(2, 0, 1)).float() / 255.0
        # Apply transforms
        image = crop_resize_transform(image)
        # Convert label
        label = IMAGENET_WNID_TO_ID[row["class"]]
        return {"image": image, "label": label}

    return map_fn
