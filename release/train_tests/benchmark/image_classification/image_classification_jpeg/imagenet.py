import torch
import numpy as np
from typing import Dict, Union, Callable, List
from PIL import Image

from constants import DatasetKey
from image_classification.imagenet import (
    get_transform,
    IMAGENET_WNID_TO_ID,
)


IMAGENET_JPEG_SPLIT_S3_ROOT = "s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC"
IMAGENET_JPEG_SPLIT_S3_DIRS = {
    DatasetKey.TRAIN: f"{IMAGENET_JPEG_SPLIT_S3_ROOT}/train",
    DatasetKey.VALID: f"{IMAGENET_JPEG_SPLIT_S3_ROOT}/val",
    DatasetKey.TEST: f"{IMAGENET_JPEG_SPLIT_S3_ROOT}/test",
}


def get_preprocess_map_fn(
    random_transforms: bool = True,
) -> Callable[[Dict[str, Union[np.ndarray, str]]], Dict[str, torch.Tensor]]:
    """Get a map function that transforms a row to the format expected by the
    training loop.

    Args:
        random_transforms: Whether to use random transforms for training

    Returns:
        A function that takes a row dict with:
        - "image": numpy array in HWC format
        - "class": WNID string

    The output is a dict with "image" and "label" keys.
    """
    crop_resize_transform = get_transform(
        to_torch_tensor=True, random_transforms=random_transforms
    )

    def map_fn(row: Dict[str, Union[np.ndarray, str]]) -> Dict[str, torch.Tensor]:
        """Process a single row into the expected format.

        Args:
            row: Dict containing "image" and "class" keys

        Returns:
            Dict with "image" and "label" keys
        """
        # Convert NumPy HWC image to PIL
        image_pil = Image.fromarray(row["image"])
        # Apply transform (includes ToTensor + Normalize)
        image = crop_resize_transform(image_pil)
        # Convert label
        label = IMAGENET_WNID_TO_ID[row["class"]]
        return {"image": image, "label": label}

    return map_fn


def get_preprocess_map_batch_fn(
    random_transforms: bool = True,
) -> Callable[[Dict[str, np.ndarray]], Dict[str, List[torch.Tensor]]]:
    """
    Returns a function compatible with map_batches, which processes a batch of images
    and labels. The returned function expects a batch in 'numpy' format and outputs a
    dict of lists of torch.Tensors.

    Args:
        random_transforms: Whether to apply random data augmentations.

    Returns:
        Callable: A function that takes a batch dict and returns a dict with processed
        images and labels.
    """
    crop_resize_transform = get_transform(
        to_torch_tensor=True, random_transforms=random_transforms
    )

    def map_batch_fn(batch: Dict[str, np.ndarray]) -> Dict[str, List[torch.Tensor]]:
        """
        Processes a batch of images and labels.

        Args:
            batch: Dict containing 'image' (np.ndarray, shape: (B, H, W, C)) and
                   'class' (np.ndarray, shape: (B,)).

        Returns:
            Dict containing 'image' (list of torch.Tensor) and 'label' (list of int).
        """
        images = batch["image"]
        classes = batch["class"]

        processed_images = []
        labels = []

        for img, cls in zip(images, classes):
            # Convert NumPy HWC image to PIL Image for transformation
            image_pil = Image.fromarray(img)
            # Apply the transformation pipeline (e.g., crop, resize, normalize, to
            # tensor)
            image = crop_resize_transform(image_pil)
            # Convert class label from WNID string to integer ID
            label = IMAGENET_WNID_TO_ID[cls]
            processed_images.append(image)
            labels.append(label)

        return {"image": processed_images, "label": labels}

    return map_batch_fn
