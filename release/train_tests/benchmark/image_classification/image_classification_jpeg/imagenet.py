import torch
import numpy as np
from image_classification.imagenet import (
    get_transform,
    IMAGENET_WNID_TO_ID,
)


IMAGENET_JPEG_SPLIT_S3_ROOT = "s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC"
IMAGENET_JPEG_SPLIT_S3_DIRS = {
    split: f"{IMAGENET_JPEG_SPLIT_S3_ROOT}/{split}"
    for split in ["train", "val", "test"]
}


def get_preprocess_map_fn(random_transforms: bool = True):
    """Get a map function that transforms a row of the dataset to the format
    expected by the training loop.

    The output is a dict with 2 keys:
    - "image": np.array of the transformed, normalized image
    - "label": An integer index of the WNID
    """
    crop_resize_transform = get_transform(
        to_torch_tensor=False, random_transforms=random_transforms
    )

    def map_fn(row):
        row["image"] = crop_resize_transform(
            torch.tensor(np.transpose(row["image"], axes=(2, 0, 1))) / 255.0
        )
        row["label"] = IMAGENET_WNID_TO_ID[row["class"]]
        row.pop("class")
        return {"image": row["image"], "label": row["label"]}

    return map_fn
