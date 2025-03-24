import io

import numpy as np
from PIL import Image
from torchvision.transforms.functional import pil_to_tensor
from image_classification.imagenet import (
    get_transform,
    IMAGENET_WNID_TO_ID,
)


IMAGENET_JPEG_SPLIT_S3_ROOT = "s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC/"
IMAGENET_JPEG_SPLIT_S3_DIRS = {
    split: f"{IMAGENET_JPEG_SPLIT_S3_ROOT}/{split}"
    for split in ["train", "val", "test"]
}


def get_preprocess_map_fn(decode_image: bool = True, random_transforms: bool = True):
    """Get a map function that transforms a row of the dataset to the format
    expected by the training loop.

    The output is a dict with 2 keys:
    - "image": np.array of the transformed, normalized image
    - "label": An integer index of the WNID

    TODO: Move these preprocessing utilities shared between tests to a separate file.
    """
    crop_resize_transform = get_transform(
        to_torch_tensor=False, random_transforms=random_transforms
    )

    def map_fn(row):
        assert "image" in row and "label" in row, row.keys()

        # Extract WNID from file path
        file_url = row["label"]
        wnid = file_url.split("/")[4]

        if decode_image:
            row["image"] = pil_to_tensor(Image.open(io.BytesIO(row["image"]))) / 255.0

        row["image"] = np.array(crop_resize_transform(row["image"]))
        row["label"] = IMAGENET_WNID_TO_ID[wnid]

        return {"image": row["image"], "label": row["label"]}

    return map_fn
