import torch
import torchvision
import os
from typing import Any, Callable
import time
import tensorflow as tf
import pandas as pd
import json

import ray
from streaming import LocalDataset


DEFAULT_IMAGE_SIZE = 224

# tf.data needs to resize all images to the same size when loading.
# This is the size of dog.jpg in s3://air-cuj-imagenet-1gb.
FULL_IMAGE_SIZE = (1213, 1546)


def iterate(dataset, label, batch_size, metrics):
    start = time.time()
    it = iter(dataset)
    num_rows = 0
    for batch in it:
        # NOTE(swang): This will be slightly off if batch_size does not divide
        # evenly into number of images but should be okay for large enough
        # datasets.
        num_rows += batch_size
    end = time.time()
    print(label, end - start, "epoch", i)

    tput = num_rows / (end - start)
    metrics[label] = tput


class MosaicDataset(LocalDataset):
    def __init__(self, local: str, transforms: Callable) -> None:
        super().__init__(local=local)
        self.transforms = transforms

    def __getitem__(self, idx: int) -> Any:
        obj = super().__getitem__(idx)
        image = obj["image"]
        label = obj["label"]
        return self.transforms(image), label


def parse_and_decode_tfrecord(example_serialized):
    feature_map = {
        "image/encoded": tf.io.FixedLenFeature([], dtype=tf.string, default_value=""),
        "image/class/label": tf.io.FixedLenFeature(
            [], dtype=tf.int64, default_value=-1
        ),
    }

    features = tf.io.parse_single_example(example_serialized, feature_map)
    label = tf.cast(features["image/class/label"], dtype=tf.int32)

    image_buffer = features["image/encoded"]
    image_buffer = tf.reshape(image_buffer, shape=[])
    image_buffer = tf.io.decode_jpeg(image_buffer, channels=3)
    return image_buffer, label


def tf_crop_and_flip(image_buffer, num_channels=3):
    """Crops the given image to a random part of the image, and randomly flips.

    We use the fused decode_and_crop op, which performs better than the two ops
    used separately in series, but note that this requires that the image be
    passed in as an un-decoded string Tensor.

    Args:
        image_buffer: scalar string Tensor representing the raw JPEG image buffer.
        bbox: 3-D float Tensor of bounding boxes arranged [1, num_boxes, coords]
            where each coordinate is [0, 1) and the coordinates are arranged as
            [ymin, xmin, ymax, xmax].
        num_channels: Integer depth of the image buffer for decoding.

    Returns:
        3-D tensor with cropped image.

    """
    # A large fraction of image datasets contain a human-annotated bounding box
    # delineating the region of the image containing the object of interest.    We
    # choose to create a new bounding box for the object which is a randomly
    # distorted version of the human-annotated bounding box that obeys an
    # allowed range of aspect ratios, sizes and overlap with the human-annotated
    # bounding box. If no box is supplied, then we assume the bounding box is
    # the entire image.
    shape = tf.shape(image_buffer)
    bbox = tf.constant(
        [0.0, 0.0, 1.0, 1.0], dtype=tf.float32, shape=[1, 1, 4]
    )  # From the entire image
    sample_distorted_bounding_box = tf.image.sample_distorted_bounding_box(
        shape,
        bounding_boxes=bbox,
        min_object_covered=0.1,
        aspect_ratio_range=[0.75, 1.33],
        area_range=[0.05, 1.0],
        max_attempts=100,
        use_image_if_no_bounding_boxes=True,
    )
    bbox_begin, bbox_size, _ = sample_distorted_bounding_box

    # Reassemble the bounding box in the format the crop op requires.
    offset_y, offset_x, _ = tf.unstack(bbox_begin)
    target_height, target_width, _ = tf.unstack(bbox_size)

    image_buffer = tf.image.crop_to_bounding_box(
        image_buffer,
        offset_height=offset_y,
        offset_width=offset_x,
        target_height=target_height,
        target_width=target_width,
    )
    # Flip to add a little more random distortion in.
    image_buffer = tf.image.random_flip_left_right(image_buffer)
    image_buffer = tf.compat.v1.image.resize(
        image_buffer,
        [DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE],
        method=tf.image.ResizeMethod.BILINEAR,
        align_corners=False,
    )
    return image_buffer


def build_tf_dataset(data_root, batch_size):
    filenames = [
        os.path.join(data_root, pathname) for pathname in os.listdir(data_root)
    ]
    ds = tf.data.Dataset.from_tensor_slices(filenames)
    ds = ds.interleave(tf.data.TFRecordDataset).map(
        parse_and_decode_tfrecord, num_parallel_calls=tf.data.experimental.AUTOTUNE
    )
    ds = ds.map(lambda img, label: (tf_crop_and_flip(img), label))
    ds = ds.batch(batch_size)
    return ds


def decode_crop_and_flip_tf_record_batch(tf_record_batch: pd.DataFrame) -> pd.DataFrame:
    """
    This version of the preprocessor fuses the load step with the crop and flip
    step, which should have better performance (at the cost of re-executing the
    load step on each epoch):
    - the reference tf.data implementation can use the fused decode_and_crop op
    - ray.data doesn't have to materialize the intermediate decoded batch.
    """

    def process_images():
        for image_buffer in tf_record_batch["image/encoded"]:
            # Each image output is ~600KB.
            image_buffer = tf.reshape(image_buffer, shape=[])
            image_buffer = tf.io.decode_jpeg(image_buffer, channels=3)
            yield tf_crop_and_flip(image_buffer).numpy()

    labels = (tf_record_batch["image/class/label"]).astype("float32")
    df = pd.DataFrame.from_dict({"image": process_images(), "label": labels})

    return df


def build_ray_dataset(data_root, batch_size):
    filenames = [
        os.path.join(data_root, pathname) for pathname in os.listdir(data_root)
    ]
    ds = ray.data.read_tfrecords(filenames)
    ds = ds.map_batches(decode_crop_and_flip_tf_record_batch, batch_format="pandas")
    return ds


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-root",
        default="/tmp/imagenet-1gb-data",
        type=str,
        help='Directory path with TFRecords. Filenames should start with "train".',
    )
    parser.add_argument(
        "--mosaic-data-root",
        default="/tmp/mosaicml-data",
        type=str,
        help='Directory path with TFRecords. Filenames should start with "train".',
    )
    parser.add_argument(
        "--tf-data-root",
        default="/tmp/tf-data",
        type=str,
        help='Directory path with TFRecords. Filenames should start with "train".',
    )
    parser.add_argument(
        "--batch-size",
        default=32,
        type=int,
        help="Batch size to use.",
    )
    parser.add_argument(
        "--num-epochs",
        default=3,
        type=int,
        help="Number of epochs to run. The throughput for the last epoch will be kept.",
    )
    args = parser.parse_args()

    metrics = {}

    # MosaicML streaming.
    transform = torchvision.transforms.Compose(
        [
            torchvision.transforms.RandomResizedCrop(
                size=DEFAULT_IMAGE_SIZE,
                scale=(0.05, 1.0),
                ratio=(0.75, 1.33),
            ),
            torchvision.transforms.RandomHorizontalFlip(),
            torchvision.transforms.ToTensor(),
        ]
    )
    mosaic_ds = MosaicDataset(args.mosaic_data_root, transforms=transform)
    num_workers = os.cpu_count()
    mosaic_dl = torch.utils.data.DataLoader(
        mosaic_ds, batch_size=args.batch_size, num_workers=num_workers
    )
    for i in range(args.num_epochs):
        iterate(mosaic_dl, "mosaic", args.batch_size, metrics)

    # Tf.data.
    tf_ds = build_tf_dataset(args.tf_data_root, args.batch_size)
    for i in range(args.num_epochs):
        iterate(tf_ds, "tf_data", args.batch_size, metrics)

    # ray.data.
    ray_ds = build_ray_dataset(args.tf_data_root, args.batch_size)
    for i in range(args.num_epochs):
        iterate(
            ray_ds.iter_batches(batch_size=args.batch_size),
            "ray_tfrecords",
            args.batch_size,
            metrics,
        )

    metrics_dict = {}
    for label, tput in metrics.items():
        metrics_dict[label] = {
            "THROUGHPUT": tput,
        }
    result_dict = {
        "perf_metrics": metrics_dict,
        "success": 1,
    }

    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/preprocessed_image_loader_microbenchmark.json"
    )

    with open(test_output_json, "wt") as f:
        json.dump(result_dict, f)
