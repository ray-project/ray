import ray
import torch
import torchvision
from torchvision.transforms.functional import pil_to_tensor
import os
import tensorflow as tf
import numpy as np
from PIL import Image
from typing import TYPE_CHECKING, Iterator, Callable, Any
import pandas as pd
from benchmark import Benchmark

import streaming
from streaming import LocalDataset, StreamingDataset

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder

if TYPE_CHECKING:
    import pyarrow

DEFAULT_IMAGE_SIZE = 224

# tf.data needs to resize all images to the same size when loading.
# This is the size of dog.jpg in s3://air-cuj-imagenet-1gb.
FULL_IMAGE_SIZE = (1213, 1546)


def build_torch_dataset(
    root_dir, batch_size, shuffle=False, num_workers=None, transform=None
):
    if num_workers is None:
        num_workers = os.cpu_count()

    data = torchvision.datasets.ImageFolder(root_dir, transform=transform)
    data_loader = torch.utils.data.DataLoader(
        data,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        persistent_workers=True,
    )
    return data_loader


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
    if len(shape) == num_channels + 1:
        shape = shape[1:]

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


def build_tfrecords_tf_dataset(data_root, batch_size):
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


def get_transform(to_torch_tensor):
    # Note(swang): This is a different order from tf.data.
    # torch: decode -> randCrop+resize -> randFlip
    # tf.data: decode -> randCrop -> randFlip -> resize
    transform = torchvision.transforms.Compose(
        [
            torchvision.transforms.RandomResizedCrop(
                antialias=True,
                size=DEFAULT_IMAGE_SIZE,
                scale=(0.05, 1.0),
                ratio=(0.75, 1.33),
            ),
            torchvision.transforms.RandomHorizontalFlip(),
        ]
        + ([torchvision.transforms.ToTensor()] if to_torch_tensor else [])
        + [
            torchvision.transforms.Normalize(
                mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
            )
        ]
    )
    return transform


# Capture `transform`` in the map UDFs.
transform = get_transform(False)


def crop_and_flip_image(row):
    # Make sure to use torch.tensor here to avoid a copy from numpy.
    row["image"] = transform(
        torch.tensor(np.transpose(row["image"], axes=(2, 0, 1))) / 255.0
    )
    return row


def center_crop_image(row):
    # Used to generate the validation set. The main difference between
    # `crop_and_flip_image` and this method is that the validation set
    # should avoid random cropping from the full image, but instead
    # should resize and take the center crop to generate more consistent
    # outputs.
    val_transform = torchvision.transforms.Compose(
        [
            torchvision.transforms.Resize(256),
            torchvision.transforms.CenterCrop(224),
            torchvision.transforms.Normalize(
                mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
            ),
        ]
    )
    # Make sure to use torch.tensor here to avoid a copy from numpy.
    row["image"] = val_transform(
        torch.tensor(
            np.transpose(row["image"], axes=(2, 0, 1)),
        )
        / 255.0
    )
    return row


def crop_and_flip_image_batch(image_batch):
    image_batch["image"] = transform(
        # Make sure to use torch.tensor here to avoid a copy from numpy.
        # Original dims are (batch_size, channels, height, width).
        torch.tensor(np.transpose(image_batch["image"], axes=(0, 3, 1, 2)) / 255.0)
    )
    return image_batch


def decode_image_crop_and_flip(row):
    row["image"] = Image.frombytes("RGB", (row["height"], row["width"]), row["image"])
    # Convert back np to avoid storing a np.object array.
    return {"image": np.array(transform(pil_to_tensor(row["image"]) / 255.0))}


class MdsDatasource(ray.data.datasource.FileBasedDatasource):
    _FILE_EXTENSIONS = ["mds"]

    def _read_stream(
        self, f: "pyarrow.NativeFile", path: str
    ) -> Iterator[ray.data.block.Block]:
        file_info = streaming.base.format.base.reader.FileInfo(
            basename=os.path.basename(path), bytes=os.stat(path).st_size, hashes={}
        )
        reader = streaming.base.format.mds.MDSReader(
            dirname=os.path.dirname(path),
            split=None,
            column_encodings=["pil", "int"],
            column_names=["image", "label"],
            column_sizes=[None, 8],
            compression=None,
            hashes=[],
            raw_data=file_info,
            samples=-1,
            size_limit=None,
            zip_data=None,
        )

        i = 0
        while True:
            try:
                row = reader.decode_sample(reader.get_sample_data(i))
            except IndexError:
                break
            row["image"] = np.array(row["image"])
            builder = DelegatingBlockBuilder()
            builder.add(row)
            block = builder.build()
            yield block

            i += 1


class MosaicDataset(LocalDataset):
    def __init__(self, local: str, transforms: Callable) -> None:
        super().__init__(local=local)
        self.transforms = transforms

    def __getitem__(self, idx: int) -> Any:
        obj = super().__getitem__(idx)
        image = obj["image"]
        label = obj["label"]
        return self.transforms(image), label


class S3MosaicDataset(StreamingDataset):
    def __init__(
        self,
        s3_bucket: str,
        num_physical_nodes,
        cache_dir: str,
        transforms: Callable,
        cache_limit=None,
        epoch_size=None,
    ) -> None:
        super().__init__(
            remote=s3_bucket,
            local=cache_dir,
            cache_limit=cache_limit,
            epoch_size=epoch_size,
            # Set StreamingDataset to read sequentially.
            shuffle=False,
            num_canonical_nodes=num_physical_nodes,
        )
        self.transforms = transforms

    def __getitem__(self, idx: int) -> Any:
        obj = super().__getitem__(idx)
        image = obj["image"]
        label = obj["label"]
        return self.transforms(image), label


def get_mosaic_dataloader(
    mosaic_data_root,
    batch_size,
    num_physical_nodes,
    epoch_size=None,
    num_workers=None,
    cache_limit=None,
):
    # MosaicML StreamingDataset.
    use_s3 = mosaic_data_root.startswith("s3://")

    if not use_s3:
        assert epoch_size is None, "epoch_size not supported for streaming.LocalDataset"
        assert (
            cache_limit is None
        ), "cache_limit not supported for streaming.LocalDataset"

    if use_s3:
        MOSAIC_CACHE = "/tmp/mosaic_cache"
        try:
            import shutil

            shutil.rmtree(MOSAIC_CACHE)
        except (OSError, FileNotFoundError):
            pass
        streaming.base.util.clean_stale_shared_memory()
        print(f"Initializing mosaic StreamingDataset, cache_limit={cache_limit}")
        mosaic_ds = S3MosaicDataset(
            s3_bucket=mosaic_data_root,
            num_physical_nodes=num_physical_nodes,
            cache_dir=MOSAIC_CACHE,
            cache_limit=cache_limit,
            epoch_size=epoch_size,
            transforms=get_transform(True),
        )
    else:
        mosaic_ds = MosaicDataset(mosaic_data_root, transforms=get_transform(True))

    if num_workers is None:
        num_workers = os.cpu_count()

    print(f"Initializing torch DataLoader with {num_workers} workers.")
    mosaic_dl = torch.utils.data.DataLoader(
        mosaic_ds,
        batch_size=batch_size,
        num_workers=num_workers,
        drop_last=True,
    )

    return mosaic_dl


def get_ray_mosaic_dataset(mosaic_data_root):
    mds_source = MdsDatasource(mosaic_data_root)
    return ray.data.read_datasource(mds_source)


def get_ray_parquet_dataset(parquet_data_root, parallelism=None):
    if parallelism is not None:
        ray_dataset = ray.data.read_parquet(
            parquet_data_root, override_num_blocks=parallelism
        )
    else:
        ray_dataset = ray.data.read_parquet(parquet_data_root)
    ray_dataset = ray_dataset.map(decode_image_crop_and_flip)
    return ray_dataset


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-root",
        default=None,
        type=str,
        help='Directory path with TFRecords. Filenames should start with "train".',
    )
    parser.add_argument(
        "--parquet-data-root",
        default=None,
        type=str,
        help="Directory path with Parquet files.",
    )
    parser.add_argument(
        "--mosaic-data-root",
        default=None,
        type=str,
        help="Directory path with MDS files.",
    )
    parser.add_argument(
        "--tf-data-root",
        default=None,
        type=str,
        help="Directory path with TFRecords.",
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
    parser.add_argument(
        "--output-file",
        default=None,
        type=str,
        help="Output CSV path.",
    )
    args = parser.parse_args()

    metrics = {}
    benchmark = Benchmark()

    if args.data_root is not None:
        # tf.data, load images.
        tf_dataset = tf.keras.preprocessing.image_dataset_from_directory(
            args.data_root,
            batch_size=args.batch_size,
            image_size=(DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE),
        )
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "tf_data",
                tf_dataset,
            )

        # tf.data, with transform.
        tf_dataset = tf.keras.preprocessing.image_dataset_from_directory(args.data_root)
        tf_dataset = tf_dataset.map(lambda img, label: (tf_crop_and_flip(img), label))
        tf_dataset.unbatch().batch(args.batch_size)
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "tf_data+transform",
                tf_dataset,
            )

        # torch, load images.
        torch_dataset = build_torch_dataset(
            args.data_root,
            args.batch_size,
            transform=torchvision.transforms.Compose(
                [
                    torchvision.transforms.Resize(
                        (DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE)
                    ),
                    torchvision.transforms.ToTensor(),
                ]
            ),
        )
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "torch",
                torch_dataset,
            )

        # torch, with transform.
        torch_dataset = build_torch_dataset(
            args.data_root, args.batch_size, transform=get_transform(True)
        )
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "torch+transform",
                torch_dataset,
            )

        # ray.data, load images.
        ray_dataset = ray.data.read_images(
            args.data_root, mode="RGB", size=(DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE)
        )
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "ray_data",
                ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            )

        # ray.data, with transform.
        ray_dataset = ray.data.read_images(args.data_root, mode="RGB").map(
            crop_and_flip_image
        )
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "ray_data+map_transform",
                ray_dataset.iter_batches(batch_size=args.batch_size),
            )

        # Pass size to read_images when using map_batches to make sure that all
        # batches have rows with the same dimensions.
        ray_dataset = ray.data.read_images(
            args.data_root, mode="RGB", size=(256, 256)
        ).map_batches(crop_and_flip_image_batch)
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "ray_data+transform",
                ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            )

        ray_dataset = ray.data.read_images(
            args.data_root, mode="RGB", size=(256, 256)
        ).map_batches(crop_and_flip_image_batch, zero_copy_batch=True)
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "ray_data+transform+zerocopy",
                ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            )

    if args.tf_data_root is not None:
        tf_dataset = build_tfrecords_tf_dataset(args.tf_data_root, args.batch_size)
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "tf_data_tfrecords+transform",
                tf_dataset,
            )

        ray_dataset = ray.data.read_tfrecords(args.tf_data_root)
        ray_dataset = ray_dataset.map_batches(
            decode_crop_and_flip_tf_record_batch,
            batch_size=args.batch_size,
            batch_format="pandas",
        )
        for i in range(args.num_epochs):
            tf_dataset = ray_dataset.to_tf(
                batch_size=args.batch_size,
                feature_columns="image",
                label_columns="label",
            )
            benchmark.run_iterate_ds(
                "ray_data_tfrecords+transform",
                tf_dataset,
            )

    if args.parquet_data_root is not None:
        ray_dataset = get_ray_parquet_dataset(args.parquet_data_root, parallelism=128)
        for i in range(args.num_epochs):
            benchmark.run_iterate_ds(
                "ray_data_parquet+map_transform",
                ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            )
        print(ray_dataset.stats())

    if args.mosaic_data_root is not None:
        num_workers = None
        mosaic_dl = get_mosaic_dataloader(
            args.mosaic_data_root,
            batch_size=args.batch_size,
            num_physical_nodes=1,
            num_workers=num_workers,
        )

        for i in range(args.num_epochs):
            benchmark.run_iterate_ds("mosaicml_mds", mosaic_dl)

        # ray.data.
        use_s3 = args.mosaic_data_root.startswith("s3://")
        if not use_s3:
            mds_source = MdsDatasource(args.mosaic_data_root)
            ray_dataset = ray.data.read_datasource(mds_source)
            ray_dataset = ray_dataset.map(crop_and_flip_image)
            for i in range(args.num_epochs):
                benchmark.run_iterate_ds(
                    "ray_data_mds+map_transform",
                    ray_dataset.iter_torch_batches(batch_size=args.batch_size),
                )

    benchmark.write_result()
