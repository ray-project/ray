#!/usr/bin/env python3
"""
Download or generate a fake TF dataset from images.
"""

from typing import Union, Iterable, Tuple
import os
import sys
import PIL

import tensorflow.compat.v1 as tf
from streaming import MDSWriter

import ray


class ImageCoder(object):
    """Helper class that provides TensorFlow image coding utilities."""

    def __init__(self):
        tf.disable_v2_behavior()

        # Create a single Session to run all image coding calls.
        self._sess = tf.Session()

        # Initializes function that decodes RGB JPEG data.
        self._decode_jpeg_data = tf.placeholder(dtype=tf.string)
        self._decode_jpeg = tf.image.decode_jpeg(self._decode_jpeg_data, channels=3)

    def decode_jpeg(self, image_data: bytes) -> tf.Tensor:
        """Decodes a JPEG image."""
        image = self._sess.run(
            self._decode_jpeg, feed_dict={self._decode_jpeg_data: image_data}
        )
        assert len(image.shape) == 3
        assert image.shape[2] == 3
        return image


def parse_single_image(image_path: str) -> Tuple[bytes, int, int]:
    with open(image_path, "rb") as f:
        image_buffer = f.read()

    coder = ImageCoder()
    image = coder.decode_jpeg(image_buffer)
    height, width, _ = image.shape

    return image_buffer, height, width


def create_single_example(image_path: str) -> tf.train.Example:
    image_buffer, height, width = parse_single_image(image_path)

    label = 0

    example = tf.train.Example(
        features=tf.train.Features(
            feature={
                "image/class/label": _int64_feature(label),
                "image/encoded": _bytes_feature(image_buffer),
            }
        )
    )

    return example


def _int64_feature(value: Union[int, Iterable[int]]) -> tf.train.Feature:
    """Inserts int64 features into Example proto."""
    if not isinstance(value, list):
        value = [value]
    return tf.train.Feature(int64_list=tf.train.Int64List(value=value))


def _bytes_feature(value: Union[bytes, str]) -> tf.train.Feature:
    """Inserts bytes features into Example proto."""
    if isinstance(value, str):
        value = bytes(value, "utf-8")
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))


def preprocess_tfdata(data_root, tf_data_root, max_images_per_file):
    examples = []
    num_shards = 0
    output_filename = os.path.join(tf_data_root, f"data-{num_shards}.tfrecord")
    for image_path in os.listdir(data_root):
        example = create_single_example(
            os.path.join(data_root, image_path)
        ).SerializeToString()
        examples.append(example)

        if len(examples) >= max_images_per_file:
            output_filename = os.path.join(tf_data_root, f"data-{num_shards}.tfrecord")
            with tf.python_io.TFRecordWriter(output_filename) as writer:
                for example in examples:
                    writer.write(example)
            print(f"Done writing {output_filename}", file=sys.stderr)
            examples = []
            num_shards += 1

    output_filename = os.path.join(tf_data_root, f"data-{num_shards}.tfrecord")
    with tf.python_io.TFRecordWriter(output_filename) as writer:
        for example in examples:
            writer.write(example)
    print(f"Done writing {output_filename}", file=sys.stderr)


def preprocess_mosaic(input_dir, output_dir):
    ds = ray.data.read_images(input_dir)
    it = ds.iter_rows()

    columns = {"image": "jpeg", "label": "int"}
    # If reading from local disk, should turn off compression and use
    # streaming.LocalDataset.
    # If uploading to S3, turn on compression (e.g., compression="snappy") and
    # streaming.StreamingDataset.
    with MDSWriter(out=output_dir, columns=columns, compression=None) as out:
        for i, img in enumerate(it):
            out.write(
                {
                    "image": PIL.Image.fromarray(img["image"]),
                    "label": 0,
                }
            )
            if i % 10 == 0:
                print(f"Wrote {i} images.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Preprocess images -> TFRecords."  # noqa: E501
    )
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
        "--max-images-per-file",
        default=32,
        type=int,
    )

    args = parser.parse_args()

    ray.init()

    preprocess_mosaic(args.data_root, args.mosaic_data_root)
    preprocess_tfdata(args.data_root, args.tf_data_root, args.max_images_per_file)
