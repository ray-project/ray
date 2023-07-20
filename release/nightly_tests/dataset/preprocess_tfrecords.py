#!/usr/bin/env python3
"""
Download or generate a fake TF dataset from images.
"""

from typing import Union, Iterable, Tuple, Optional
import os
import requests
import shutil
import sys
import tensorflow.compat.v1 as tf
import time

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


def preprocess_images(data_root, tf_data_root, max_images_per_file):
    examples = []
    num_shards = 0
    output_filename = os.path.join(tf_data_root, f"data-{num_shards}.tfrecord")
    for subdir in os.listdir(data_root):
        for image_path in os.listdir(os.path.join(data_root, subdir)):
            print(image_path)
            example = create_single_example(os.path.join(data_root, subdir, image_path)).SerializeToString()
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
        "--tf-data-root",
        default="/tmp/tf-data",
        type=str,
        help='Directory path with TFRecords. Filenames should start with "train".',
    )
    parser.add_argument(
        "--max-images-per-file",
        default=256,
        type=int,
    )

    args = parser.parse_args()

    ray.init()

    preprocess_images(args.data_root, args.tf_data_root, args.max_images_per_file)
