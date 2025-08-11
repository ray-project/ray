#!/usr/bin/env python3
"""
Download or generate a fake dataset for training resnet50 in TensorFlow.
"""

from typing import Union, Iterable, Tuple, Optional
import os
import requests
import shutil
import sys
import tensorflow.compat.v1 as tf
import time

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
import ray

DEFAULT_IMAGE_URL = "https://air-example-data-2.s3.us-west-2.amazonaws.com/1G-image-data-synthetic-raw/dog.jpg"  # noqa: E501


def parse_args() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Download or generate a fake dataset for training resnet50 in TensorFlow."  # noqa: E501
    )
    parser.add_argument(
        "--num-shards",
        type=int,
        default=32,
        help="The number of files to create in the output directory.",
    )
    parser.add_argument(
        "--output-directory",
        type=str,
        required=True,
        help="The directory in which to place the fake dataset files.",
    )

    parser.add_argument(
        "--single-image-url",
        type=str,
        default=DEFAULT_IMAGE_URL,
        help="If --shard-url is not provided, use the image found at this URL to generate a fake dataset.",  # noqa: E501
    )

    parser.add_argument(
        "--num-nodes",
        type=int,
        default=1,
        help="The total number of nodes to expect in the cluster. "
        "Files will be generated on each of these nodes.",
    )

    input_data_group = parser.add_mutually_exclusive_group(required=True)
    input_data_group.add_argument(
        "--shard-url",
        type=str,
        default=None,
        help="Download this shard and copy it --num-shards times.",
    )
    input_data_group.add_argument(
        "--num-images-per-shard",
        type=int,
        help="Copy the image at --single-image-url this many times and store in each tfrecord shard.",  # noqa: E501
    )

    args = parser.parse_args()
    return args


@ray.remote
def generate_local_files(
    num_shards: int,
    num_images_per_shard: Optional[int],
    shard_url: Optional[str],
    image_url: str,
    output_directory: str,
) -> None:
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    for filename in os.listdir(output_directory):
        os.remove(os.path.join(output_directory, filename))

    print(
        f"Creating a tfrecord dataset with {num_shards} shards, {num_images_per_shard} images per shard, in the output directory {output_directory}"  # noqa: E501
    )

    def gen_filename(i: int, total: int) -> str:
        return os.path.join(
            output_directory, f"single-image-repeated-{i:05d}-of-{total:05d}"
        )

    filenames = [gen_filename(i, num_shards) for i in range(num_shards)]

    if num_images_per_shard:
        single_example = create_single_example(image_url).SerializeToString()
        write_shard(filenames[0], single_example, num_images_per_shard)
    elif shard_url:
        download_single_shard(shard_url, filenames[0])

    bcast_single_shard(filenames[0], filenames[1:])


def bcast_single_shard(src_filename: str, dst_filenames: Iterable[str]) -> None:
    print(f"Copying {src_filename} {len(dst_filenames)} times")

    # TODO(swang): Mark the file path with the number of images contained and
    # don't write again if not needed.
    for dst in dst_filenames:
        print(f"Copying {src_filename} to {dst}")
        shutil.copyfile(src_filename, dst)


def download_single_shard(
    shard_url: str, dst_filename: str, chunk_size_mb: int = 512
) -> None:

    print(f"Downloading single shard from {shard_url} to {dst_filename}")
    with requests.get(shard_url, stream=True) as request:
        assert request.ok, "Downloading shard failed"
        with open(dst_filename, "wb") as dst:
            for chunk in request.iter_content(chunk_size=chunk_size_mb * 1 << 20):
                bytes_written = dst.write(chunk)
                print(f"Wrote {bytes_written / (1 << 20):0.02f} MB to {dst_filename}")


def write_shard(
    output_filename: str, single_record: str, num_images_per_shard: int
) -> None:
    # TODO(swang): Make sure it works in cluster setting. Need to either sync
    # all data files to worker nodes using VM launcher's file_mounts or run
    # data script on each node.
    with tf.python_io.TFRecordWriter(output_filename) as writer:
        for _ in range(num_images_per_shard):
            writer.write(single_record)
    print(f"Done writing {output_filename}", file=sys.stderr)


class ImageCoder(object):
    """Helper class that provides TensorFlow image coding utilities."""

    def __init__(self):
        tf.disable_v2_behavior()

        # Create a single Session to run all image coding calls.
        self._sess = tf.Session()

        # Initializes function that converts PNG to JPEG data.
        self._png_data = tf.placeholder(dtype=tf.string)
        image = tf.image.decode_png(self._png_data, channels=3)
        self._png_to_jpeg = tf.image.encode_jpeg(image, format="rgb", quality=100)

        # Initializes function that converts CMYK JPEG data to RGB JPEG data.
        self._cmyk_data = tf.placeholder(dtype=tf.string)
        image = tf.image.decode_jpeg(self._cmyk_data, channels=0)
        self._cmyk_to_rgb = tf.image.encode_jpeg(image, format="rgb", quality=100)

        # Initializes function that decodes RGB JPEG data.
        self._decode_jpeg_data = tf.placeholder(dtype=tf.string)
        self._decode_jpeg = tf.image.decode_jpeg(self._decode_jpeg_data, channels=3)

    def png_to_jpeg(self, image_data: bytes) -> tf.Tensor:
        """Converts a PNG compressed image to a JPEG Tensor."""
        return self._sess.run(self._png_to_jpeg, feed_dict={self._png_data: image_data})

    def cmyk_to_rgb(self, image_data: bytes) -> tf.Tensor:
        """Converts a CMYK image to RGB Tensor."""
        return self._sess.run(
            self._cmyk_to_rgb, feed_dict={self._cmyk_data: image_data}
        )

    def decode_jpeg(self, image_data: bytes) -> tf.Tensor:
        """Decodes a JPEG image."""
        image = self._sess.run(
            self._decode_jpeg, feed_dict={self._decode_jpeg_data: image_data}
        )
        assert len(image.shape) == 3
        assert image.shape[2] == 3
        return image


def get_single_image(image_url: str) -> bytes:
    r = requests.get(image_url)
    assert r.ok, "Downloading image failed"
    return r.content


def parse_single_image(image_url: str) -> Tuple[bytes, int, int]:
    image_buffer = get_single_image(image_url)

    coder = ImageCoder()
    image = coder.decode_jpeg(image_buffer)
    height, width, _ = image.shape

    return image_buffer, height, width


def create_single_example(image_url: str) -> tf.train.Example:
    image_buffer, height, width = parse_single_image(image_url)

    colorspace = "RGB"
    channels = 3
    image_format = "JPEG"
    label = 0
    synset = "dummy-synset"
    filename = "dummy-filename"

    example = tf.train.Example(
        features=tf.train.Features(
            feature={
                "image/height": _int64_feature(height),
                "image/width": _int64_feature(width),
                "image/colorspace": _bytes_feature(colorspace),
                "image/channels": _int64_feature(channels),
                "image/class/label": _int64_feature(label),
                "image/class/synset": _bytes_feature(synset),
                "image/format": _bytes_feature(image_format),
                "image/filename": _bytes_feature(os.path.basename(filename)),
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


if __name__ == "__main__":
    args = parse_args()

    ray.init()

    def get_num_nodes():
        return len([node for node in ray.nodes() if node["Alive"]])

    num_nodes = get_num_nodes()
    while num_nodes < args.num_nodes:
        print(f"Cluster currently has {num_nodes} nodes, expecting {args.num_nodes}")
        time.sleep(1)
        num_nodes = get_num_nodes()
    assert num_nodes == args.num_nodes

    results = []
    for node in ray.nodes():
        if not node["Alive"]:
            continue
        results.append(
            generate_local_files.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node["NodeID"], soft=False
                )
            ).remote(
                args.num_shards,
                args.num_images_per_shard,
                args.shard_url,
                args.single_image_url,
                args.output_directory,
            )
        )
    ray.get(results)
