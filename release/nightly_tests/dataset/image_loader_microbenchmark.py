from collections import defaultdict
import ray
import torch
import torchvision
import os
import time
import json
import tensorflow as tf
import numpy as np


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
    shape = tf.shape(image_buffer)[1:]
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


def get_transform(to_torch_tensor):
    # Note(swang): This is a different order from tf.data.
    # torch: decode -> randCrop+resize -> randFlip
    # tf.data: decode -> randCrop -> randFlip -> resize
    transform = torchvision.transforms.Compose(
        [
            torchvision.transforms.RandomResizedCrop(
                size=DEFAULT_IMAGE_SIZE,
                scale=(0.05, 1.0),
                ratio=(0.75, 1.33),
            ),
            torchvision.transforms.RandomHorizontalFlip(),
        ]
        + ([torchvision.transforms.ToTensor()] if to_torch_tensor else [])
    )
    return transform


def crop_and_flip_image_batch(image_batch):
    transform = get_transform(False)
    image_batch["image"] = transform(
        # Make sure to use torch.tensor here to avoid a copy from numpy.
        # Original dims are (batch_size, channels, height, width).
        torch.tensor(np.transpose(image_batch["image"], axes=(0, 3, 1, 2)))
    )
    return image_batch

def crop_and_flip_image(row):
    transform = get_transform(False)
    # Make sure to use torch.tensor here to avoid a copy from numpy.
    row["image"] = transform(torch.tensor(np.transpose(row["image"], axes=(2, 0, 1))))
    return row



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-root",
        default="/home/ray/imagenet-1gb-data",
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

    tf_dataset = tf.keras.preprocessing.image_dataset_from_directory(
        args.data_root, batch_size=args.batch_size, image_size=FULL_IMAGE_SIZE
    )
    for i in range(args.num_epochs):
        iterate(tf_dataset, "tf_data", args.batch_size, metrics)
    tf_dataset = tf_dataset.map(lambda img, label: (tf_crop_and_flip(img), label))
    for i in range(args.num_epochs):
        iterate(tf_dataset, "tf_data+transform", args.batch_size, metrics)

    torch_dataset = build_torch_dataset(
        args.data_root, args.batch_size, transform=torchvision.transforms.ToTensor()
    )
    for i in range(args.num_epochs):
        iterate(torch_dataset, "torch", args.batch_size, metrics)
    torch_dataset = build_torch_dataset(
        args.data_root, args.batch_size, transform=get_transform(True)
    )
    for i in range(args.num_epochs):
        iterate(torch_dataset, "torch+transform", args.batch_size, metrics)

    ray_dataset = ray.data.read_images(args.data_root).map(crop_and_flip_image)
    for i in range(args.num_epochs):
        iterate(
            ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            "ray_data+map_transform",
            args.batch_size,
            metrics,
        )

    ray_dataset = ray.data.read_images(args.data_root).map_batches(
        crop_and_flip_image_batch
    )
    for i in range(args.num_epochs):
        iterate(
            ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            "ray_data+transform",
            args.batch_size,
            metrics,
        )

    ray_dataset = ray.data.read_images(args.data_root).map_batches(
        crop_and_flip_image_batch, zero_copy_batch=True
    )
    for i in range(args.num_epochs):
        iterate(
            ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            "ray_data+transform+zerocopy",
            args.batch_size,
            metrics,
        )

    ray_dataset = ray.data.read_images(args.data_root)
    for i in range(args.num_epochs):
        iterate(
            ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            "ray_data",
            args.batch_size,
            metrics,
        )

    # Test manual loading using map_batches on the pathnames.
    def load(batch):
        batch["image"] = [torchvision.io.read_image(path) for path in batch["image"]]
        return batch

    paths = []
    for subdir in os.listdir(args.data_root):
        paths += [
            os.path.join(args.data_root, subdir, basename)
            for basename in os.listdir(os.path.join(args.data_root, subdir))
        ]
    ray_dataset = ray.data.from_items([{"image": path} for path in paths]).map_batches(
        load
    )
    for i in range(args.num_epochs):
        iterate(
            ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            "ray_data_manual_load",
            args.batch_size,
            metrics,
        )

    metrics_dict = defaultdict(dict)
    for label, tput in metrics.items():
        metrics_dict[label].update({"THROUGHPUT": tput})

    result_dict = {
        "perf_metrics": metrics_dict,
        "success": 1,
    }

    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/image_loader_microbenchmark.json"
    )

    with open(test_output_json, "wt") as f:
        json.dump(result_dict, f)

    print(f"Finished benchmark, metrics exported to {test_output_json}.")
