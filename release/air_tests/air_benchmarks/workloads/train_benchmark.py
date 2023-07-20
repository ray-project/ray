import ray
from ray.air import session
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig


import time

import torch
import torchvision

DEFAULT_IMAGE_SIZE = 224


def iterate(dataset, label, metrics):
    start = time.time()
    it = iter(dataset)
    num_rows = 0
    for batch in it:
        num_rows += len(batch)
    end = time.time()
    print(label, end - start, "epoch", i)

    tput = num_rows / (end - start)
    metrics[label] = tput


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
        + [torchvision.transforms.ToTensor()]
        if to_torch_tensor
        else []
    )
    return transform


def crop_and_flip_image_batch(image_batch):
    transform = get_transform(False)
    batch_size, height, width, channels = image_batch["image"].shape
    tensor_shape = (batch_size, channels, height, width)
    image_batch["image"] = transform(
        torch.Tensor(image_batch["image"].reshape(tensor_shape))
    )
    return image_batch


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-root",
        default="s3://air-cuj-imagenet-1gb",
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
        default=2,
        type=int,
        help="Number of epochs to run. The throughput for the last epoch will be kept.",
    )
    args = parser.parse_args()

    metrics = {}
    ray_dataset = ray.data.read_images(args.data_root).map_batches(
        crop_and_flip_image_batch
    )
    for i in range(args.num_epochs):
        iterate(
            ray_dataset.iter_torch_batches(batch_size=args.batch_size),
            "ray.data+transform",
            metrics,
        )

    def train_loop_per_worker():
        # Get an iterator to the dataset we passed in below.
        it = session.get_dataset_shard("train")

        # Train for 10 epochs over the data. We'll use a shuffle buffer size
        # of 10k elements, and prefetch up to 10 batches of size 128 each.
        for _ in range(10):
            for batch in it.iter_batches(
                local_shuffle_buffer_size=10000, batch_size=128, prefetch_batches=10
            ):
                pass

    start_t = time.time()
    torch_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": ray_dataset},
    )
    torch_trainer.fit()
    end_t = time.time()
    metrics["ray.torchtrainer.fit"] = end_t - start_t
