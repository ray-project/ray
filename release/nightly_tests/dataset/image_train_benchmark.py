import ray
from ray.air import session
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig

import time
import os
import json
import torchvision
import torch

# This benchmark does the following:
# 1) Read images with ray.data.read_images()
# 2) Apply preprocessing with map_batches()
# 3) Train TorchTrainer on processed data
# Metrics recorded to the output file are:
# - ray.torchtrainer.fit: Throughput of the final epoch in
#   TorchTrainer.fit() (step 3 above)


# Constants and utility methods for image-based benchmarks.
DEFAULT_IMAGE_SIZE = 224


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
        help="Directory path with files.",
    )
    parser.add_argument(
        "--batch-size",
        default=32,
        type=int,
        help="Batch size to use.",
    )
    parser.add_argument(
        "--num-epochs",
        # Use 2 epochs and report the throughput of the last epoch, in case
        # there is warmup in the first epoch.
        default=2,
        type=int,
        help="Number of epochs to run. The throughput for the last epoch will be kept.",
    )
    parser.add_argument(
        "--num-workers",
        default=1,
        type=int,
        help="Number of workers.",
    )
    args = parser.parse_args()

    metrics = {}

    ray_dataset = (
        # 1) Read in data with read_images()
        ray.data.read_images(args.data_root)
        # 2) Preprocess data by applying transformation with map_batches()
        .map_batches(crop_and_flip_image_batch)
    )

    def train_loop_per_worker():
        it = session.get_dataset_shard("train")

        for i in range(args.num_epochs):
            num_rows = 0
            start_t = time.time()
            for batch in it.iter_batches(
                batch_size=args.batch_size, prefetch_batches=10
            ):
                num_rows += len(batch)
            end_t = time.time()
            # Record throughput per epoch.
            epoch_tput = num_rows / (end_t - start_t)
            session.report({"tput": epoch_tput, "epoch": i})

    # 3) Train TorchTrainer on processed data
    torch_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=args.num_workers),
        datasets={"train": ray_dataset},
    )
    result = torch_trainer.fit()

    # Report the throughput of the last training epoch.
    metrics["ray.TorchTrainer.fit"] = list(result.metrics_dataframe["tput"])[-1]

    # Gather up collected metrics, and write to output JSON file.
    metrics_list = []
    for label, tput in metrics.items():
        metrics_list.append(
            {
                "perf_metric_name": label,
                "perf_metric_value": tput,
                "perf_metric_type": "THROUGHPUT",
            }
        )
    test_name = f"read_images_train{args.num_workers}_cpu"
    result_dict = {
        test_name: metrics_list,
        "success": 1,
    }

    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/train_torch_image_benchmark.json"
    )

    with open(test_output_json, "wt") as f:
        json.dump(result_dict, f)

    print(f"Finished benchmark, metrics exported to {test_output_json}:")
    print(metrics)
