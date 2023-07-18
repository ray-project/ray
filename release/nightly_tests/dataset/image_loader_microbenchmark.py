import ray
import torch
import torchvision
import os
import time
import json


DEFAULT_IMAGE_SIZE = 224
DIR = "/home/ray/data"


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


if __name__ == "__main__":
    metrics = []

    def iterate(dataset, label):
        start = time.time()
        it = iter(dataset)
        num_rows = 0
        for batch in it:
            num_rows += len(batch)
        end = time.time()
        print(label, end - start, "epoch", i)

        tput = num_rows / (end - start)
        metrics.append(
            {
                "perf_metric_name": label,
                "perf_metric_value": tput,
                "perf_metric_type": "THROUGHPUT",
            }
        )

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

    torch_dataset = build_torch_dataset(DIR, 32, transform=get_transform(True))
    for i in range(3):
        iterate(torch_dataset, "torch+transform")
    torch_dataset = build_torch_dataset(
        DIR, 32, transform=torchvision.transforms.ToTensor()
    )
    for i in range(3):
        iterate(torch_dataset, "torch")

    ray_dataset = ray.data.read_images(DIR).map_batches(crop_and_flip_image_batch)
    for i in range(3):
        iterate(ray_dataset.iter_torch_batches(batch_size=32), "ray.data+transform")

    ray_dataset = ray.data.read_images(DIR).map_batches(
        crop_and_flip_image_batch, zero_copy_batch=True
    )
    for i in range(3):
        iterate(
            ray_dataset.iter_torch_batches(batch_size=32), "ray.data+transform+zerocopy"
        )

    ray_dataset = ray.data.read_images(DIR)
    for i in range(3):
        iterate(ray_dataset.iter_torch_batches(batch_size=None), "ray.data")

    ray_dataset = ray.data.read_images(DIR).map_batches(
        lambda x: x, batch_format="pyarrow", batch_size=None
    )
    for i in range(3):
        iterate(
            ray_dataset.iter_torch_batches(batch_size=None),
            "ray.data+dummy_pyarrow_transform",
        )

    ray_dataset = ray.data.read_images(DIR).map_batches(
        lambda x: x, batch_format="numpy", batch_size=None
    )
    for i in range(3):
        iterate(
            ray_dataset.iter_torch_batches(batch_size=None),
            "ray.data+dummy_np_transform",
        )

    # Test manual loading using map_batches on the pathnames.
    def load(batch):
        batch["image"] = [torchvision.io.read_image(path) for path in batch["image"]]
        return batch

    paths = [os.path.join(DIR, path) for path in os.listdir(DIR)]
    ray_dataset = ray.data.from_items([{"image": path} for path in paths]).map_batches(
        load
    )
    for i in range(3):
        iterate(ray_dataset.iter_torch_batches(batch_size=32), "ray.data_manual_load")

    result_dict = {
        "perf_metrics": metrics,
        "success": 1,
    }

    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/image_loader_microbenchmark.json"
    )

    with open(test_output_json, "wt") as f:
        json.dump(result_dict, f)
