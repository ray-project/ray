import gc
import json
import os
import time
from typing import Callable
import torch
import torchvision

from ray.data.dataset import Dataset


class Benchmark:
    """Benchmark class used for Ray Datasets.

    Call ``run(fn)`` to benchmark a specific piece of code/function.
    ``fn`` is expected to return the final Dataset. Benchmark ensures
    final Dataset is fully executed. Plan to add Dataset statistics
    logging in the future.

    Call ``write_result()`` to write benchmark result in file.
    Result can be rendered in dashboard later through other tool.
    We should use this class for any benchmark related to Ray Datasets.
    It works for both local and distribute benchmarking.

    A typical workflow would be:

    benchmark = Benchmark(...)

    # set up (such as input read or generation)
    ...

    benchmark.run(..., fn_1)
    benchmark.run(..., fn_2)
    benchmark.run(..., fn_3)

    benchmark.write_result()

    See example usage in ``aggregate_benchmark.py``.
    """

    def __init__(self, name):
        self.name = name
        self.result = {}
        print(f"Running benchmark: {name}")

    def run(self, name: str, fn: Callable[..., Dataset], **fn_run_args):
        gc.collect()

        print(f"Running case: {name}")
        start_time = time.perf_counter()
        output_ds = fn(**fn_run_args)
        output_ds.materialize()
        duration = time.perf_counter() - start_time

        # TODO(chengsu): Record more metrics based on dataset stats.
        self.result[name] = {"time": duration}
        print(f"Result of case {name}: {self.result[name]}")

    def write_result(self):
        test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
        with open(test_output_json, "w") as f:
            f.write(json.dumps(self.result))
        print(f"Finish benchmark: {self.name}")


def iterate(dataset, label, metrics):
    """ Iterate over `dataset`, calculating the throughput and 
        adds it into the `metrics` dict under the `label` key."""
    start = time.time()
    it = iter(dataset)
    num_rows = 0
    for batch in it:
        num_rows += len(batch)
    end = time.time()

    tput = num_rows / (end - start)
    metrics[label] = tput


# Constants and utility methods for image-based benchmarks.
DEFAULT_IMAGE_SIZE = 224

def get_transform(to_torch_tensor, image_size=DEFAULT_IMAGE_SIZE):
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
