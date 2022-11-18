import click
import time
import json
import os
import numpy as np

import torch
from torchvision import transforms
from torchvision.models import resnet18

import ray
from ray.train.torch import TorchCheckpoint, TorchPredictor
from ray.train.batch_predictor import BatchPredictor
from ray.data.preprocessors import BatchMapper


def preprocess(image_batch: np.ndarray) -> np.ndarray:
    """
    User Pytorch code to transform user image with outer dimension of batch size.
    """
    preprocess = transforms.Compose(
        [
            # Torchvision's ToTensor does not accept outer batch dimension
            transforms.CenterCrop(224),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    # Outer dimension is batch size such as (10, 256, 256, 3) -> (10, 3, 256, 256)
    transposed_torch_tensor = torch.Tensor(image_batch.transpose(0, 3, 1, 2))
    return preprocess(transposed_torch_tensor).numpy()


@click.command(help="Run Batch prediction on Pytorch ResNet models.")
@click.option("--data-size-gb", type=int, default=1)
@click.option("--smoke-test", is_flag=True, default=False)
def main(data_size_gb: int, smoke_test: bool = False):
    data_url = (
        f"s3://anonymous@air-example-data-2/{data_size_gb}G-image-data-synthetic-raw"
    )

    if smoke_test:
        # Only read one image
        data_url = [data_url + "/dog.jpg"]
        print("Running smoke test on CPU with a single example")
    else:
        print(
            f"Running GPU batch prediction with {data_size_gb}GB data from {data_url}"
        )

    start = time.time()
    dataset = ray.data.read_images(data_url, size=(256, 256))

    model = resnet18(pretrained=True)

    preprocessor = BatchMapper(preprocess, batch_format="numpy")
    ckpt = TorchCheckpoint.from_model(model=model, preprocessor=preprocessor)

    predictor = BatchPredictor.from_checkpoint(ckpt, TorchPredictor)
    predictor.predict(
        dataset,
        num_gpus_per_worker=int(not smoke_test),
        batch_size=512,
    )
    total_time_s = round(time.time() - start, 2)

    # For structured output integration with internal tooling
    results = {
        "data_size_gb": data_size_gb,
    }
    results["perf_metrics"] = [
        {
            "perf_metric_name": "total_time_s",
            "perf_metric_value": total_time_s,
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": "throughout_MB_s",
            "perf_metric_value": (data_size_gb * 1024 / total_time_s),
            "perf_metric_type": "THROUGHPUT",
        },
    ]

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    print(results)


if __name__ == "__main__":
    main()
