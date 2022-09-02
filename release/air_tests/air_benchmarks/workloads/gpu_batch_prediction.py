import click
import time
import json
import os
import pandas as pd

from torchvision import transforms
from torchvision.models import resnet18

import ray
from ray.train.torch import TorchCheckpoint, TorchPredictor
from ray.train.batch_predictor import BatchPredictor
from ray.data.preprocessors import BatchMapper


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    User Pytorch code to transform user image.
    """
    preprocess = transforms.Compose(
        [
            transforms.ToTensor(),
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    df.loc[:, "image"] = [preprocess(image).numpy() for image in df["image"]]
    return df


@click.command(help="Run Batch prediction on Pytorch ResNet models.")
@click.option("--data-size-gb", type=int, default=1)
def main(data_size_gb: int):
    data_url = f"s3://air-example-data-2/{data_size_gb}G-image-data-synthetic-raw"
    print(f"Running GPU batch prediction with {data_size_gb}GB data from {data_url}")
    start = time.time()
    dataset = ray.data.read_images(root=data_url, size=(256, 256))

    model = resnet18(pretrained=True)

    preprocessor = BatchMapper(preprocess)
    ckpt = TorchCheckpoint.from_model(model=model, preprocessor=preprocessor)

    predictor = BatchPredictor.from_checkpoint(ckpt, TorchPredictor)
    predictor.predict(dataset, num_gpus_per_worker=1, feature_columns=["image"])
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
