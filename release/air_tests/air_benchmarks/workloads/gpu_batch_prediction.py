import click
import time
import json
import os
import numpy as np
import pandas as pd
from io import BytesIO
from typing import List

from PIL import Image
from torchvision import transforms
from torchvision.models import resnet18

import ray
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.train.torch import to_air_checkpoint, TorchPredictor
from ray.train.batch_predictor import BatchPredictor
from ray.data.preprocessors import BatchMapper


# TODO(jiaodong): Remove this once ImageFolder #24641 merges
def convert_to_pandas(byte_item_list: List[bytes]) -> pd.DataFrame:
    """
    Convert input bytes into pandas DataFrame with image column and value of
    TensorArray to prevent serializing ndarray image data.
    """
    images = [
        Image.open(BytesIO(byte_item)).convert("RGB") for byte_item in byte_item_list
    ]
    images = [np.asarray(image) for image in images]

    return pd.DataFrame({"image": TensorArray(images)})


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    User Pytorch code to transform user image. Note we still use pandas as
    intermediate format to hold images as shorthand of python dictionary.
    """
    preprocess = transforms.Compose(
        [
            transforms.ToTensor(),
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    df["image"] = df["image"].map(preprocess)
    df["image"] = df["image"].map(lambda x: x.numpy())
    df["image"] = TensorArray(df["image"])
    return df


@click.command(help="Run Batch prediction on Pytorch ResNet models.")
@click.option("--data-size-gb", type=int, default=1)
def main(data_size_gb: int):
    data_url = f"s3://air-example-data-2/{data_size_gb}G-image-data-synthetic-raw"
    print(f"Running GPU batch prediction with {data_size_gb}GB data from {data_url}")
    start = time.time()
    dataset = ray.data.read_binary_files(paths=data_url)
    # TODO(jiaodong): Remove this once ImageFolder #24641 merges
    dataset = dataset.map_batches(convert_to_pandas)

    model = resnet18(pretrained=True)

    preprocessor = BatchMapper(preprocess)
    ckpt = to_air_checkpoint(model=model, preprocessor=preprocessor)

    predictor = BatchPredictor.from_checkpoint(ckpt, TorchPredictor)
    predictor.predict(dataset, num_gpus_per_worker=1)
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
