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
import torch
import torch.nn as nn
import torch.optim as optim

import ray
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.train.torch import to_air_checkpoint
from ray.data.preprocessors import BatchMapper
from ray import train
from ray.air import session
from ray.train.torch import TorchTrainer


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
    # Dummy label since we're only testing training throughput
    labels = [1 for _ in range(len(images))]

    return pd.DataFrame({"image": TensorArray(images), "label": labels})


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


def train_loop_per_worker(config):
    raw_model = resnet18(pretrained=True)
    model = train.torch.prepare_model(raw_model)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(model.parameters(), lr=0.001, momentum=0.9)

    train_dataset_shard = session.get_dataset_shard("train")

    for epoch in range(config["num_epochs"]):
        running_loss = 0.0
        for i, data in enumerate(
            train_dataset_shard.iter_batches(
                batch_size=config["batch_size"], batch_format="numpy"
            )
        ):
            # get the inputs; data is a list of [inputs, labels]
            inputs = torch.as_tensor(data["image"], dtype=torch.float32).to(
                device="cuda"
            )
            labels = torch.as_tensor(data["label"], dtype=torch.int64).to(device="cuda")
            # zero the parameter gradients
            optimizer.zero_grad()

            # forward + backward + optimize
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

            # print statistics
            running_loss += loss.item()
            if i % 2000 == 1999:  # print every 2000 mini-batches
                print(f"[{epoch + 1}, {i + 1:5d}] loss: {running_loss / 2000:.3f}")
                running_loss = 0.0

        session.report(
            dict(running_loss=running_loss),
            checkpoint=to_air_checkpoint(model),
        )


@click.command(help="Run Batch prediction on Pytorch ResNet models.")
@click.option("--data-size-gb", type=int, default=1)
@click.option("--num-epochs", type=int, default=10)
def main(data_size_gb: int, num_epochs=10):
    data_url = f"s3://air-example-data-2/{data_size_gb}G-image-data-synthetic-raw"
    print(
        "Running Pytorch image model training with "
        f"{data_size_gb}GB data from {data_url}"
    )
    print(f"Training for {num_epochs} epochs.")
    start = time.time()
    dataset = ray.data.read_binary_files(paths=data_url)
    # TODO(jiaodong): Remove this once ImageFolder #24641 merges
    dataset = dataset.map_batches(convert_to_pandas)

    preprocessor = BatchMapper(preprocess)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={"batch_size": 64, "num_epochs": num_epochs},
        datasets={"train": dataset},
        preprocessor=preprocessor,
        scaling_config={"num_workers": 1, "use_gpu": True},
    )
    trainer.fit()

    total_time_s = round(time.time() - start, 2)

    # For structured output integration with internal tooling
    results = {"data_size_gb": data_size_gb, "num_epochs": num_epochs}
    results["perf_metrics"] = [
        {
            "perf_metric_name": "total_time_s",
            "perf_metric_value": total_time_s,
            "perf_metric_type": "LATENCY",
        },
        {
            "perf_metric_name": "throughout_MB_s",
            "perf_metric_value": round(
                num_epochs * data_size_gb * 1024 / total_time_s, 2
            ),
            "perf_metric_type": "THROUGHPUT",
        },
    ]

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    print(results)


if __name__ == "__main__":
    main()
