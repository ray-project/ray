# This file is adapted from https://github.com/Eventual-Inc/Daft/tree/9da265d8f1e5d5814ae871bed3cee1b0757285f5/benchmarking/ai/image_classification
from __future__ import annotations

import time
import uuid

import daft
from daft import col
import numpy as np
import ray
import torch
from torchvision import transforms
from torchvision.models import ResNet18_Weights, resnet18


NUM_GPU_NODES = 8
INPUT_PATH = "s3://anonymous@ray-example-data/imagenet/metadata_file"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
BATCH_SIZE = 100
IMAGE_DIM = (3, 224, 224)

daft.context.set_runner_ray()


@ray.remote
def warmup():
    pass


# NOTE: On a fresh Ray cluster, it can take a minute or longer to schedule the first
#       task. To ensure benchmarks compare data processing speed and not cluster startup
#       overhead, this code launches a several tasks as warmup.
ray.get([warmup.remote() for _ in range(64)])


weights = ResNet18_Weights.DEFAULT
transform = transforms.Compose([transforms.ToTensor(), weights.transforms()])


@daft.udf(
    return_dtype=daft.DataType.string(),
    concurrency=NUM_GPU_NODES,
    num_gpus=1.0,
    batch_size=BATCH_SIZE,
)
class ResNetModel:
    def __init__(self):
        self.weights = weights
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = resnet18(weights=weights).to(self.device)
        self.model.eval()

    def __call__(self, images):
        if len(images) == 0:
            return []
        torch_batch = torch.from_numpy(np.array(images.to_pylist())).to(self.device)
        with torch.inference_mode():
            prediction = self.model(torch_batch)
            predicted_classes = prediction.argmax(dim=1).detach().cpu()
            predicted_labels = [
                self.weights.meta["categories"][i] for i in predicted_classes
            ]
            return predicted_labels


start_time = time.time()

df = daft.read_parquet(INPUT_PATH)
# NOTE: Limit to the 803,580 images Daft uses in their benchmark.
df = df.limit(803_580)
# NOTE: We need to manually repartition the DataFrame to achieve good performance. This
# code isn't in Daft's benchmark, possibly because their Parquet metadata is
# pre-partitioned. Note we're using `repartition(NUM_GPUS)` instead of
# `into_partitions(NUM_CPUS * 2)` as suggested in Daft's documentation. In our
# experiments, the recommended approach led to OOMs, crashes, and slower performance.
df = df.repartition(NUM_GPU_NODES)
df = df.with_column(
    "decoded_image",
    df["image_url"]
    .url.download()
    .image.decode(on_error="null", mode=daft.ImageMode.RGB),
)
# NOTE: At least one image encounters this error: https://github.com/etemesi254/zune-image/issues/244.
# So, we need to return "null" for errored files and filter them out.
df = df.where(df["decoded_image"].not_null())
df = df.with_column(
    "norm_image",
    df["decoded_image"].apply(
        func=lambda image: transform(image),
        return_dtype=daft.DataType.tensor(
            dtype=daft.DataType.float32(), shape=IMAGE_DIM
        ),
    ),
)
df = df.with_column("label", ResNetModel(col("norm_image")))
df = df.select("image_url", "label")
df.write_parquet(OUTPUT_PATH)

print("Runtime:", time.time() - start_time)
