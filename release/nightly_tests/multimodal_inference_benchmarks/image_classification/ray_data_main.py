from __future__ import annotations

import io
import time
import torch
from packaging import version
from PIL import Image
from torchvision import transforms
from torchvision.models import ResNet18_Weights, resnet18
from ray.data.expressions import download
import numpy as np
import uuid
import ray


NUM_GPU_NODES = 8
INPUT_PATH = "s3://anonymous@ray-example-data/imagenet/metadata_file"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
BATCH_SIZE = 100

weights = ResNet18_Weights.DEFAULT
transform = transforms.Compose([transforms.ToTensor(), weights.transforms()])

ray.init()


@ray.remote
def warmup():
    pass


# NOTE: On a fresh Ray cluster, it can take a minute or longer to schedule the first
#       task. To ensure benchmarks compare data processing speed and not cluster startup
#       overhead, this code launches a several tasks as warmup.
ray.get([warmup.remote() for _ in range(64)])


def deserialize_image(row):
    image = Image.open(io.BytesIO(row["bytes"])).convert("RGB")
    # NOTE: Remove the `bytes` column since we don't need it anymore. This is done by
    # the system automatically on Ray Data 2.51+ with the `with_column` API.
    del row["bytes"]
    row["image"] = np.array(image)
    return row


def transform_image(row):
    row["norm_image"] = transform(row["image"]).numpy()
    # NOTE: Remove the `image` column since we don't need it anymore. This is done by
    # the system automatically on Ray Data 2.51+ with the `with_column` API.
    del row["image"]
    return row


class ResNetActor:
    def __init__(self):
        self.weights = weights
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = resnet18(weights=self.weights).to(self.device)
        self.model.eval()

    def __call__(self, batch):
        torch_batch = torch.from_numpy(batch["norm_image"]).to(self.device)
        # NOTE: Remove the `norm_image` column since we don't need it anymore. This is
        # done by the system automatically on Ray Data 2.51+ with the `with_column`
        # API.
        del batch["norm_image"]
        with torch.inference_mode():
            prediction = self.model(torch_batch)
            predicted_classes = prediction.argmax(dim=1).detach().cpu()
            predicted_labels = [
                self.weights.meta["categories"][i] for i in predicted_classes
            ]
            batch["label"] = predicted_labels
            return batch


start_time = time.time()


# You can use `download` on Ray 2.50+.
if version.parse(ray.__version__) > version.parse("2.49.2"):
    ds = (
        ray.data.read_parquet(INPUT_PATH)
        # NOTE: Limit to the 803,580 images Daft uses in their benchmark.
        .limit(803_580)
        .with_column("bytes", download("image_url"))
        .map(fn=deserialize_image)
        .map(fn=transform_image)
        .map_batches(
            fn=ResNetActor,
            batch_size=BATCH_SIZE,
            num_gpus=1.0,
            concurrency=NUM_GPU_NODES,
        )
        .select_columns(["image_url", "label"])
    )
    ds.write_parquet(OUTPUT_PATH)

else:
    # NOTE: Limit to the 803,580 images Daft uses in their benchmark.
    paths = ray.data.read_parquet(INPUT_PATH).limit(803_580).take_all()
    paths = [row["image_url"] for row in paths]
    ds = (
        ray.data.read_images(
            paths, include_paths=True, ignore_missing_paths=True, mode="RGB"
        )
        .map(fn=transform_image)
        .map_batches(
            fn=ResNetActor,
            batch_size=BATCH_SIZE,
            num_gpus=1.0,
            concurrency=NUM_GPU_NODES,
        )
        .select_columns(["path", "label"])
    )
    ds.write_parquet(OUTPUT_PATH)


print("Runtime:", time.time() - start_time)
