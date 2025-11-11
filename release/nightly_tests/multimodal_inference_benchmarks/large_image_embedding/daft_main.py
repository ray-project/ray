import time
import uuid

import numpy as np
from pybase64 import b64decode
import ray
import torch
from transformers import ViTImageProcessor, ViTForImageClassification

from daft import DataType, udf
import daft


BATCH_SIZE = 1024

INPUT_PREFIX = "s3://anonymous@ray-example-data/image-datasets/10TiB-b64encoded-images-in-parquet-v3/"
OUTPUT_PREFIX = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"

PROCESSOR = ViTImageProcessor(
    do_convert_rgb=None,
    do_normalize=True,
    do_rescale=True,
    do_resize=True,
    image_mean=[0.5, 0.5, 0.5],
    image_std=[0.5, 0.5, 0.5],
    resample=2,
    rescale_factor=0.00392156862745098,
    size={"height": 224, "width": 224},
)


daft.context.set_runner_ray()


@ray.remote
def warmup():
    pass


# NOTE: On a fresh Ray cluster, it can take a minute or longer to schedule the first
#       task. To ensure benchmarks compare data processing speed and not cluster startup
#       overhead, this code launches a several tasks as warmup.
ray.get([warmup.remote() for _ in range(64)])


def decode(data: bytes) -> bytes:
    decoded_data = b64decode(data, None, True)
    return decoded_data


def preprocess(image):
    outputs = PROCESSOR(images=image)["pixel_values"]
    assert len(outputs) == 1, type(outputs)
    return outputs[0]


@udf(
    return_dtype=DataType.tensor(DataType.float32()),
    batch_size=BATCH_SIZE,
    num_gpus=1,
    concurrency=40,
)
class Infer:
    def __init__(self):
        self._device = "cuda" if torch.cuda.is_available() else "cpu"
        self._model = ViTForImageClassification.from_pretrained(
            "google/vit-base-patch16-224"
        ).to(self._device)

    def __call__(self, image_column) -> np.ndarray:
        image_ndarray = np.array(image_column.to_pylist())
        with torch.inference_mode():
            next_tensor = torch.from_numpy(image_ndarray).to(
                dtype=torch.float32, device=self._device, non_blocking=True
            )
            output = self._model(next_tensor).logits
            return output.cpu().detach().numpy()


start_time = time.time()

df = daft.read_parquet(INPUT_PREFIX)
df = df.with_column("image", df["image"].apply(decode, return_dtype=DataType.binary()))
df = df.with_column("image", df["image"].image.decode(mode=daft.ImageMode.RGB))
df = df.with_column("height", df["image"].image_height())
df = df.with_column("width", df["image"].image.width())
df = df.with_column(
    "image",
    df["image"].apply(preprocess, return_dtype=DataType.tensor(DataType.float32())),
)
df = df.with_column("embeddings", Infer(df["image"]))
df = df.select("embeddings")
df.write_parquet(OUTPUT_PREFIX)

print("Runtime", time.time() - start_time)
