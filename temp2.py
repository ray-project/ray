import numpy as np
import io
import ray
import os
from PIL import Image

dataset = ray.data.read_parquet(os.path.expanduser("~/Datasets/cifar-10/data"))
print(dataset)


def map_fn(row):
    row = dict(row)
    array = row["image"]
    image = Image.fromarray(array)
    stream = io.BytesIO()
    image.save(stream, format="JPEG")
    data = stream.getvalue()
    row["image"] = data
    return row


ds = ray.data.from_items([map_fn(row) for row in dataset.iter_rows()])
