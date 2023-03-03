import boto3
from pathlib import Path
import os
import tensorflow_datasets as tfds


def download_dataset_from_s3(destination_dir: str):
    destination_path = Path(destination_dir).resolve()
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket("air-example-data")
    for obj in bucket.objects.filter(Prefix="food-101-tiny"):
        os.makedirs(os.path.dirname(obj.key), exist_ok=True)
        bucket.download_file(obj.key, str(destination_path / obj.key))


def get_datasets(global_batch_size: int, batch_size_per_worker: int) -> dict:
    IMG_SIZE = 224

    if not os.path.exists("./food-101-tiny"):
        download_dataset_from_s3(destination_dir=".")

    builder = tfds.ImageFolder("./food-101-tiny")
    train_ds = builder.as_dataset(
        split="train", shuffle_files=False, batch_size=global_batch_size
    )
    train_ds = train_ds.map(
        lambda data: (
            tf.image.resize(data["image"], (IMG_SIZE, IMG_SIZE)),
            data["label"],
        )
    )
    valid_ds = builder.as_dataset(
        split="valid", shuffle_files=True, batch_size=global_batch_size
    )
    valid_ds = valid_ds.map(
        lambda data: (
            tf.image.resize(data["image"], (IMG_SIZE, IMG_SIZE)),
            data["label"],
        )
    )
    return {"train": train_ds, "valid": valid_ds}
