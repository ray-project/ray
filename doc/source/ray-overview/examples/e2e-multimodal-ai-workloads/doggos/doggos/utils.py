import os
import random
import re
from io import BytesIO

import boto3
import numpy as np
import requests
import torch
from PIL import Image


def add_class(row):
    row["class"] = row["path"].rsplit("/", 3)[-2]
    return row


def delete_s3_objects(s3_path):
    s3 = boto3.client("s3")
    match = re.match(r"s3://([^/]+)/(.+)", s3_path)
    if not match:
        raise ValueError(f"Invalid S3 path: {s3_path}")
    bucket_name, prefix = match.groups()
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in response:  # No objects found
            print(f"No objects found at {s3_path}")
            return
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": objects_to_delete},
        )
        print(f"Deleted {len(objects_to_delete)} objects from {s3_path}")
    except s3.exceptions.NoSuchBucket:
        print(f"Bucket '{bucket_name}' does not exist.")
    except Exception as e:
        print(f"Error deleting objects from {s3_path}: {e}")


def set_seeds(seed=42):
    """Set seeds for reproducibility."""
    np.random.seed(seed)
    random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    eval("setattr(torch.backends.cudnn, 'deterministic', True)")
    eval("setattr(torch.backends.cudnn, 'benchmark', False)")
    os.environ["PYTHONHASHSEED"] = str(seed)


def url_to_array(url):
    return np.array(Image.open(BytesIO(requests.get(url).content)).convert("RGB"))
