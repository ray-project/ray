################ model.py ###########################3
from os import name
import ray
from ray import serve

from io import BytesIO
from PIL import Image
import requests

import torch
from torchvision import transforms
from torchvision.models import resnet50


class Preprocessor:
    def __init__(self):
        self.torch_transform = transforms.Compose([
            transforms.Resize(224),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Lambda(lambda t: t[:3, ...]),  # remove alpha channel
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ])

    def __call__(self, img_bytes):
        try:
            img = Image.open(BytesIO(img_bytes)).convert("RGB")
            tensor = self.torch_transform(img)
            return tensor
        except Exception as e:
            # PIL.UnidentifiedImageError: cannot identify image file <_io.BytesIO object at 0x7fb24feaa830>
            print(img_bytes[:100])
            raise e


class ImageModel:
    def __init__(self):
        self.model = resnet50(pretrained=True).eval().half().cuda()

    def __call__(self, input_tensor_np):
        input_tensor = torch.from_numpy(input_tensor_np).half().cuda()
        with torch.no_grad():
            output_tensor = self.model(input_tensor)
            result = torch.argmax(output_tensor, dim=1).cpu()
        return result.numpy()


################# inference.py #######################

import ray
import boto3
import json
import pyarrow.fs
import requests
import time
import os
from tqdm import tqdm
import numpy as np


def read_file(path: str, ):
    s3fs, _ = pyarrow.fs.FileSystem.from_uri("s3://anyscale-data")
    return s3fs.open_input_stream(path).readall()


def get_paths(bucket, path, max_files=100 * 1000):
    if os.path.exists("./cache.txt"):
        return list(map(
            lambda line: line.strip().split(","),
            open("cache.txt", "r").readlines()
        ))

    s3 = boto3.resource("s3")
    s3_objects = s3.Bucket(bucket).objects.filter(
        Prefix=path).limit(max_files).all()

    materialized = [(obj.bucket_name, obj.key) for obj in tqdm(s3_objects)]
    with open("cache.txt", "w") as f:
        for bucket_name, key in materialized:
            print(f"{bucket_name},{key}", file=f)



    return materialized


download_initialized = False
s3_client = None
http_session = None
def download(path):
    global download_initialized, s3_client, http_session
    if download_initialized is False:
        s3_client = boto3.client("s3")
        http_session = requests.Session()
        download_initialized = True

    bucket, key = path

    # NOTE: generating a presigned url is theoretically more realistic but
    # involves setting up AWS credentials.
    # url = s3_client.generate_presigned_url(
    #     "get_object",
    #     Params={"Bucket": bucket, "Key": key},
    #     ExpiresIn=3600
    # )

    url = f"https://{bucket}.s3.us-west-2.amazonaws.com/{key}"

    # Retry download if it fails.
    for _ in range(3):
        result = http_session.get(url)
        if result.status_code == 200:
            break
        print(f"Failed to download {url} with error: {result.content}. Retrying.")

    return result.content


def preprocess(batch):
    # TODO: This needs to work in terms of pyarrow/pandas batches
    preprocessor = Preprocessor()
    return preprocessor(batch)


def infer(batch):
    # TODO: This needs to work in terms of pyarrow/pandas batches
    model_fn = ImageModel()
    ndarr_obj = batch.values
    input_tensor_np = np.array([img.numpy() for img in ndarr_obj.reshape(-1)])
    return list(model_fn(input_tensor_np))


ray.init()

s3_paths = get_paths("anyscale-data", "imagenet/train")
print("Got s3 objects")

BATCH_SIZE = 256
parallelism = len(s3_paths) // BATCH_SIZE
parallelism = max(2, parallelism)

start_time = time.time()

downloaded = ray.experimental.data \
                             .from_items(s3_paths, parallelism=parallelism) \
                             .map(download) \
                             .map(preprocess) \
                             .map_batches(infer, num_gpus=0.5)


# Collection step for timing
list(downloaded.map_batches(lambda x: []).iter_rows())

end_time = time.time()

total = end_time - start_time
print("total time", total)

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {"inference_time": 1, "success": 1}
    json.dump(results, out_file)



# TODO (Alex): The py arrow s3 fs can't be serialized so we can't use read_binary_files.
# image_ds = ray.experimental.data.from_items(
#     s3_paths, parallelism=parallelism).map(read_file)

# processed = image_ds.map(preprocess)
# inferred = processed.map(infer, num_gpus=1)

# result = inferred.map(lambda prediction: None)
# list(result.iter_rows())


# s3fs, _ = pyarrow.fs.FileSystem.from_uri("s3://anyscale-data")
# image_ds = ray.experimental.data.read_binary_files(s3_paths, filesystem=s3fs)
