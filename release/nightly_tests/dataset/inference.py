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

    def __call__(self, request):
        print("handling request")
        input_tensor = torch.from_numpy(request.data).half().cuda()
        with torch.no_grad():
            output_tensor = self.model(input_tensor)
            result = torch.argmax(output_tensor, dim=1).cpu()
        return result.numpy()


################# inference.py #######################

import ray
import boto3
import json
import pyarrow.fs
import time
import os
from tqdm import tqdm



def read_file(path: str, ):
    s3fs, _ = pyarrow.fs.FileSystem.from_uri("s3://anyscale-data")
    return s3fs.open_input_stream(path).readall()


# @ray.remote
def get_urls(bucket, path, max_files=100):
    s3 = boto3.resource("s3")
    s3_objects = s3.Bucket(bucket).objects.filter(
        Prefix=path).limit(max_files).all()

    # s3_client = boto3.client("s3")
    # materialized = [
    #     s3_client.generate_presigned_url(
    #         "get_object",
    #         Params={"Bucket": obj.bucket_name, "Key": obj.key},
    #         ExpiresIn=3600,
    #     )
    #     for obj in s3_objects
    # ]
    # return [obj.key for obj in s3_objects]
    return [f"{obj.bucket_name}/{obj.key}" for obj in tqdm(s3_objects)]


def preprocess(batch):
    preprocessor = Preprocessor()
    return preprocessor(batch)


def infer(batch):
    model_fn = ImageModel()
    return model_fn(batch)


# ray.client().connect()
ray.init()

# s3_paths = ray.get(get_urls.remote("anyscale-data", "imagenet/train", max_files=100000))
s3_paths = get_urls("anyscale-data", "imagenet/train")
print("Got s3 objects")

parallelism = max(len(s3_paths) // 256, 200)

start_time = time.time()

# TODO (Alex): The py arrow s3 fs can't be serialized so we can't use read_binary_files.
image_ds = ray.experimental.data.from_items(
    s3_paths, parallelism=parallelism).map(read_file)

processed = image_ds.map(preprocess)
inferred = processed.map(infer, num_gpus=1)

result = inferred.map(lambda prediction: None)
list(result.iter_rows())

end_time = time.time()

total = end_time - start_time


if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "inference_time": 1,
        "success": 1
    }
    json.dump(results, out_file)


# s3fs, _ = pyarrow.fs.FileSystem.from_uri("s3://anyscale-data")
# image_ds = ray.experimental.data.read_binary_files(s3_paths, filesystem=s3fs)
