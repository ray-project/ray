from io import BytesIO
from PIL import Image

import torch
from torchvision import transforms
from torchvision.models import resnet50

import ray
import boto3
import json
import time
import os
from tqdm import tqdm
import numpy as np


class Preprocessor:
    def __init__(self):
        self.torch_transform = transforms.Compose(
            [
                transforms.Resize(224),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Lambda(lambda t: t[:3, ...]),  # remove alpha channel
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )

    def __call__(self, img_bytes):
        try:
            img = Image.open(BytesIO(img_bytes)).convert("RGB")
            tensor = self.torch_transform(img)
            return tensor
        except Exception as e:
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


def get_paths(bucket, path, max_files=100 * 1000):
    s3 = boto3.resource("s3")
    s3_objects = s3.Bucket(bucket).objects.filter(Prefix=path).limit(max_files).all()
    materialized = [(obj.bucket_name, obj.key) for obj in tqdm(s3_objects)]
    return materialized


def preprocess(batch):
    preprocessor = Preprocessor()
    return preprocessor(batch)


infer_initialized = False
model_fn = None


def infer(batch):
    global infer_initialized, model_fn
    if not infer_initialized:
        infer_initialized = True
        model_fn = ImageModel()
    ndarr_obj = batch.values
    input_tensor_np = np.array([img.numpy() for img in ndarr_obj.reshape(-1)])
    return list(model_fn(input_tensor_np))


ray.init()

start_time = time.time()

print("Downloading...")
ds = ray.data.read_binary_files(
    "s3://anyscale-data/small-images/",
    parallelism=1000,
    ray_remote_args={"num_cpus": 0.5},
)
# Do a blocking map so that we can measure the download time.
ds = ds.map(lambda x: x)

end_download_time = time.time()
print("Preprocessing...")
ds = ds.map(preprocess)
end_preprocess_time = time.time()
print("Inferring...")
# NOTE: set a small batch size to avoid OOM on GRAM when doing inference.
ds = ds.map_batches(
    infer, num_gpus=0.25, batch_size=128, batch_format="pandas", compute="actors"
)

end_time = time.time()

download_time = end_download_time - start_time
preprocess_time = end_preprocess_time - end_download_time
infer_time = end_time - end_preprocess_time
total_time = end_time - start_time

print("Download time", download_time)
print("Preprocess time", preprocess_time)
print("Infer time", infer_time)

print("total time", total_time)

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "download_time": download_time,
        "preprocess_time": preprocess_time,
        "inference_time": infer_time,
        "total_time": total_time,
    }
    json.dump(results, out_file)
