import time
import traceback
from io import BytesIO

import numpy as np
import pandas as pd
from PIL import Image
from sqlalchemy import values
from torchvision import transforms

import ray
from ray.air.util.tensor_extensions.pandas import TensorArray


def convert_to_pandas(byte_item_list):
    preprocess = transforms.Compose(
        [
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )

    images = [
        Image.open(BytesIO(byte_item)).convert("RGB") for byte_item in byte_item_list
    ]
    images = [preprocess(image) for image in images]
    images = [np.array(image) for image in images]

    return pd.DataFrame({"image": TensorArray(images)})


node_high_memory_usage_fraction = 0.95
node_high_memory_monitor_min_interval_ms = 100

params_use_oom_killer = [False, True]
params_num_cpus = [2, 4, 6, 8, 10]
params_num_blocks = [25, 50, 100, 200]
params_infinite_retries = [False, True]
params_object_store_memory = [10e9]

use_oom_killer = False
infinite_retries = True
num_cpus = 10
num_blocks = 300
object_store_memory = 10e9
reuse_worker = True

system_config = {}
if use_oom_killer:
    system_config["node_high_memory_usage_fraction"] = node_high_memory_usage_fraction
    system_config[
        "node_high_memory_monitor_min_interval_ms"
    ] = node_high_memory_monitor_min_interval_ms
if not reuse_worker:
    system_config["idle_worker_killing_time_threshold_ms"] = 0
    system_config["num_workers_soft_limit"] = 0
    system_config["enable_worker_prestart"] = False
if len(system_config) > 0:
    ray.init(
        num_cpus=num_cpus,
        object_store_memory=object_store_memory,
        _metrics_export_port=8080,
        _system_config=system_config,
    )
else:
    ray.init(
        num_cpus=num_cpus,
        object_store_memory=object_store_memory,
        _metrics_export_port=8080,
    )

print(
    f"running for use oom? {use_oom_killer} num_cpus {num_cpus} blocks {num_blocks} inf retries {infinite_retries}"
)
for i in range(1):
    start_time = time.time()
    if infinite_retries:
        dataset = ray.data.read_binary_files(
            paths=["/home/ray/raydev_mnt/air-cuj-imagenet-20gb/"],
            parallelism=num_blocks,
            ray_remote_args={"max_retries": -1},
        )
    else:
        dataset = ray.data.read_binary_files(
            paths=["/home/ray/raydev_mnt/air-cuj-imagenet-20gb/"],
            parallelism=num_blocks,
        )
    end_time = time.time()
    print(f"The execution time of read_binary_files is: {end_time-start_time}")

    # start_time = time.time()
    # dataset = dataset.limit(int(dataset.count() / 4))
    # end_time = time.time()
    # print(dataset.is_fully_executed())
    # print(f"The execution time of limit is: {end_time-start_time}")

    start_time = time.time()
    try:
        if infinite_retries:
            dataset = dataset.map_batches(convert_to_pandas, max_retries=-1)
        else:
            dataset = dataset.map_batches(convert_to_pandas)
    except Exception as e:
        print(e)
        print(traceback.format_exc())
    end_time = time.time()
    print(f"The execution time is: {end_time-start_time}")
    print(f"max task rss {dataset.max_rss()} uss {dataset.max_uss()}")

    time.sleep(1000)

    # start_time = time.time()
    # try:
    #     dataset = dataset.map_batches(convert_to_pandas)
    # except Exception as e:
    #     print(e)
    #     print(traceback.format_exc())
    # end_time = time.time()
    # print(f"The execution time of map_batches is: {end_time-start_time}")

    # model = resnet18(pretrained=True)
    # ckpt = to_air_checkpoint(model=model)

    # predictor = BatchPredictor.from_checkpoint(ckpt, TorchPredictor)
    # predictor.predict(dataset, num_gpus_per_worker=1)
