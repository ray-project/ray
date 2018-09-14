from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

import argparse
import numpy as np
import tensorflow as tf

from ray.experimental.sgd.example.test_model import TFBenchModel
from ray.experimental.sgd.sgd import DistributedSGD

if __name__ == "__main__":
    ray.init()

    model_creator = (
        lambda worker_idx, device_idx: TFBenchModel(batch=1, use_cpus=True))

    sgd = DistributedSGD(
        model_creator, num_workers=2, devices_per_worker=2, use_cpus=True)

    for _ in range(100):
        loss = sgd.step()
        print("Current loss", loss)
