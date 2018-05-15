from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

import argparse
import numpy as np
import tensorflow as tf

from test_model import TFBenchModel
from sgd import DistributedSGD


if __name__ == "__main__":
    ray.init()
    
    model_creator = (
        lambda i, j: TFBenchModel(batch=1, use_cpus=True))

    sgd = DistributedSGD(
        model_creator, num_workers=2, devices_per_worker=2, use_cpus=True)

    for _ in range(100):
        loss = sgd.step()
        print("Current loss", loss)
