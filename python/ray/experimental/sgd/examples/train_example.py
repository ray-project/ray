from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import pytest
import tempfile
import torch
import torch.distributed as dist

from ray.tests.conftest import ray_start_2_cpus  # noqa: F401
from ray.experimental.sgd.pytorch import PyTorchTrainer, Resources

from ray.experimental.sgd.tests.pytorch_utils import (
    model_creator, optimizer_creator, data_creator)

def train_example(num_replicas):
    trainer1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        num_replicas=num_replicas)
    trainer1.train()

    filename = os.path.join(tempfile.mkdtemp(), "checkpoint")
    trainer1.save(filename)

    model1 = trainer1.get_model()

    trainer1.shutdown()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-address",
        required=True,
        type=str,
        help="the address to use for Redis")
    args, _ = parser.parse_known_args()

    import ray
    ray.init(redis_address=args.redis_address)
    train_example(num_replicas)
