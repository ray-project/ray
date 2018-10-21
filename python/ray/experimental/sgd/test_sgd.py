#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray
from ray.experimental.sgd.tfbench.test_model import TFBenchModel
from ray.experimental.sgd.sgd import DistributedSGD

parser = argparse.ArgumentParser()
parser.add_argument(
    "--num-iters", default=100, type=int, help="Number of iterations to run")
parser.add_argument("--batch-size", default=1, type=int, help="SGD batch size")
parser.add_argument("--num-workers", default=2, type=int)
parser.add_argument("--devices-per-worker", default=2, type=int)
parser.add_argument(
    "--strategy", default="simple", type=str, help="One of 'simple' or 'ps'")
parser.add_argument(
    "--gpu", action="store_true", help="Use GPUs for optimization")

if __name__ == "__main__":
    ray.init()

    args, _ = parser.parse_known_args()

    model_creator = (
        lambda worker_idx, device_idx: TFBenchModel(
            batch=args.batch_size, use_cpus=True))

    sgd = DistributedSGD(
        model_creator,
        num_workers=args.num_workers,
        devices_per_worker=args.devices_per_worker,
        gpu=args.gpu,
        strategy=args.strategy)

    for i in range(args.num_iters):
        print("Step", i)
        loss = sgd.step()
        print("Current loss", loss)
