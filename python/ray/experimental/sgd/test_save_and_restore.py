#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import time

import ray
from ray.experimental.sgd.tfbench.test_model import TFBenchModel
from ray.experimental.sgd.sgd import DistributedSGD

parser = argparse.ArgumentParser()
parser.add_argument("--redis-address", default=None, type=str)
parser.add_argument("--num-iters", default=10, type=int)
parser.add_argument("--batch-size", default=1, type=int)
parser.add_argument("--num-workers", default=2, type=int)
parser.add_argument("--grad-shard-bytes", default=10000000, type=int)
parser.add_argument("--devices-per-worker", default=2, type=int)
parser.add_argument("--all-reduce-alg", default="simple", type=str)
parser.add_argument("--object-store-memory", default=None, type=int)
parser.add_argument("--checkpoint-dir", default="/tmp", type=str)
parser.add_argument(
    "--strategy", default="simple", type=str, help="One of 'simple' or 'ps'")
parser.add_argument(
    "--gpu", action="store_true", help="Use GPUs for optimization")

if __name__ == "__main__":
    args, _ = parser.parse_known_args()
    ray.init(
        redis_address=args.redis_address,
        object_store_memory=args.object_store_memory)

    model_creator = (
        lambda worker_idx, device_idx: TFBenchModel(
            batch=args.batch_size, use_cpus=not args.gpu))

    sgd = DistributedSGD(
        model_creator,
        num_workers=args.num_workers,
        devices_per_worker=args.devices_per_worker,
        gpu=args.gpu,
        strategy=args.strategy,
        grad_shard_bytes=args.grad_shard_bytes,
        all_reduce_alg=args.all_reduce_alg)

    if not os.path.exists(args.checkpoint_dir):
        raise ValueError(
            "Checkpoint directory does not exist: %s" % args.checkpoint_dir)

    def step(i):
        start = time.time()
        print("== Step {} ==".format(i))
        stats = sgd.step(fetch_stats=True)
        ips = ((args.batch_size * args.num_workers * args.devices_per_worker) /
               (time.time() - start))
        print("Iteration time", time.time() - start, "Images per second", ips)
        print("Current loss", stats)

    i = 0
    while i < args.num_iters:
        step(i)
        i += 1

    print("Saving checkpoint...")
    sgd.save_checkpoint(args.checkpoint_dir)
    print("Done saving checkpoint")

    step(i)

    print("Restoring checkpoint")
    sgd.restore_checkpoint(args.checkpoint_dir)
    print("Done restoring checkpoint")

    step(i)
