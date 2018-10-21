#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import time

import ray
from ray.experimental.sgd.tfbench.test_model import TFBenchModel
from ray.experimental.sgd.sgd import DistributedSGD

parser = argparse.ArgumentParser()
parser.add_argument("--redis-address", default=None, type=str)
parser.add_argument("--num-iters", default=10, type=int)
parser.add_argument("--batch-size", default=1, type=int)
parser.add_argument("--num-workers", default=2, type=int)
parser.add_argument("--max-shard-bytes", default=10000000, type=int)
parser.add_argument("--devices-per-worker", default=2, type=int)
parser.add_argument("--stats-interval", default=10, type=int)
parser.add_argument(
    "--strategy", default="ps", type=str, help="One of 'simple' or 'ps'")
parser.add_argument(
    "--gpu", action="store_true", help="Use GPUs for optimization")

if __name__ == "__main__":
    args, _ = parser.parse_known_args()
    ray.init(redis_address=args.redis_address)

    model_creator = (
        lambda worker_idx, device_idx: TFBenchModel(
            batch=args.batch_size, use_cpus=True))

    sgd = DistributedSGD(
        model_creator,
        num_workers=args.num_workers,
        devices_per_worker=args.devices_per_worker,
        gpu=args.gpu,
        strategy=args.strategy,
        max_shard_bytes=args.max_shard_bytes)

    t = []

    for i in range(args.num_iters):
        start = time.time()
        fetch_stats = i % args.stats_interval == 0
        print("== Step {} ==".format(i))
        stats = sgd.step(fetch_stats=fetch_stats)
        ips = ((args.batch_size * args.num_workers * args.devices_per_worker) /
               (time.time() - start))
        print("Iteration time", time.time() - start, "Images per second", ips)
        t.append(ips)
        if fetch_stats:
            print("Current loss", stats)

    print("Peak throughput", max([sum(t[i:i + 5]) / 5 for i in range(len(t))]))
