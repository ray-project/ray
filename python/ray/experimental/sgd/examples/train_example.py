from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from ray.experimental.sgd.pytorch import PyTorchTrainer

from ray.experimental.sgd.tests.pytorch_utils import (
    model_creator, optimizer_creator, data_creator)


def train_example(num_replicas=1, use_gpu=False):
    trainer1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        num_replicas=num_replicas,
        use_gpu=use_gpu,
        backend="gloo")
    trainer1.train()
    trainer1.shutdown()
    print("success!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-address",
        required=False,
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--num-replicas",
        "-n",
        type=int,
        default=1,
        help="Sets number of replicas for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    args, _ = parser.parse_known_args()

    import ray

    ray.init(redis_address=args.redis_address)
    train_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
