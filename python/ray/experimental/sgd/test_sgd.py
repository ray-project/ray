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

if __name__ == "__main__":
    ray.init()

    args, _ = parser.parse_known_args()

    model_creator = (
        lambda worker_idx, device_idx: TFBenchModel(batch=1, use_cpus=True))

    sgd = DistributedSGD(
        model_creator,
        num_workers=2,
        devices_per_worker=2,
        use_cpus=True,
        use_plasma_op=False)

    for _ in range(args.num_iters):
        loss = sgd.step()
        print("Current loss", loss)
