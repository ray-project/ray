from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import argparse
import json

from ray.tune.trial import Resources


def json_to_resources(data):
    if type(data) is str:
        data = json.loads(data)
    return Resources(
        data.get("cpu", 0), data.get("gpu", 0),
        data.get("driver_cpu_limit"), data.get("driver_gpu_limit"))


def resources_to_json(resources):
    return {
        "cpu": resources.cpu,
        "gpu": resources.gpu,
        "driver_cpu_limit": resources.driver_cpu_limit,
        "driver_gpu_limit": resources.driver_gpu_limit,
    }


def make_parser(**kwargs):
    """Returns a base argument parser for the ray.tune tool."""

    parser = argparse.ArgumentParser(**kwargs)

    # Note: keep this in sync with rllib/train.py
    parser.add_argument("--alg", default=None, type=str,
                        help="The learning algorithm to train.")
    parser.add_argument("--stop", default="{}", type=json.loads,
                        help="The stopping criteria, specified in JSON.")
    parser.add_argument("--config", default="{}", type=json.loads,
                        help="The config of the algorithm, specified in JSON.")
    parser.add_argument("--resources", default='{"cpu": 1}',
                        type=json_to_resources,
                        help="Amount of resources to allocate per trial.")
    parser.add_argument("--repeat", default=1, type=int,
                        help="Number of times to repeat each trial.")
    parser.add_argument("--local-dir", default="/tmp/ray", type=str,
                        help="Local dir to save training results to.")
    parser.add_argument("--upload-dir", default="", type=str,
                        help="URI to upload training results to.")
    parser.add_argument("--checkpoint-freq", default=0, type=int,
                        help="How many iterations between checkpoints.")
    parser.add_argument("--scheduler", default="FIFO", type=str,
                        help="FIFO, MedianStopping, or HyperBand")
    parser.add_argument("--scheduler-config", default="{}", type=json.loads,
                        help="Config options to pass to the scheduler.")

    # Note: this currently only makes sense when running a single trial
    parser.add_argument("--restore", default=None, type=str,
                        help="If specified, restore from this checkpoint.")

    # TODO(ekl) environments are RL specific
    parser.add_argument("--env", default=None, type=str,
                        help="The gym environment to use.")

    return parser
