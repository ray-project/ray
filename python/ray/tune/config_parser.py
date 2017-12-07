from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import argparse
import json

from ray.tune import TuneError
from ray.tune.trial import Resources


def json_to_resources(data):
    if type(data) is str:
        data = json.loads(data)
    for k in data:
        if k not in Resources._fields:
            raise TuneError(
                "Unknown resource type {}, must be one of {}".format(
                    k, Resources._fields))
    return Resources(
        data.get("cpu", 1), data.get("gpu", 0),
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
    parser.add_argument(
        "--run", default=None, type=str,
        help="The algorithm or model to train. This may refer to the name "
        "of a built-on algorithm (e.g. RLLib's DQN or PPO), or a "
        "user-defined trainable function or class registered in the "
        "tune registry.")
    parser.add_argument(
        "--stop", default="{}", type=json.loads,
        help="The stopping criteria, specified in JSON. The keys may be any "
        "field in TrainingResult, e.g. "
        "'{\"time_total_s\": 600, \"timesteps_total\": 100000}' to stop "
        "after 600 seconds or 100k timesteps, whichever is reached first.")
    parser.add_argument(
        "--config", default="{}", type=json.loads,
        help="Algorithm-specific configuration (e.g. env, hyperparams), "
        "specified in JSON.")
    parser.add_argument(
        "--resources", default='{"cpu": 1}', type=json_to_resources,
        help="Machine resources to allocate per trial, e.g. "
        "'{\"cpu\": 64, \"gpu\": 8}'. Note that GPUs will not be assigned "
        "unless you specify them here.")
    parser.add_argument(
        "--repeat", default=1, type=int,
        help="Number of times to repeat each trial.")
    parser.add_argument(
        "--local-dir", default="/tmp/ray", type=str,
        help="Local dir to save training results to. Defaults to '/tmp/ray'.")
    parser.add_argument(
        "--upload-dir", default="", type=str,
        help="Optional URI to upload training results to.")
    parser.add_argument(
        "--checkpoint-freq", default=0, type=int,
        help="How many training iterations between checkpoints. "
        "A value of 0 (default) disables checkpointing.")
    parser.add_argument(
        "--scheduler", default="FIFO", type=str,
        help="FIFO (default), MedianStopping, or HyperBand.")
    parser.add_argument(
        "--scheduler-config", default="{}", type=json.loads,
        help="Config options to pass to the scheduler.")

    # Note: this currently only makes sense when running a single trial
    parser.add_argument("--restore", default=None, type=str,
                        help="If specified, restore from this checkpoint.")

    return parser
