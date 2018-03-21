from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import argparse
import json

from ray.tune import TuneError
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.trial import Resources


def json_to_resources(data):
    if type(data) is str:
        data = json.loads(data)
    for k in data:
        if k in ["driver_cpu_limit", "driver_gpu_limit"]:
            raise TuneError(
                "The field `{}` is no longer supported. Use `extra_cpu` "
                "or `extra_gpu` instead.".format(k))
        if k not in Resources._fields:
            raise TuneError(
                "Unknown resource type {}, must be one of {}".format(
                    k, Resources._fields))
    return Resources(
        data.get("cpu", 1), data.get("gpu", 0),
        data.get("extra_cpu", 0), data.get("extra_gpu", 0))


def resources_to_json(resources):
    if resources is None:
        resources = Resources(cpu=1, gpu=0)
    return {
        "cpu": resources.cpu,
        "gpu": resources.gpu,
        "extra_cpu": resources.extra_cpu,
        "extra_gpu": resources.extra_gpu,
    }


def _tune_error(msg):
    raise TuneError(msg)


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
        "--resources", help="Deprecated, use --trial-resources.",
        type=lambda v: _tune_error(
            "The `resources` argument is no longer supported. "
            "Use `trial_resources` or --trial-resources instead."))
    parser.add_argument(
        "--trial-resources", default='{"cpu": 1}', type=json_to_resources,
        help="Machine resources to allocate per trial, e.g. "
        "'{\"cpu\": 64, \"gpu\": 8}'. Note that GPUs will not be assigned "
        "unless you specify them here.")
    parser.add_argument(
        "--repeat", default=1, type=int,
        help="Number of times to repeat each trial.")
    parser.add_argument(
        "--local-dir", default=DEFAULT_RESULTS_DIR, type=str,
        help="Local dir to save training results to. Defaults to '{}'.".format(
            DEFAULT_RESULTS_DIR))
    parser.add_argument(
        "--upload-dir", default="", type=str,
        help="Optional URI to sync training results to (e.g. s3://bucket).")
    parser.add_argument(
        "--checkpoint-freq", default=0, type=int,
        help="How many training iterations between checkpoints. "
        "A value of 0 (default) disables checkpointing.")
    parser.add_argument(
        "--max-failures", default=3, type=int,
        help="Try to recover a trial from its last checkpoint at least this "
        "many times. Only applies if checkpointing is enabled.")
    parser.add_argument(
        "--scheduler", default="FIFO", type=str,
        help="FIFO (default), MedianStopping, AsyncHyperBand, or HyperBand.")
    parser.add_argument(
        "--scheduler-config", default="{}", type=json.loads,
        help="Config options to pass to the scheduler.")

    # Note: this currently only makes sense when running a single trial
    parser.add_argument("--restore", default=None, type=str,
                        help="If specified, restore from this checkpoint.")

    return parser
