from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import argparse
import json
import numpy as np
import os
import random

from ray.tune.trial import Trial, Resources


def _resource_json(data):
    values = json.loads(data)
    return Resources(values.get('cpu', 0), values.get('gpu', 0))


def make_parser(description):
    """Returns a base argument parser for the ray.tune tool."""

    parser = argparse.ArgumentParser(description=(description))

    parser.add_argument("--alg", default="PPO", type=str,
                        help="The learning algorithm to train.")
    parser.add_argument("--stop", default="{}", type=json.loads,
                        help="The stopping criteria, specified in JSON.")
    parser.add_argument("--config", default="{}", type=json.loads,
                        help="The config of the algorithm, specified in JSON.")
    parser.add_argument("--resources", default='{"cpu": 1}',
                        type=_resource_json,
                        help="Amount of resources to allocate per trial.")
    parser.add_argument("--num-trials", default=1, type=int,
                        help="Number of trials to evaluate.")
    parser.add_argument("--local-dir", default="/tmp/ray", type=str,
                        help="Local dir to save training results to.")
    parser.add_argument("--upload-dir", default=None, type=str,
                        help="URI to upload training results to.")
    parser.add_argument("--checkpoint-freq", default=None, type=int,
                        help="How many iterations between checkpoints.")

    # TODO(ekl) environments are RL specific
    parser.add_argument("--env", default=None, type=str,
                        help="The gym environment to use.")

    return parser


def parse_to_trials(config):
    """Parses a json config to the number of trials specified by the config.

    The input config is a mapping from experiment names to an argument
    dictionary describing a set of trials. These args include the parser args
    documented in make_parser().
    """

    def resolve(agent_cfg, resolved_vars, i):
        assert type(agent_cfg) == dict
        cfg = agent_cfg.copy()
        for p, val in cfg.items():
            if type(val) == dict and "eval" in val:
                cfg[p] = eval(val["eval"], {
                    "random": random,
                    "np": np,
                }, {
                    "_i": i,
                })
                resolved_vars[p] = True
        return cfg, resolved_vars

    def to_argv(config):
        argv = []
        for k, v in config.items():
            argv.append("--{}".format(k.replace("_", "-")))
            if type(v) is str:
                argv.append(v)
            else:
                argv.append(json.dumps(v))
        return argv

    def param_str(config, resolved_vars):
        return "_".join(
            [k + "=" + str(v) for k, v in sorted(config.items())
                if resolved_vars.get(k)])

    parser = make_parser("Ray hyperparameter tuning tool")
    trials = []
    for experiment_name, exp_cfg in config.items():
        args = parser.parse_args(to_argv(exp_cfg))
        grid_search = _GridSearchGenerator(args.config)
        for i in range(args.num_trials):
            next_cfg, resolved_vars = grid_search.next()
            resolved, resolved_vars = resolve(next_cfg, resolved_vars, i)
            if resolved_vars:
                tag_str = "{}_{}".format(
                    i, param_str(resolved, resolved_vars))
            else:
                tag_str = str(i)
            trials.append(Trial(
                args.env, args.alg, resolved,
                os.path.join(args.local_dir, experiment_name), tag_str,
                args.resources, args.stop, args.checkpoint_freq, None,
                args.upload_dir))

    return trials


class _GridSearchGenerator(object):
    """Generator that implements grid search over a set of value lists."""

    def __init__(self, agent_cfg):
        self.cfg = agent_cfg
        self.grid_values = []
        for p, val in sorted(agent_cfg.items()):
            if type(val) == dict and "grid_search" in val:
                assert type(val["grid_search"] == list)
                self.grid_values.append((p, val["grid_search"]))
        self.value_indices = [0] * len(self.grid_values)

    def next(self):
        cfg = self.cfg.copy()
        resolved_vars = {}
        for i, (k, values) in enumerate(self.grid_values):
            idx = self.value_indices[i]
            cfg[k] = values[idx]
            resolved_vars[k] = True
        if self.grid_values:
            self._increment(0)
        return cfg, resolved_vars

    def _increment(self, i):
        self.value_indices[i] += 1
        if self.value_indices[i] >= len(self.grid_values[i][1]):
            self.value_indices[i] = 0
            if i + 1 < len(self.value_indices):
                self._increment(i + 1)
