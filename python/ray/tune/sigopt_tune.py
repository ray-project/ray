from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import random
import copy
import os
import time
from sigopt import Connection
import argparse
import sys
import yaml

import ray
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.error import TuneError
from ray.tune.variant_generator import to_argv
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.config_parser import make_parser, json_to_resources, resources_to_json
from ray.tune import Trainable, TrainingResult, register_trainable


from ray.tune.confidential import SIGOPT_KEY


class SigOptScheduler(FIFOScheduler):
    """Sigopt Wrapper. Can only support 10 concurrent trials at once."""
    def __init__(
            self, experiments, reward_attr="episode_reward_mean"):
        FIFOScheduler.__init__(self)
        assert len(experiments) == 1, "Currently only support 1 experiment"
        name, spec = list(experiments.keys())[0], list(experiments.values())[0]

        # Parse given spec
        self.spec = spec

        spec = copy.deepcopy(self.spec)
        if "env" in spec:
            spec["config"] = spec.get("config", {})
            spec["config"]["env"] = spec["env"]
            del spec["env"]

        self.parser = make_parser()
        self.args = self.parser.parse_args(to_argv(spec))


        # Parse function
        parameters = []
        self.default_config = copy.deepcopy(spec["config"])
        for keyname, val in spec["config"].items():
            if type(val) is dict:
                vcopy = copy.deepcopy(val)
                vcopy["name"] = keyname
                parameters.append(vcopy)

        self.conn = Connection(client_token=SIGOPT_KEY)
        self.max_concurrent = 10
        self.experiment = self.conn.experiments().create(
            name='Tune Experiment ({})'.format(name),
            parameters=parameters,
            parallel_bandwidth=self.max_concurrent,
        )
        self.suggestions = {}
        self._reward_attr = reward_attr

    def generate_trial(self):
        """Generate trial from Sigopt Service"""
        assert len(self.suggestions) < self.max_concurrent, "Too many concurrent trials!"

        suggestion = self.conn.experiments(
            self.experiment.id).suggestions().create()
        suggested_config = dict(suggestion.assignments)
        new_cfg = copy.deepcopy(self.default_config)
        new_cfg.update(suggested_config)
        args = self.args
        experiment_tag = "sigopt_{}_{}".format(self.experiment.id, suggestion.id)
        trial = Trial(
            trainable_name=self.spec["run"],
            config=new_cfg,
            local_dir=args.local_dir,
            experiment_tag=experiment_tag,
            resources=json_to_resources(self.spec.get("resources", {})),
            stopping_criterion=self.spec.get("stop", {}),
            checkpoint_freq=args.checkpoint_freq,
            restore_path=self.spec.get("restore"),
            upload_dir=args.upload_dir)
        self.suggestions[trial] = suggestion
        print("Adding new trial - {}".format(len(self.suggestions)))

        return trial

    def on_trial_result(self, trial_runner, trial, result):
        return TrialScheduler.CONTINUE

    def on_trial_complete(self, trial_runner, trial, result):
        suggestion = self.suggestions.pop(trial)

        self.conn.experiments(self.experiment.id).observations().create(
            suggestion=suggestion.id,
            value=getattr(result, self._reward_attr),
        )
        trial_runner.add_trial(self.generate_trial())

    def debug_string(self):
        return "Using SigOpt"


def run_experiments(experiments, with_server=False,
                    server_port=4321, verbose=True):

    # Make sure rllib agents are registered
    from ray import rllib  # noqa # pylint: disable=unused-import

    scheduler = SigOptScheduler(experiments)
    runner = TrialRunner(
        scheduler, launch_web_server=with_server, server_port=server_port)

    for i in range(scheduler.max_concurrent):  # number of concurrent trials
        trial = scheduler.generate_trial()
        trial.set_verbose(verbose)
        runner.add_trial(trial)
    print(runner.debug_string(max_debug=99999))

    last_debug = 0
    while not runner.is_finished():
        runner.step()
        if time.time() - last_debug > DEBUG_PRINT_INTERVAL:
            print(runner.debug_string())
            last_debug = time.time()

    print(runner.debug_string(max_debug=99999))

    for trial in runner.get_trials():
        # TODO(rliaw): What about errored?
        if trial.status != Trial.TERMINATED:
            raise TuneError("Trial did not complete", trial)

    return runner.get_trials()


def parse_config_eval(config):
    new_copy = {}
    for k in config:
        if k == "eval":
            assert len(config) == 1 and type(config[k]) is str
            return eval(config[k])
        elif type(config[k]) is dict:
            new_copy[k] = parse_config_eval(config[k])
            print(new_copy[k])
        else:
            new_copy[k] = config[k]

    return new_copy


if __name__ == '__main__':
    parser = make_parser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Train a reinforcement learning agent with SigOPT.")

    # See also the base parser definition in ray/tune/config_parser.py
    parser.add_argument(
        "--redis-address", default=None, type=str,
        help="The Redis address of the cluster.")
    parser.add_argument(
        "--num-cpus", default=None, type=int,
        help="Number of CPUs to allocate to Ray.")
    parser.add_argument(
        "--num-gpus", default=None, type=int,
        help="Number of GPUs to allocate to Ray.")
    parser.add_argument(
        "--experiment-name", default="default", type=str,
        help="Name of the subdirectory under `local_dir` to put results in.")
    parser.add_argument(
        "--env", default=None, type=str, help="The gym environment to use.")
    parser.add_argument(
        "-f", "--config-file", default=None, type=str,
        help="If specified, use config options from this file. Note that this "
        "overrides any trial-specific options set via flags above.")

    args = parser.parse_args(sys.argv[1:])
    if args.config_file:
        with open(args.config_file) as f:
            experiments = yaml.load(f)
    else:
        # Note: keep this in sync with tune/config_parser.py
        experiments = {
            args.experiment_name: {  # i.e. log to ~/ray_results/default
                "run": args.run,
                "checkpoint_freq": args.checkpoint_freq,
                "local_dir": args.local_dir,
                "resources": resources_to_json(args.resources),
                "stop": args.stop,
                "config": dict(args.config, env=args.env),
                "restore": args.restore,
                "repeat": args.repeat,
                "upload_dir": args.upload_dir,
            }
        }

    for exp in experiments.values():
        if not exp.get("run"):
            parser.error("the following arguments are required: --run")
        if not exp.get("env") and not exp.get("config", {}).get("env"):
            parser.error("the following arguments are required: --env")

    ray.init(
        redis_address=args.redis_address,
        num_cpus=args.num_cpus, num_gpus=args.num_gpus)

    for exper in experiments:
        experiments[exper]["config"] = parse_config_eval(experiments[exper]["config"])
    run_experiments(experiments)
