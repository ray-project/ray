from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import random
import copy
import os
import time
from sigopt import Connection

import ray
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.error import TuneError
from ray.tune.variant_generator import to_argv
from ray.tune.trial_runner import TrialRunner
from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.config_parser import make_parser, json_to_resources
# from ray.tune.confidential import SIGOPT_KEY


class SigOptScheduler(FIFOScheduler):
    """Sigopt Wrapper. Can only support 10 concurrent trials at once."""
    def __init__(
            self, experiments, reward_attr="episode_reward_mean"):
        FIFOScheduler.__init__(self)
        assert len(experiments) == 1, "Currently only support 1 experiment"
        name, spec = list(experiments.keys())[0], list(experiments.values())[0]
        self.spec = spec
        parameters = []
        for keyname, val in spec["config"].items():
            assert "sigopt" in val
            vcopy = copy.deepcopy(val["sigopt"])
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
        self.parser = make_parser()

    def generate_trial(self):
        """Generate trial from Sigopt Service"""
        assert len(self.suggestions) < self.max_concurrent, "Too many concurrent trials!"

        suggestion = self.conn.experiments(
            self.experiment.id).suggestions().create()

        suggested_config = dict(suggestion.assignments)

        args = self.parser.parse_args(to_argv(self.spec))
        experiment_tag = "sigopt_{}_{}".format(self.experiment.id, suggestion.id)
        trial = Trial(
            trainable_name=self.spec["run"],
            config=suggested_config,
            local_dir=args.local_dir,
            experiment_tag=experiment_tag,
            resources=json_to_resources(self.spec.get("resources", {})),
            stopping_criterion=self.spec.get("stop", {}),
            checkpoint_freq=args.checkpoint_freq,
            restore_path=self.spec.get("restore"),
            upload_dir=args.upload_dir)
        self.suggestions[trial] = suggestion

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


if __name__ == '__main__':
    from ray.tune import Trainable, TrainingResult, register_trainable
    import json
    ray.init(redis_address=ray.services.get_node_ip_address() + ":6379"))

    run_experiments({
        "sigopt": {
            "run": "PPO",
            "stop": {"training_iteration": 10},
            "repeat": 20,
            "resources": {"cpu": 1, "gpu": 0},
            "config": {
                "width": {"sigopt": dict(type='double', bounds=dict(min=10.0, max=100.0))},
                "height": {"sigopt": dict(type='double', bounds=dict(min=0.0, max=100.0))},
            },
        }
    }, )
