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
from ray.tune.confidential import SIGOPT_KEY


class SigOptScheduler(FIFOScheduler):
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

    # def on_trial_remove(self, trial_runner, trial):
    #     """Marks trial as completed if it is paused and has previously ran."""
    #     if trial.status is Trial.PAUSED and trial in self._results:
    #         self._completed_trials.add(trial)

    def debug_string(self):
        return "Using SigOpt"

    # def _get_median_result(self, time):
    #     scores = []
    #     for trial in self._completed_trials:
    #         scores.append(self._running_result(trial, time))
    #     if len(scores) >= self._min_samples_required:
    #         return np.median(scores)
    #     else:
    #         return float('-inf')

    # def _running_result(self, trial, t_max=float('inf')):
    #     results = self._results[trial]
    #     # TODO(ekl) we could do interpolation to be more precise, but for now
    #     # assume len(results) is large and the time diffs are roughly equal
    #     return np.mean(
    #         [getattr(r, self._reward_attr)
    #             for r in results if getattr(r, self._time_attr) <= t_max])

    # def _best_result(self, trial):
    #     results = self._results[trial]
    #     return max([getattr(r, self._reward_attr) for r in results])



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
    ray.init()
    import json
    class MyTrainableClass(Trainable):
        """Example agent whose learning curve is a random sigmoid.

        The dummy hyperparameters "width" and "height" determine the slope and
        maximum reward value reached.
        """

        def _setup(self):
            self.timestep = 0

        def _train(self):
            self.timestep += 1
            v = np.tanh(float(self.timestep) / self.config["width"])
            v *= self.config["height"]

            # Here we use `episode_reward_mean`, but you can also report other
            # objectives such as loss or accuracy (see tune/result.py).
            return TrainingResult(episode_reward_mean=v, timesteps_this_iter=1)

        def _save(self, checkpoint_dir):
            path = os.path.join(checkpoint_dir, "checkpoint")
            with open(path, "w") as f:
                f.write(json.dumps({"timestep": self.timestep}))
            return path

        def _restore(self, checkpoint_path):
            with open(checkpoint_path) as f:
                self.timestep = json.loads(f.read())["timestep"]


    register_trainable("my_class", MyTrainableClass)
    run_experiments({
        "hyperband_test": {
            "run": "my_class",
            "stop": {"training_iteration": 10},
            "repeat": 20,
            "resources": {"cpu": 1, "gpu": 0},
            "config": {
                "width": {"sigopt": dict(type='double', bounds=dict(min=10.0, max=100.0))},
                "height": {"sigopt": dict(type='double', bounds=dict(min=0.0, max=100.0))},
            },
        }
    }, )
