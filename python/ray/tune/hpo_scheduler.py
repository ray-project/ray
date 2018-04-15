from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import copy
import numpy as np
try:
    import hyperopt as hpo
except Exception as e:
    hpo = None

from ray.tune.trial import Trial
from ray.tune.error import TuneError
from ray.tune.trial_scheduler import TrialScheduler, FIFOScheduler
from ray.tune.config_parser import make_parser
from ray.tune.variant_generator import to_argv


class HyperOptScheduler(FIFOScheduler):
    """FIFOScheduler that uses HyperOpt to provide trial suggestions.

    Requires HyperOpt to be installed via source.
    Uses the Tree-structured Parzen Estimators algorithm. Externally added
    trials will not be tracked by HyperOpt. Also,
    variant generation will be limited, as the hyperparameter configuration
    must be specified using HyperOpt primitives.

    Parameters:
        max_concurrent (int | None): Number of maximum concurrent trials.
            If None, then trials will be queued only if resources
            are available.
        reward_attr (str): The TrainingResult objective value attribute.
            This refers to an increasing value, which is internally negated
            when interacting with HyperOpt. Suggestion procedures
            will use this attribute.

    Examples:
        >>> space = {'param': hp.uniform('param', 0, 20)}
        >>> config = {"my_exp": {
                          "run": "exp",
                          "repeat": 5,
                          "config": {"space": space}}}
        >>> run_experiments(config, scheduler=HyperOptScheduler())
    """

    def __init__(self, max_concurrent=None, reward_attr="episode_reward_mean"):
        assert hpo is not None, "HyperOpt must be installed!"
        assert type(max_concurrent) in [type(None), int]
        if type(max_concurrent) is int:
            assert max_concurrent > 0
        self._max_concurrent = max_concurrent  # NOTE: this is modified later
        self._reward_attr = reward_attr
        self._experiment = None

    def add_experiment(self, experiment, trial_runner):
        """Tracks one experiment.

        Will error if one tries to track multiple experiments.
        """
        assert self._experiment is None, "HyperOpt only tracks one experiment!"
        self._experiment = experiment

        self._output_path = experiment.name
        spec = copy.deepcopy(experiment.spec)

        # Set Scheduler field, as Tune Parser will default to FIFO
        assert spec.get("scheduler") in [None, "HyperOpt"], "Incorrectly " \
            "specified scheduler!"
        spec["scheduler"] = "HyperOpt"

        if "env" in spec:
            spec["config"] = spec.get("config", {})
            spec["config"]["env"] = spec["env"]
            del spec["env"]

        space = spec["config"]["space"]
        del spec["config"]["space"]

        self.parser = make_parser()
        self.args = self.parser.parse_args(to_argv(spec))
        self.args.scheduler = "HyperOpt"
        self.default_config = copy.deepcopy(spec["config"])

        self.algo = hpo.tpe.suggest
        self.domain = hpo.Domain(lambda spc: spc, space)
        self._hpopt_trials = hpo.Trials()
        self._tune_to_hp = {}
        self._num_trials_left = self.args.repeat

        if type(self._max_concurrent) is int:
            self._max_concurrent = min(self._max_concurrent, self.args.repeat)

        self.rstate = np.random.RandomState()
        self.trial_generator = self._trial_generator()
        self._add_new_trials_if_needed(trial_runner)

    def _trial_generator(self):
        while self._num_trials_left > 0:
            new_cfg = copy.deepcopy(self.default_config)
            new_ids = self._hpopt_trials.new_trial_ids(1)
            self._hpopt_trials.refresh()

            # Get new suggestion from
            new_trials = self.algo(new_ids, self.domain, self._hpopt_trials,
                                   self.rstate.randint(2**31 - 1))
            self._hpopt_trials.insert_trial_docs(new_trials)
            self._hpopt_trials.refresh()
            new_trial = new_trials[0]
            new_trial_id = new_trial["tid"]
            suggested_config = hpo.base.spec_from_misc(new_trial["misc"])
            new_cfg.update(suggested_config)

            kv_str = "_".join([
                "{}={}".format(k,
                               str(v)[:5])
                for k, v in sorted(suggested_config.items())
            ])
            experiment_tag = "{}_{}".format(new_trial_id, kv_str)

            # Keep this consistent with tune.variant_generator
            trial = Trial(
                trainable_name=self.args.run,
                config=new_cfg,
                local_dir=os.path.join(self.args.local_dir, self._output_path),
                experiment_tag=experiment_tag,
                resources=self.args.trial_resources,
                stopping_criterion=self.args.stop,
                checkpoint_freq=self.args.checkpoint_freq,
                restore_path=self.args.restore,
                upload_dir=self.args.upload_dir,
                max_failures=self.args.max_failures)

            self._tune_to_hp[trial] = new_trial_id
            self._num_trials_left -= 1
            yield trial

    def on_trial_result(self, trial_runner, trial, result):
        ho_trial = self._get_hyperopt_trial(self._tune_to_hp[trial])
        now = hpo.utils.coarse_utcnow()
        ho_trial['book_time'] = now
        ho_trial['refresh_time'] = now
        return TrialScheduler.CONTINUE

    def on_trial_error(self, trial_runner, trial):
        ho_trial = self._get_hyperopt_trial(self._tune_to_hp[trial])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_ERROR
        ho_trial['misc']['error'] = (str(TuneError), "Tune Error")
        self._hpopt_trials.refresh()
        del self._tune_to_hp[trial]

    def on_trial_remove(self, trial_runner, trial):
        ho_trial = self._get_hyperopt_trial(self._tune_to_hp[trial])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_ERROR
        ho_trial['misc']['error'] = (str(TuneError), "Tune Removed")
        self._hpopt_trials.refresh()
        del self._tune_to_hp[trial]

    def on_trial_complete(self, trial_runner, trial, result):
        ho_trial = self._get_hyperopt_trial(self._tune_to_hp[trial])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_DONE
        hp_result = self._to_hyperopt_result(result)
        ho_trial['result'] = hp_result
        self._hpopt_trials.refresh()
        del self._tune_to_hp[trial]

    def _to_hyperopt_result(self, result):
        return {"loss": -getattr(result, self._reward_attr), "status": "ok"}

    def _get_hyperopt_trial(self, tid):
        return [t for t in self._hpopt_trials.trials if t["tid"] == tid][0]

    def choose_trial_to_run(self, trial_runner):
        self._add_new_trials_if_needed(trial_runner)
        return FIFOScheduler.choose_trial_to_run(self, trial_runner)

    def _add_new_trials_if_needed(self, trial_runner):
        """Checks if there is a next trial ready to be queued.

        This is determined by tracking the number of concurrent
        experiments and trials left to run. If self._max_concurrent is None,
        scheduler will add new trial if there is none that are pending.
        """
        pending = [
            t for t in trial_runner.get_trials() if t.status == Trial.PENDING
        ]
        if self._num_trials_left <= 0:
            return
        if self._max_concurrent is None:
            if not pending:
                trial_runner.add_trial(next(self.trial_generator))
        else:
            while self._num_live_trials() < self._max_concurrent:
                try:
                    trial_runner.add_trial(next(self.trial_generator))
                except StopIteration:
                    break

    def _num_live_trials(self):
        return len(self._tune_to_hp)
