from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import numpy as np
import hyperopt as hpo

from ray.tune.config_parser import make_parser
from ray.tune.variant_generator import to_argv
from ray.tune.trial import Trial
from ray.tune import TuneError
from ray.tune.experiment import Experiment


class HyperOptExperiment(Experiment):
    """Tracks experiment with HyperOpt trial suggestions.

    Requires HyperOpt to be installed. Uses the Tree of Parzen Estimators
    algorithm. Mixing standard trial configuration with this class results
    in undefined behavior and hence unrecommended.

    Note that this class takes in a loss attribute rather than a reward.

    Parameters:
        name (str):Inherited from ray.tune.experiment.
        run (str): Inherited from ray.tune.experiment.
        max_concurrent (int): Number of maximum concurrent trials.
        loss_attr (str): The TrainingResult objective value attribute.
            This may refer to any decreasing value. Suggestion procedures
            will use this attribute.
        kwargs: Arguments inherited from ray.tune.experiment.
    """

    def __init__(self, name, run, max_concurrent=10,
                 loss_attr="mean_loss", **kwargs):
        self._max_concurrent = max_concurrent  # NOTE: this is modified later
        self._loss_attr = loss_attr
        super(HyperOptExperiment, self).__init__(name, run, **kwargs)

    def _initialize_generator(self, spec, name):
        if "env" in spec:
            spec["config"] = spec.get("config", {})
            spec["config"]["env"] = spec["env"]
            del spec["env"]

        assert "space" in spec["config"], "HyperOpt need a 'space' value!"
        space = spec["config"]["space"]
        del spec["config"]["space"]

        parser = make_parser()
        self.args = parser.parse_args(to_argv(spec))
        self.default_config = copy.deepcopy(spec["config"])

        self.algo = hpo.tpe.suggest
        self.domain = hpo.Domain(lambda hp_spec: hp_spec, space)

        self._hpopt_trials = hpo.Trials()
        self._tune_to_hp = {}
        self._num_trials_left = self.args.repeat

        self._max_concurrent = min(self._max_concurrent, self.args.repeat)
        self.rstate = np.random.RandomState()
        self.trial_generator = self._trial_generator()

    def _trial_generator(self):
        while self._num_trials_left > 0:
            new_cfg = copy.deepcopy(self.default_config)
            new_ids = self._hpopt_trials.new_trial_ids(1)
            self._hpopt_trials.refresh()

            new_trials = self.algo(
                new_ids, self.domain, self._hpopt_trials,
                self.rstate.randint(2 ** 31 - 1))

            self._hpopt_trials.insert_trial_docs(new_trials)
            self._hpopt_trials.refresh()
            new_trial = new_trials[0]
            new_trial_id = new_trial["tid"]

            suggested_config = hpo.base.spec_from_misc(new_trial["misc"])
            new_cfg.update(suggested_config)
            kv_str = "_".join(["{}={}".format(k, str(v)[:5])
                               for k, v in suggested_config.items()])
            experiment_tag = "hyperopt_{}_{}".format(new_trial_id, kv_str)

            trial = Trial(
                trainable_name=self.args.run,
                config=new_cfg,
                local_dir=self.args.local_dir,
                experiment_tag=experiment_tag,
                resources=self.args.resources,
                stopping_criterion=self.args.stop,
                checkpoint_freq=self.args.checkpoint_freq,
                restore_path=self.args.restore,
                upload_dir=self.args.upload_dir)

            self._tune_to_hp[trial] = new_trial_id
            self._num_trials_left -= 1
            yield trial

    def on_trial_stop(self, trial, error=False):
        """Updates Hyperopt's logical tracking of trials."""
        ho_trial = self._get_dynamic_trial(self._tune_to_hp[trial])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        if error:
            ho_trial['state'] = hpo.base.JOB_STATE_ERROR
            ho_trial['misc']['error'] = (str(TuneError), "Trial stopped early")
        self._hpopt_trials.refresh()
        del self._tune_to_hp[trial]

    def on_trial_complete(self, trial):
        """Updates Hyperopt's logical tracking of trials."""
        ho_trial = self._get_dynamic_trial(self._tune_to_hp[trial])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_DONE
        if trial.last_result:
            hp_result = self._convert_result(trial.last_result)
            ho_trial['result'] = hp_result
        self._hpopt_trials.refresh()

    def ready(self):
        """Checks if there is a next trial ready to be queued.

        This is determined by tracking the number of concurrent
        experiments and trials left to run."""
        return (self._num_trials_left > 0 and
                self._num_live_trials() < self._max_concurrent)

    def _num_live_trials(self):
        return len(self._tune_to_hp)

    def get_hyperopt_trials(self):
        """Returns Hyperopt's logical tracking of trials."""
        return self._hpopt_trials

    def _convert_result(self, result):
        return {"loss": getattr(result, self._loss_attr),
                "status": "ok"}

    def _get_dynamic_trial(self, tid):
        return [t for t in self._hpopt_trials.trials if t["tid"] == tid][0]
