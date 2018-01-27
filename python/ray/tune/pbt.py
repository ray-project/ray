from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import random
import math
import copy

from ray.tune.error import TuneError
from ray.tune.trial import Trial
from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.variant_generator import _format_vars


# Parameters are transferred from the top PBT_QUANTILE fraction of trials to
# the bottom PBT_QUANTILE fraction.
PBT_QUANTILE = 0.25


class TrialState(object):
    def __init__(self, trial):
        self.orig_tag = trial.experiment_tag
        self.last_score = None
        self.last_checkpoint = None
        self.last_perturbation_time = 0

    def __repr__(self):
        return str((
            self.last_score, self.last_checkpoint,
            self.last_perturbation_time))


def explore(config, mutations, resample):
    new_config = copy.deepcopy(config)
    for key, distribution in mutations.items():
        if isinstance(distribution, list):
            new_config[key] = random.choice(distribution)
        elif resample:
            new_config[key] = distribution(config)
        elif random.random() > 0.5:
            new_config[key] = config[key] * 1.2
        else:
            new_config[key] = config[key] * 0.8
    print(
        "[explore] perturbed config from {} -> {}".format(config, new_config))
    return new_config


def make_experiment_tag(orig_tag, config, mutations):
    resolved_vars = {}
    for k in mutations.keys():
        resolved_vars[("config", k)] = config[k]
    return "{}@perturbed[{}]".format(orig_tag, _format_vars(resolved_vars))


class PopulationBasedTraining(FIFOScheduler):
    """Implements the Population Based Training (PBT) algorithm.

    PBT trains a group of models (or agents) in parallel. Periodically, poorly
    performing models clone the state of the top performers, and a random
    mutation is applied to their hyper-parameters in the hopes of
    outperforming the current top models.

    Unlike other hyperparameter search algorithms, PBT mutates hyperparameters
    during training time. This enables very fast hyperparameter discovery and
    also automatically discovers good annealing schedules.

    To learn more about PBT, check out the DeepMind blog post:

    ``https://deepmind.com/blog/population-based-training-neural-networks/``

    This Ray Tune PBT implementation considers all trials added as part of the
    PBT population. If the number of trials exceeds the cluster capacity,
    they will be time-multiplexed as to balance training progress across the
    population. This implementation is currently **Experimental**.

    Args:
        time_attr (str): The TrainingResult attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        reward_attr (str): The TrainingResult objective value attribute. As
            with `time_attr`, this may refer to any objective value. Stopping
            procedures will use this attribute.
        perturbation_interval (float): Models will be considered for
            perturbation at this interval. Note that perturbation incurs
            checkpoint overhead, so you shouldn't set this to be too frequent.
        hyperparam_mutations (dict): Hyperparams to mutate. The format is
            as follows: for each key, either a list or function can be
            provided. A list specifies values for a discrete parameter.
            A function specifies the distribution of a continuous parameter.
            For example, here A and B are discrete and C is continuous:
            {
                "A": ["list", "of", "possible", "values"],
                "B": [0.0, 1.0, 2.0],
                "C": lambda config: np.random.uniform(10.0),
            }
            Note that `mutations` is specified independently of the Ray Tune
            trial variations (though you will probably want `mutations` to be
            consistent with those). The trial variations just define the
            initial PBT population.
        resample_probability (float): The probability of resampling from the
            original distribution. If not resampled, the value will be
            perturbed by a factor of 1.2 or 0.8. Note that discrete hyperparams
            are always resampled.
    """

    def __init__(
            self, time_attr="time_total_s", reward_attr="episode_reward_mean",
            perturbation_interval=60.0, hyperparam_mutations={},
            resample_probability=0.25):
        if not hyperparam_mutations:
            raise TuneError(
                "You must specify at least one parameter to mutate with PBT.")
        FIFOScheduler.__init__(self)
        self._reward_attr = reward_attr
        self._time_attr = time_attr
        self._perturbation_interval = perturbation_interval
        self._hyperparam_mutations = hyperparam_mutations
        self._resample_probability = resample_probability
        self._trial_state = {}

        # Metrics
        self._num_checkpoints = 0
        self._num_perturbations = 0

    def on_trial_add(self, trial_runner, trial):
        self._trial_state[trial] = TrialState(trial)

    def on_trial_result(self, trial_runner, trial, result):
        time = getattr(result, self._time_attr)
        state = self._trial_state[trial]

        if time - state.last_perturbation_time < self._perturbation_interval:
            return TrialScheduler.CONTINUE  # too soon

        score = getattr(result, self._reward_attr)
        state.last_score = score
        state.last_perturbation_time = time
        lower_quantile, upper_quantile = self._quantiles()

        if trial in upper_quantile:
            state.last_checkpoint = trial.checkpoint(to_object_store=True)
            self._num_checkpoints += 1
        else:
            state.last_checkpoint = None  # not a top trial

        if trial in lower_quantile:
            trial_to_clone = random.choice(upper_quantile)
            assert trial != trial_to_clone
            self._exploit(trial, trial_to_clone)

        for trial in trial_runner.get_trials():
            if trial.status in [Trial.PENDING, Trial.PAUSED]:
                return TrialScheduler.PAUSE  # yield time to other trials

        return TrialScheduler.CONTINUE

    def _exploit(self, trial, trial_to_clone):
        """Transfers perturbed state from trial_to_clone -> trial."""

        trial_state = self._trial_state[trial]
        new_state = self._trial_state[trial_to_clone]
        if not new_state.last_checkpoint:
            print("[pbt] warn: no checkpoint for trial, skip exploit", trial)
            return
        new_config = explore(
            trial_to_clone.config, self._hyperparam_mutations,
            random.random() < self._resample_probability)
        print(
            "[exploit] transferring weights from trial "
            "{} (score {}) -> {} (score {})".format(
                trial_to_clone, new_state.last_score, trial,
                trial_state.last_score))
        # TODO(ekl) restarting the trial is expensive. We should implement a
        # lighter way reset() method that can alter the trial config.
        trial.stop()
        trial.config = new_config
        trial.experiment_tag = make_experiment_tag(
            trial_state.orig_tag, new_config, self._hyperparam_mutations)
        print("Last checkpoint", new_state.last_checkpoint)
        trial.start(new_state.last_checkpoint)
        self._num_perturbations += 1

    def _quantiles(self):
        """Returns trials in the lower and upper `quantile` of the population.
        
        If there is not enough data to compute this, returns empty lists."""

        trials = []
        for trial, state in self._trial_state.items():
            if state.last_score is not None:
                trials.append(trial)
        trials.sort(key=lambda t: self._trial_state[t].last_score)

        if len(trials) <= 1:
            return [], []
        else:
            return (
                trials[:int(math.ceil(len(trials)*PBT_QUANTILE))],
                trials[int(math.floor(-len(trials)*PBT_QUANTILE)):])

    def choose_trial_to_run(self, trial_runner, *args):
        """Ensures all trials get fair share of time (as defined by time_attr).

        This enables the PBT scheduler to support a greater number of
        concurrent trials than can fit in the cluster at any given time.
        """

        candidates = []
        for trial in trial_runner.get_trials():
            if trial.status in [Trial.PENDING, Trial.PAUSED] and \
                    trial_runner.has_resources(trial.resources):
                candidates.append(trial)
        candidates.sort(
            key=lambda trial: self._trial_state[trial].last_perturbation_time)
        return candidates[0] if candidates else None

    def debug_string(self):
        return "PopulationBasedTraining: {} checkpoints, {} perturbs".format(
            self._num_checkpoints, self._num_perturbations)
