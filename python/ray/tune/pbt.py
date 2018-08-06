from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import math
import copy

from ray.tune.error import TuneError
from ray.tune.trial import Trial
from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.suggest.variant_generator import format_vars

# Parameters are transferred from the top PBT_QUANTILE fraction of trials to
# the bottom PBT_QUANTILE fraction.
PBT_QUANTILE = 0.25


class PBTTrialState(object):
    """Internal PBT state tracked per-trial."""

    def __init__(self, trial):
        self.orig_tag = trial.experiment_tag
        self.last_score = None
        self.last_checkpoint = None
        self.last_perturbation_time = 0

    def __repr__(self):
        return str((self.last_score, self.last_checkpoint,
                    self.last_perturbation_time))


def explore(config, mutations, resample_probability, custom_explore_fn):
    """Return a config perturbed as specified.

    Args:
        config (dict): Original hyperparameter configuration.
        mutations (dict): Specification of mutations to perform as documented
            in the PopulationBasedTraining scheduler.
        resample_probability (float): Probability of allowing resampling of a
            particular variable.
        custom_explore_fn (func): Custom explore fn applied after built-in
            config perturbations are.
    """
    new_config = copy.deepcopy(config)
    for key, distribution in mutations.items():
        if isinstance(distribution, list):
            if random.random() < resample_probability or \
                    config[key] not in distribution:
                new_config[key] = random.choice(distribution)
            elif random.random() > 0.5:
                new_config[key] = distribution[max(
                    0,
                    distribution.index(config[key]) - 1)]
            else:
                new_config[key] = distribution[min(
                    len(distribution) - 1,
                    distribution.index(config[key]) + 1)]
        else:
            if random.random() < resample_probability:
                new_config[key] = distribution()
            elif random.random() > 0.5:
                new_config[key] = config[key] * 1.2
            else:
                new_config[key] = config[key] * 0.8
            if type(config[key]) is int:
                new_config[key] = int(new_config[key])
    if custom_explore_fn:
        new_config = custom_explore_fn(new_config)
        assert new_config is not None, \
            "Custom explore fn failed to return new config"
    print("[explore] perturbed config from {} -> {}".format(
        config, new_config))
    return new_config


def make_experiment_tag(orig_tag, config, mutations):
    """Appends perturbed params to the trial name to show in the console."""

    resolved_vars = {}
    for k in mutations.keys():
        resolved_vars[("config", k)] = config[k]
    return "{}@perturbed[{}]".format(orig_tag, format_vars(resolved_vars))


class PopulationBasedTraining(FIFOScheduler):
    """Implements the Population Based Training (PBT) algorithm.

    https://deepmind.com/blog/population-based-training-neural-networks

    PBT trains a group of models (or agents) in parallel. Periodically, poorly
    performing models clone the state of the top performers, and a random
    mutation is applied to their hyperparameters in the hopes of
    outperforming the current top models.

    Unlike other hyperparameter search algorithms, PBT mutates hyperparameters
    during training time. This enables very fast hyperparameter discovery and
    also automatically discovers good annealing schedules.

    This Ray Tune PBT implementation considers all trials added as part of the
    PBT population. If the number of trials exceeds the cluster capacity,
    they will be time-multiplexed as to balance training progress across the
    population.

    Args:
        time_attr (str): The training result attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        reward_attr (str): The training result objective value attribute. As
            with `time_attr`, this may refer to any objective value. Stopping
            procedures will use this attribute.
        perturbation_interval (float): Models will be considered for
            perturbation at this interval of `time_attr`. Note that
            perturbation incurs checkpoint overhead, so you shouldn't set this
            to be too frequent.
        hyperparam_mutations (dict): Hyperparams to mutate. The format is
            as follows: for each key, either a list or function can be
            provided. A list specifies an allowed set of categorical values.
            A function specifies the distribution of a continuous parameter.
            You must specify at least one of `hyperparam_mutations` or
            `custom_explore_fn`.
        resample_probability (float): The probability of resampling from the
            original distribution when applying `hyperparam_mutations`. If not
            resampled, the value will be perturbed by a factor of 1.2 or 0.8
            if continuous, or changed to an adjacent value if discrete.
        custom_explore_fn (func): You can also specify a custom exploration
            function. This function is invoked as `f(config)` after built-in
            perturbations from `hyperparam_mutations` are applied, and should
            return `config` updated as needed. You must specify at least one of
            `hyperparam_mutations` or `custom_explore_fn`.

    Example:
        >>> pbt = PopulationBasedTraining(
        >>>     time_attr="training_iteration",
        >>>     reward_attr="episode_reward_mean",
        >>>     perturbation_interval=10,  # every 10 `time_attr` units
        >>>                                # (training_iterations in this case)
        >>>     hyperparam_mutations={
        >>>         # Perturb factor1 by scaling it by 0.8 or 1.2. Resampling
        >>>         # resets it to a value sampled from the lambda function.
        >>>         "factor_1": lambda: random.uniform(0.0, 20.0),
        >>>         # Perturb factor2 by changing it to an adjacent value, e.g.
        >>>         # 10 -> 1 or 10 -> 100. Resampling will choose at random.
        >>>         "factor_2": [1, 10, 100, 1000, 10000],
        >>>     })
        >>> run_experiments({...}, scheduler=pbt)
    """

    def __init__(self,
                 time_attr="time_total_s",
                 reward_attr="episode_reward_mean",
                 perturbation_interval=60.0,
                 hyperparam_mutations={},
                 resample_probability=0.25,
                 custom_explore_fn=None):
        if not hyperparam_mutations and not custom_explore_fn:
            raise TuneError(
                "You must specify at least one of `hyperparam_mutations` or "
                "`custom_explore_fn` to use PBT.")
        FIFOScheduler.__init__(self)
        self._reward_attr = reward_attr
        self._time_attr = time_attr
        self._perturbation_interval = perturbation_interval
        self._hyperparam_mutations = hyperparam_mutations
        self._resample_probability = resample_probability
        self._trial_state = {}
        self._custom_explore_fn = custom_explore_fn

        # Metrics
        self._num_checkpoints = 0
        self._num_perturbations = 0

    def on_trial_add(self, trial_runner, trial):
        self._trial_state[trial] = PBTTrialState(trial)

    def on_trial_result(self, trial_runner, trial, result):
        time = result[self._time_attr]
        state = self._trial_state[trial]

        if time - state.last_perturbation_time < self._perturbation_interval:
            return TrialScheduler.CONTINUE  # avoid checkpoint overhead

        score = result[self._reward_attr]
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
            assert trial is not trial_to_clone
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
        new_config = explore(trial_to_clone.config, self._hyperparam_mutations,
                             self._resample_probability,
                             self._custom_explore_fn)
        print("[exploit] transferring weights from trial "
              "{} (score {}) -> {} (score {})".format(
                  trial_to_clone, new_state.last_score, trial,
                  trial_state.last_score))
        # TODO(ekl) restarting the trial is expensive. We should implement a
        # lighter way reset() method that can alter the trial config.
        trial.stop(stop_logger=False)
        trial.config = new_config
        trial.experiment_tag = make_experiment_tag(
            trial_state.orig_tag, new_config, self._hyperparam_mutations)
        trial.start(new_state.last_checkpoint)
        self._num_perturbations += 1
        # Transfer over the last perturbation time as well
        trial_state.last_perturbation_time = new_state.last_perturbation_time

    def _quantiles(self):
        """Returns trials in the lower and upper `quantile` of the population.

        If there is not enough data to compute this, returns empty lists."""

        trials = []
        for trial, state in self._trial_state.items():
            if state.last_score is not None and not trial.is_finished():
                trials.append(trial)
        trials.sort(key=lambda t: self._trial_state[t].last_score)

        if len(trials) <= 1:
            return [], []
        else:
            return (trials[:int(math.ceil(len(trials) * PBT_QUANTILE))],
                    trials[int(math.floor(-len(trials) * PBT_QUANTILE)):])

    def choose_trial_to_run(self, trial_runner):
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

    def reset_stats(self):
        self._num_perturbations = 0
        self._num_checkpoints = 0

    def last_scores(self, trials):
        scores = []
        for trial in trials:
            state = self._trial_state[trial]
            if state.last_score is not None and not trial.is_finished():
                scores.append(state.last_score)
        return scores

    def debug_string(self):
        return "PopulationBasedTraining: {} checkpoints, {} perturbs".format(
            self._num_checkpoints, self._num_perturbations)
