import copy
import logging
import json
import math
import os
import random
import shutil

from ray.tune.error import TuneError
from ray.tune.result import TRAINING_ITERATION
from ray.tune.logger import _SafeFallbackEncoder
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.suggest.variant_generator import format_vars
from ray.tune.trial import Trial, Checkpoint

logger = logging.getLogger(__name__)


class PBTTrialState:
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
        if isinstance(distribution, dict):
            new_config.update({
                key: explore(config[key], mutations[key], resample_probability,
                             None)
            })
        elif isinstance(distribution, list):
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
    logger.info("[explore] perturbed config from {} -> {}".format(
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

    This Tune PBT implementation considers all trials added as part of the
    PBT population. If the number of trials exceeds the cluster capacity,
    they will be time-multiplexed as to balance training progress across the
    population. To run multiple trials, use `tune.run(num_samples=<int>)`.

    In {LOG_DIR}/{MY_EXPERIMENT_NAME}/, all mutations are logged in
    `pbt_global.txt` and individual policy perturbations are recorded
    in pbt_policy_{i}.txt. Tune logs: [target trial tag, clone trial tag,
    target trial iteration, clone trial iteration, old config, new config]
    on each perturbation step.

    Args:
        time_attr (str): The training result attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        metric (str): The training result objective value attribute. Stopping
            procedures will use this attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
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
        quantile_fraction (float): Parameters are transferred from the top
            `quantile_fraction` fraction of trials to the bottom
            `quantile_fraction` fraction. Needs to be between 0 and 0.5.
            Setting it to 0 essentially implies doing no exploitation at all.
        resample_probability (float): The probability of resampling from the
            original distribution when applying `hyperparam_mutations`. If not
            resampled, the value will be perturbed by a factor of 1.2 or 0.8
            if continuous, or changed to an adjacent value if discrete.
        custom_explore_fn (func): You can also specify a custom exploration
            function. This function is invoked as `f(config)` after built-in
            perturbations from `hyperparam_mutations` are applied, and should
            return `config` updated as needed. You must specify at least one of
            `hyperparam_mutations` or `custom_explore_fn`.
        log_config (bool): Whether to log the ray config of each model to
            local_dir at each exploit. Allows config schedule to be
            reconstructed.

    .. code-block:: python

        import random
        from ray import tune
        from ray.tune.schedulers import PopulationBasedTraining

        pbt = PopulationBasedTraining(
            time_attr="training_iteration",
            metric="episode_reward_mean",
            mode="max",
            perturbation_interval=10,  # every 10 `time_attr` units
                                       # (training_iterations in this case)
            hyperparam_mutations={
                # Perturb factor1 by scaling it by 0.8 or 1.2. Resampling
                # resets it to a value sampled from the lambda function.
                "factor_1": lambda: random.uniform(0.0, 20.0),
                # Perturb factor2 by changing it to an adjacent value, e.g.
                # 10 -> 1 or 10 -> 100. Resampling will choose at random.
                "factor_2": [1, 10, 100, 1000, 10000],
            })
        tune.run({...}, num_samples=8, scheduler=pbt)
    """

    def __init__(self,
                 time_attr="time_total_s",
                 reward_attr=None,
                 metric="episode_reward_mean",
                 mode="max",
                 perturbation_interval=60.0,
                 hyperparam_mutations={},
                 quantile_fraction=0.25,
                 resample_probability=0.25,
                 custom_explore_fn=None,
                 log_config=True):
        for value in hyperparam_mutations.values():
            if not (isinstance(value, (list, dict)) or callable(value)):
                raise TypeError("`hyperparam_mutation` values must be either "
                                "a List, Dict, or callable.")

        if not hyperparam_mutations and not custom_explore_fn:
            raise TuneError(
                "You must specify at least one of `hyperparam_mutations` or "
                "`custom_explore_fn` to use PBT.")

        if quantile_fraction > 0.5 or quantile_fraction < 0:
            raise TuneError(
                "You must set `quantile_fraction` to a value between 0 and"
                "0.5. Current value: '{}'".format(quantile_fraction))

        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"

        if reward_attr is not None:
            mode = "max"
            metric = reward_attr
            logger.warning(
                "`reward_attr` is deprecated and will be removed in a future "
                "version of Tune. "
                "Setting `metric={}` and `mode=max`.".format(reward_attr))

        FIFOScheduler.__init__(self)
        self._metric = metric
        if mode == "max":
            self._metric_op = 1.
        elif mode == "min":
            self._metric_op = -1.
        self._time_attr = time_attr
        self._perturbation_interval = perturbation_interval
        self._hyperparam_mutations = hyperparam_mutations
        self._quantile_fraction = quantile_fraction
        self._resample_probability = resample_probability
        self._trial_state = {}
        self._custom_explore_fn = custom_explore_fn
        self._log_config = log_config

        # Metrics
        self._num_checkpoints = 0
        self._num_perturbations = 0

    def on_trial_add(self, trial_runner, trial):
        self._trial_state[trial] = PBTTrialState(trial)

    def on_trial_result(self, trial_runner, trial, result):
        if self._time_attr not in result or self._metric not in result:
            return TrialScheduler.CONTINUE
        time = result[self._time_attr]
        state = self._trial_state[trial]

        if time - state.last_perturbation_time < self._perturbation_interval:
            return TrialScheduler.CONTINUE  # avoid checkpoint overhead

        score = self._metric_op * result[self._metric]
        state.last_score = score
        state.last_perturbation_time = time
        lower_quantile, upper_quantile = self._quantiles()

        if trial in upper_quantile:
            # The trial last result is only updated after the scheduler
            # callback. So, we override with the current result.
            state.last_checkpoint = trial_runner.trial_executor.save(
                trial, Checkpoint.MEMORY, result=result)
            self._num_checkpoints += 1
        else:
            state.last_checkpoint = None  # not a top trial

        if trial in lower_quantile:
            trial_to_clone = random.choice(upper_quantile)
            assert trial is not trial_to_clone
            self._exploit(trial_runner.trial_executor, trial, trial_to_clone)

        for trial in trial_runner.get_trials():
            if trial.status in [Trial.PENDING, Trial.PAUSED]:
                return TrialScheduler.PAUSE  # yield time to other trials

        return TrialScheduler.CONTINUE

    def _log_config_on_step(self, trial_state, new_state, trial,
                            trial_to_clone, new_config):
        """Logs transition during exploit/exploit step.

        For each step, logs: [target trial tag, clone trial tag, target trial
        iteration, clone trial iteration, old config, new config].
        """
        trial_name, trial_to_clone_name = (trial_state.orig_tag,
                                           new_state.orig_tag)
        trial_id = trial.trial_id
        trial_to_clone_id = trial_to_clone.trial_id
        trial_path = os.path.join(trial.local_dir,
                                  "pbt_policy_" + trial_id + ".txt")
        trial_to_clone_path = os.path.join(
            trial_to_clone.local_dir,
            "pbt_policy_" + trial_to_clone_id + ".txt")
        policy = [
            trial_name, trial_to_clone_name,
            trial.last_result.get(TRAINING_ITERATION, 0),
            trial_to_clone.last_result.get(TRAINING_ITERATION, 0),
            trial_to_clone.config, new_config
        ]
        # Log to global file.
        with open(os.path.join(trial.local_dir, "pbt_global.txt"), "a+") as f:
            print(json.dumps(policy, cls=_SafeFallbackEncoder), file=f)
        # Overwrite state in target trial from trial_to_clone.
        if os.path.exists(trial_to_clone_path):
            shutil.copyfile(trial_to_clone_path, trial_path)
        # Log new exploit in target trial log.
        with open(trial_path, "a+") as f:
            f.write(json.dumps(policy, cls=_SafeFallbackEncoder) + "\n")

    def _exploit(self, trial_executor, trial, trial_to_clone):
        """Transfers perturbed state from trial_to_clone -> trial.

        If specified, also logs the updated hyperparam state.
        """
        trial_state = self._trial_state[trial]
        new_state = self._trial_state[trial_to_clone]
        if not new_state.last_checkpoint:
            logger.info("[pbt]: no checkpoint for trial."
                        " Skip exploit for Trial {}".format(trial))
            return
        new_config = explore(trial_to_clone.config, self._hyperparam_mutations,
                             self._resample_probability,
                             self._custom_explore_fn)
        logger.info("[exploit] transferring weights from trial "
                    "{} (score {}) -> {} (score {})".format(
                        trial_to_clone, new_state.last_score, trial,
                        trial_state.last_score))

        if self._log_config:
            self._log_config_on_step(trial_state, new_state, trial,
                                     trial_to_clone, new_config)

        new_tag = make_experiment_tag(trial_state.orig_tag, new_config,
                                      self._hyperparam_mutations)
        reset_successful = trial_executor.reset_trial(trial, new_config,
                                                      new_tag)

        # TODO(ujvl): Refactor Scheduler abstraction to abstract
        #  mechanism for trial restart away. We block on restore
        #  and suppress train on start as a stop-gap fix to
        #  https://github.com/ray-project/ray/issues/7258.
        if reset_successful:
            trial_executor.restore(
                trial, new_state.last_checkpoint, block=True)
        else:
            trial_executor.stop_trial(trial, stop_logger=False)
            trial.config = new_config
            trial.experiment_tag = new_tag
            trial_executor.start_trial(
                trial, new_state.last_checkpoint, train=False)

        self._num_perturbations += 1
        # Transfer over the last perturbation time as well
        trial_state.last_perturbation_time = new_state.last_perturbation_time

    def _quantiles(self):
        """Returns trials in the lower and upper `quantile` of the population.

        If there is not enough data to compute this, returns empty lists.
        """
        trials = []
        for trial, state in self._trial_state.items():
            if state.last_score is not None and not trial.is_finished():
                trials.append(trial)
        trials.sort(key=lambda t: self._trial_state[t].last_score)

        if len(trials) <= 1:
            return [], []
        else:
            num_trials_in_quantile = int(
                math.ceil(len(trials) * self._quantile_fraction))
            if num_trials_in_quantile > len(trials) / 2:
                num_trials_in_quantile = int(math.floor(len(trials) / 2))
            return (trials[:num_trials_in_quantile],
                    trials[-num_trials_in_quantile:])

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
