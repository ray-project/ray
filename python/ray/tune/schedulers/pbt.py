import copy
import json
import logging
import math
import os
import random
import shutil
from typing import Callable, Dict, List, Optional, Tuple, Union

from ray.tune.execution import trial_runner
from ray.tune.error import TuneError
from ray.tune.result import DEFAULT_METRIC, TRAINING_ITERATION
from ray.tune.search import SearchGenerator
from ray.tune.utils.util import SafeFallbackEncoder
from ray.tune.search.sample import Domain, Function
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.search.variant_generator import format_vars
from ray.tune.experiment import Trial
from ray.util import PublicAPI
from ray.util.debug import log_once
from ray.util.ml_utils.checkpoint_manager import CheckpointStorage

logger = logging.getLogger(__name__)


class _PBTTrialState:
    """Internal PBT state tracked per-trial."""

    def __init__(self, trial: Trial):
        self.orig_tag = trial.experiment_tag
        self.last_score = None
        self.last_checkpoint = None
        self.last_perturbation_time = 0
        self.last_train_time = 0  # Used for synchronous mode.
        self.last_result = None  # Used for synchronous mode.

    def __repr__(self) -> str:
        return str(
            (
                self.last_score,
                self.last_checkpoint,
                self.last_train_time,
                self.last_perturbation_time,
            )
        )


def _explore(
    config: Dict,
    mutations: Dict,
    resample_probability: float,
    custom_explore_fn: Optional[Callable],
) -> Dict:
    """Return a config perturbed as specified.

    Args:
        config: Original hyperparameter configuration.
        mutations: Specification of mutations to perform as documented
            in the PopulationBasedTraining scheduler.
        resample_probability: Probability of allowing resampling of a
            particular variable.
        custom_explore_fn: Custom explore fn applied after built-in
            config perturbations are.
    """
    new_config = copy.deepcopy(config)
    for key, distribution in mutations.items():
        if isinstance(distribution, dict):
            new_config.update(
                {key: _explore(config[key], mutations[key], resample_probability, None)}
            )
        elif isinstance(distribution, list):
            if (
                random.random() < resample_probability
                or config[key] not in distribution
            ):
                new_config[key] = random.choice(distribution)
            elif random.random() > 0.5:
                new_config[key] = distribution[
                    max(0, distribution.index(config[key]) - 1)
                ]
            else:
                new_config[key] = distribution[
                    min(len(distribution) - 1, distribution.index(config[key]) + 1)
                ]
        else:
            if random.random() < resample_probability:
                new_config[key] = (
                    distribution.sample(None)
                    if isinstance(distribution, Domain)
                    else distribution()
                )
            elif random.random() > 0.5:
                new_config[key] = config[key] * 1.2
            else:
                new_config[key] = config[key] * 0.8
            if isinstance(config[key], int):
                new_config[key] = int(new_config[key])
    if custom_explore_fn:
        new_config = custom_explore_fn(new_config)
        assert new_config is not None, "Custom explore fn failed to return new config"
    return new_config


def _make_experiment_tag(orig_tag: str, config: Dict, mutations: Dict) -> str:
    """Appends perturbed params to the trial name to show in the console."""

    resolved_vars = {}
    for k in mutations.keys():
        resolved_vars[("config", k)] = config[k]
    return "{}@perturbed[{}]".format(orig_tag, format_vars(resolved_vars))


def _fill_config(
    config: Dict, attr: str, search_space: Union[Callable, Domain, list, dict]
):
    """Add attr to config by sampling from search_space."""
    if callable(search_space):
        config[attr] = search_space()
    elif isinstance(search_space, Domain):
        config[attr] = search_space.sample(None)
    elif isinstance(search_space, list):
        config[attr] = random.choice(search_space)
    elif isinstance(search_space, dict):
        config[attr] = {}
        for k, v in search_space.items():
            _fill_config(config[attr], k, v)


@PublicAPI
class PopulationBasedTraining(FIFOScheduler):
    """Implements the Population Based Training (PBT) algorithm.

    https://www.deepmind.com/blog/population-based-training-of-neural-networks

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
        time_attr: The training result attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        metric: The training result objective value attribute. Stopping
            procedures will use this attribute. If None but a mode was passed,
            the `ray.tune.result.DEFAULT_METRIC` will be used per default.
        mode: One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        perturbation_interval: Models will be considered for
            perturbation at this interval of `time_attr`. Note that
            perturbation incurs checkpoint overhead, so you shouldn't set this
            to be too frequent.
        burn_in_period: Models will not be considered for
            perturbation before this interval of `time_attr` has passed. This
            guarantees that models are trained for at least a certain amount
            of time or timesteps before being perturbed.
        hyperparam_mutations: Hyperparams to mutate. The format is
            as follows: for each key, either a list, function,
            or a tune search space object (tune.loguniform, tune.uniform,
            etc.) can be provided. A list specifies an allowed set of
            categorical values. A function or tune search space object
            specifies the distribution of a continuous parameter. You must
            use tune.choice, tune.uniform, tune.loguniform, etc.. Arbitrary
            tune.sample_from objects are not supported.
            You must specify at least one of `hyperparam_mutations` or
            `custom_explore_fn`.
            Tune will use the search space provided by
            `hyperparam_mutations` for the initial samples if the
            corresponding attributes are not present in `config`.
        quantile_fraction: Parameters are transferred from the top
            `quantile_fraction` fraction of trials to the bottom
            `quantile_fraction` fraction. Needs to be between 0 and 0.5.
            Setting it to 0 essentially implies doing no exploitation at all.
        resample_probability: The probability of resampling from the
            original distribution when applying `hyperparam_mutations`. If not
            resampled, the value will be perturbed by a factor of 1.2 or 0.8
            if continuous, or changed to an adjacent value if discrete.
        custom_explore_fn: You can also specify a custom exploration
            function. This function is invoked as `f(config)` after built-in
            perturbations from `hyperparam_mutations` are applied, and should
            return `config` updated as needed. You must specify at least one of
            `hyperparam_mutations` or `custom_explore_fn`.
        log_config: Whether to log the ray config of each model to
            local_dir at each exploit. Allows config schedule to be
            reconstructed.
        require_attrs: Whether to require time_attr and metric to appear
            in result for every iteration. If True, error will be raised
            if these values are not present in trial result.
        synch: If False, will use asynchronous implementation of
            PBT. Trial perturbations occur every perturbation_interval for each
            trial independently. If True, will use synchronous implementation
            of PBT. Perturbations will occur only after all trials are
            synced at the same time_attr every perturbation_interval.
            Defaults to False. See Appendix A.1 here
            https://arxiv.org/pdf/1711.09846.pdf.

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
                # Alternatively, use tune search space primitives.
                # The search space for factor_1 is equivalent to factor_2.
                "factor_2": tune.uniform(0.0, 20.0),
                # Perturb factor3 by changing it to an adjacent value, e.g.
                # 10 -> 1 or 10 -> 100. Resampling will choose at random.
                "factor_3": [1, 10, 100, 1000, 10000],
                # Using tune.choice is NOT equivalent to the above.
                # factor_4 is treated as a continuous hyperparameter.
                "factor_4": tune.choice([1, 10, 100, 1000, 10000]),
            })
        tune.run({...}, num_samples=8, scheduler=pbt)
    """

    def __init__(
        self,
        time_attr: str = "time_total_s",
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        perturbation_interval: float = 60.0,
        burn_in_period: float = 0.0,
        hyperparam_mutations: Dict = None,
        quantile_fraction: float = 0.25,
        resample_probability: float = 0.25,
        custom_explore_fn: Optional[Callable] = None,
        log_config: bool = True,
        require_attrs: bool = True,
        synch: bool = False,
    ):
        hyperparam_mutations = hyperparam_mutations or {}
        for value in hyperparam_mutations.values():
            if not (isinstance(value, (list, dict, Domain)) or callable(value)):
                raise TypeError(
                    "`hyperparam_mutation` values must be either "
                    "a List, Dict, a tune search space object, or "
                    "a callable."
                )
            if isinstance(value, Function):
                raise ValueError(
                    "arbitrary tune.sample_from objects are not "
                    "supported for `hyperparam_mutation` values."
                    "You must use other built in primitives like"
                    "tune.uniform, tune.loguniform, etc."
                )

            if not hyperparam_mutations and not custom_explore_fn:
                raise TuneError(
                    "You must specify at least one of `hyperparam_mutations` "
                    "or `custom_explore_fn` to use PBT."
                )

        if quantile_fraction > 0.5 or quantile_fraction < 0:
            raise ValueError(
                "You must set `quantile_fraction` to a value between 0 and"
                "0.5. Current value: '{}'".format(quantile_fraction)
            )

        if perturbation_interval <= 0:
            raise ValueError(
                "perturbation_interval must be a positive number greater "
                "than 0. Current value: '{}'".format(perturbation_interval)
            )

        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."

        FIFOScheduler.__init__(self)
        self._metric = metric
        self._mode = mode
        self._metric_op = None
        if self._mode == "max":
            self._metric_op = 1.0
        elif self._mode == "min":
            self._metric_op = -1.0
        self._time_attr = time_attr
        self._perturbation_interval = perturbation_interval
        self._burn_in_period = burn_in_period
        self._hyperparam_mutations = hyperparam_mutations
        self._quantile_fraction = quantile_fraction
        self._resample_probability = resample_probability
        self._trial_state = {}
        self._custom_explore_fn = custom_explore_fn
        self._log_config = log_config
        self._require_attrs = require_attrs
        self._synch = synch
        self._next_perturbation_sync = max(
            self._perturbation_interval,
            self._burn_in_period,
        )

        # Metrics
        self._num_checkpoints = 0
        self._num_perturbations = 0

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], **spec
    ) -> bool:
        if self._metric and metric:
            return False
        if self._mode and mode:
            return False

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        if self._mode == "max":
            self._metric_op = 1.0
        elif self._mode == "min":
            self._metric_op = -1.0

        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        return True

    def on_trial_add(self, trial_runner: "trial_runner.TrialRunner", trial: Trial):
        if trial_runner.search_alg is not None and isinstance(
            trial_runner.search_alg, SearchGenerator
        ):
            raise ValueError(
                "Search algorithms cannot be used with {} "
                "schedulers. Please remove {}.".format(
                    self.__class__.__name__, trial_runner.search_alg
                )
            )

        if not self._metric or not self._metric_op:
            raise ValueError(
                "{} has been instantiated without a valid `metric` ({}) or "
                "`mode` ({}) parameter. Either pass these parameters when "
                "instantiating the scheduler, or pass them as parameters "
                "to `tune.run()`".format(
                    self.__class__.__name__, self._metric, self._mode
                )
            )

        self._trial_state[trial] = _PBTTrialState(trial)

        for attr in self._hyperparam_mutations.keys():
            if attr not in trial.config:
                if log_once(attr + "-missing"):
                    logger.debug(
                        "Cannot find {} in config. Using search "
                        "space provided by hyperparam_mutations."
                    )
                # Add attr to trial's config by sampling search space from
                # hyperparam_mutations.
                _fill_config(trial.config, attr, self._hyperparam_mutations[attr])
                # Make sure this attribute is added to CLI output.
                trial.evaluated_params[attr] = trial.config[attr]

    def on_trial_result(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, result: Dict
    ) -> str:
        if self._time_attr not in result:
            time_missing_msg = (
                "Cannot find time_attr {} "
                "in trial result {}. Make sure that this "
                "attribute is returned in the "
                "results of your Trainable.".format(self._time_attr, result)
            )
            if self._require_attrs:
                raise RuntimeError(
                    time_missing_msg
                    + "If this error is expected, you can change this to "
                    "a warning message by "
                    "setting PBT(require_attrs=False)"
                )
            else:
                if log_once("pbt-time_attr-error"):
                    logger.warning(time_missing_msg)
        if self._metric not in result:
            metric_missing_msg = (
                "Cannot find metric {} in trial result {}. "
                "Make sure that this attribute is returned "
                "in the "
                "results of your Trainable.".format(self._metric, result)
            )
            if self._require_attrs:
                raise RuntimeError(
                    metric_missing_msg + "If this error is expected, "
                    "you can change this to a warning message by "
                    "setting PBT(require_attrs=False)"
                )
            else:
                if log_once("pbt-metric-error"):
                    logger.warning(metric_missing_msg)

        if self._metric not in result or self._time_attr not in result:
            return TrialScheduler.CONTINUE

        time = result[self._time_attr]
        state = self._trial_state[trial]

        # Continue training if burn-in period has not been reached, yet.
        if time < self._burn_in_period:
            return TrialScheduler.CONTINUE

        # Continue training if perturbation interval has not been reached, yet.
        if time - state.last_perturbation_time < self._perturbation_interval:
            return TrialScheduler.CONTINUE  # avoid checkpoint overhead

        self._save_trial_state(state, time, result, trial)

        if not self._synch:
            state.last_perturbation_time = time
            lower_quantile, upper_quantile = self._quantiles()
            decision = TrialScheduler.CONTINUE
            for other_trial in trial_runner.get_trials():
                if other_trial.status in [Trial.PENDING, Trial.PAUSED]:
                    decision = TrialScheduler.PAUSE
                    break
            self._checkpoint_or_exploit(
                trial, trial_runner.trial_executor, upper_quantile, lower_quantile
            )
            return TrialScheduler.NOOP if trial.status == Trial.PAUSED else decision
        else:
            # Synchronous mode.
            if any(
                self._trial_state[t].last_train_time < self._next_perturbation_sync
                and t != trial
                for t in trial_runner.get_live_trials()
            ):
                logger.debug("Pausing trial {}".format(trial))
            else:
                # All trials are synced at the same timestep.
                lower_quantile, upper_quantile = self._quantiles()
                all_trials = trial_runner.get_trials()
                not_in_quantile = []
                for t in all_trials:
                    if t not in lower_quantile and t not in upper_quantile:
                        not_in_quantile.append(t)
                # Move upper quantile trials to beginning and lower quantile
                # to end. This ensures that checkpointing of strong trials
                # occurs before exploiting of weaker ones.
                all_trials = upper_quantile + not_in_quantile + lower_quantile
                for t in all_trials:
                    logger.debug("Perturbing Trial {}".format(t))
                    self._trial_state[t].last_perturbation_time = time
                    self._checkpoint_or_exploit(
                        t, trial_runner.trial_executor, upper_quantile, lower_quantile
                    )

                all_train_times = [
                    self._trial_state[t].last_train_time
                    for t in trial_runner.get_trials()
                ]
                max_last_train_time = max(all_train_times)
                self._next_perturbation_sync = max(
                    self._next_perturbation_sync + self._perturbation_interval,
                    max_last_train_time,
                )
            # In sync mode we should pause all trials once result comes in.
            # Once a perturbation step happens for all trials, they should
            # still all be paused.
            # choose_trial_to_run will then pick the next trial to run out of
            # the paused trials.
            return (
                TrialScheduler.NOOP
                if trial.status == Trial.PAUSED
                else TrialScheduler.PAUSE
            )

    def _save_trial_state(
        self, state: _PBTTrialState, time: int, result: Dict, trial: Trial
    ):
        """Saves necessary trial information when result is received.
        Args:
            state: The state object for the trial.
            time: The current timestep of the trial.
            result: The trial's result dictionary.
            trial: The trial object.
        """

        # This trial has reached its perturbation interval.
        # Record new state in the state object.
        score = self._metric_op * result[self._metric]
        state.last_score = score
        state.last_train_time = time
        state.last_result = result

        return score

    def _checkpoint_or_exploit(
        self,
        trial: Trial,
        trial_executor: "trial_runner.RayTrialExecutor",
        upper_quantile: List[Trial],
        lower_quantile: List[Trial],
    ):
        """Checkpoint if in upper quantile, exploits if in lower."""
        state = self._trial_state[trial]
        if trial in upper_quantile:
            # The trial last result is only updated after the scheduler
            # callback. So, we override with the current result.
            logger.debug("Trial {} is in upper quantile".format(trial))
            logger.debug("Checkpointing {}".format(trial))
            if trial.status == Trial.PAUSED:
                # Paused trial will always have an in-memory checkpoint.
                state.last_checkpoint = trial.checkpoint
            else:
                state.last_checkpoint = trial_executor.save(
                    trial, CheckpointStorage.MEMORY, result=state.last_result
                )
            self._num_checkpoints += 1
        else:
            state.last_checkpoint = None  # not a top trial

        if trial in lower_quantile:
            logger.debug("Trial {} is in lower quantile".format(trial))
            trial_to_clone = random.choice(upper_quantile)
            assert trial is not trial_to_clone
            if not self._trial_state[trial_to_clone].last_checkpoint:
                logger.info(
                    "[pbt]: no checkpoint for trial."
                    " Skip exploit for Trial {}".format(trial)
                )
                return
            self._exploit(trial_executor, trial, trial_to_clone)

    def _log_config_on_step(
        self,
        trial_state: _PBTTrialState,
        new_state: _PBTTrialState,
        trial: Trial,
        trial_to_clone: Trial,
        new_config: Dict,
    ):
        """Logs transition during exploit/exploit step.

        For each step, logs: [target trial tag, clone trial tag, target trial
        iteration, clone trial iteration, old config, new config].
        """
        trial_name, trial_to_clone_name = (trial_state.orig_tag, new_state.orig_tag)
        trial_id = trial.trial_id
        trial_to_clone_id = trial_to_clone.trial_id
        trial_path = os.path.join(trial.local_dir, "pbt_policy_" + trial_id + ".txt")
        trial_to_clone_path = os.path.join(
            trial_to_clone.local_dir, "pbt_policy_" + trial_to_clone_id + ".txt"
        )
        policy = [
            trial_name,
            trial_to_clone_name,
            trial.last_result.get(TRAINING_ITERATION, 0),
            trial_to_clone.last_result.get(TRAINING_ITERATION, 0),
            trial_to_clone.config,
            new_config,
        ]
        # Log to global file.
        with open(os.path.join(trial.local_dir, "pbt_global.txt"), "a+") as f:
            print(json.dumps(policy, cls=SafeFallbackEncoder), file=f)
        # Overwrite state in target trial from trial_to_clone.
        if os.path.exists(trial_to_clone_path):
            shutil.copyfile(trial_to_clone_path, trial_path)
        # Log new exploit in target trial log.
        with open(trial_path, "a+") as f:
            f.write(json.dumps(policy, cls=SafeFallbackEncoder) + "\n")

    def _get_new_config(self, trial, trial_to_clone):
        """Gets new config for trial by exploring trial_to_clone's config."""
        return _explore(
            trial_to_clone.config,
            self._hyperparam_mutations,
            self._resample_probability,
            self._custom_explore_fn,
        )

    def _exploit(
        self,
        trial_executor: "trial_runner.RayTrialExecutor",
        trial: Trial,
        trial_to_clone: Trial,
    ):
        """Transfers perturbed state from trial_to_clone -> trial.

        If specified, also logs the updated hyperparam state.
        """
        trial_state = self._trial_state[trial]
        new_state = self._trial_state[trial_to_clone]
        logger.info(
            "[exploit] transferring weights from trial "
            "{} (score {}) -> {} (score {})".format(
                trial_to_clone, new_state.last_score, trial, trial_state.last_score
            )
        )

        new_config = self._get_new_config(trial, trial_to_clone)

        # Only log mutated hyperparameters and not entire config.
        old_hparams = {
            k: v
            for k, v in trial_to_clone.config.items()
            if k in self._hyperparam_mutations
        }
        new_hparams = {
            k: v for k, v in new_config.items() if k in self._hyperparam_mutations
        }
        logger.info(
            "[explore] perturbed config from {} -> {}".format(old_hparams, new_hparams)
        )

        if self._log_config:
            self._log_config_on_step(
                trial_state, new_state, trial, trial_to_clone, new_config
            )

        new_tag = _make_experiment_tag(
            trial_state.orig_tag, new_config, self._hyperparam_mutations
        )
        if trial.status == Trial.PAUSED:
            # If trial is paused we update it with a new checkpoint.
            # When the trial is started again, the new checkpoint is used.
            if not self._synch:
                raise TuneError(
                    "Trials should be paused here only if in "
                    "synchronous mode. If you encounter this error"
                    " please raise an issue on Ray Github."
                )
        else:
            trial_executor.stop_trial(trial)
            trial_executor.set_status(trial, Trial.PAUSED)
        trial.set_experiment_tag(new_tag)
        trial.set_config(new_config)
        trial.on_checkpoint(new_state.last_checkpoint)

        self._num_perturbations += 1
        # Transfer over the last perturbation time as well
        trial_state.last_perturbation_time = new_state.last_perturbation_time
        trial_state.last_train_time = new_state.last_train_time

    def _quantiles(self) -> Tuple[List[Trial], List[Trial]]:
        """Returns trials in the lower and upper `quantile` of the population.

        If there is not enough data to compute this, returns empty lists.
        """
        trials = []
        for trial, state in self._trial_state.items():
            logger.debug("Trial {}, state {}".format(trial, state))
            if trial.is_finished():
                logger.debug("Trial {} is finished".format(trial))
            if state.last_score is not None and not trial.is_finished():
                trials.append(trial)
        trials.sort(key=lambda t: self._trial_state[t].last_score)

        if len(trials) <= 1:
            return [], []
        else:
            num_trials_in_quantile = int(
                math.ceil(len(trials) * self._quantile_fraction)
            )
            if num_trials_in_quantile > len(trials) / 2:
                num_trials_in_quantile = int(math.floor(len(trials) / 2))
            return (trials[:num_trials_in_quantile], trials[-num_trials_in_quantile:])

    def choose_trial_to_run(
        self, trial_runner: "trial_runner.TrialRunner"
    ) -> Optional[Trial]:
        """Ensures all trials get fair share of time (as defined by time_attr).

        This enables the PBT scheduler to support a greater number of
        concurrent trials than can fit in the cluster at any given time.
        """
        candidates = []
        for trial in trial_runner.get_trials():
            if (
                trial.status
                in [
                    Trial.PENDING,
                    Trial.PAUSED,
                ]
                and trial_runner.trial_executor.has_resources_for_trial(trial)
            ):
                if not self._synch:
                    candidates.append(trial)
                elif (
                    self._trial_state[trial].last_train_time
                    < self._next_perturbation_sync
                ):
                    candidates.append(trial)
        candidates.sort(key=lambda trial: self._trial_state[trial].last_train_time)
        return candidates[0] if candidates else None

    # Unit test only. TODO(xwjiang): Remove test-specific APIs.
    def reset_stats(self):
        self._num_perturbations = 0
        self._num_checkpoints = 0

    # Unit test only. TODO(xwjiang): Remove test-specific APIs.
    def last_scores(self, trials: List[Trial]) -> List[float]:
        scores = []
        for trial in trials:
            state = self._trial_state[trial]
            if state.last_score is not None and not trial.is_finished():
                scores.append(state.last_score)
        return scores

    def debug_string(self) -> str:
        return "PopulationBasedTraining: {} checkpoints, {} perturbs".format(
            self._num_checkpoints, self._num_perturbations
        )


@PublicAPI
class PopulationBasedTrainingReplay(FIFOScheduler):
    """Replays a Population Based Training run.

    Population Based Training does not return a single hyperparameter
    configuration, but rather a schedule of configurations. For instance,
    PBT might discover that a larger learning rate leads to good results
    in the first training iterations, but that a smaller learning rate
    is preferable later.

    This scheduler enables replaying these parameter schedules from
    a finished PBT run. This requires that population based training has
    been run with ``log_config=True``, which is the default setting.

    The scheduler will only accept and train a single trial. It will
    start with the initial config of the existing trial and update the
    config according to the schedule.

    Args:
        policy_file: The PBT policy file. Usually this is
            stored in ``~/ray_results/experiment_name/pbt_policy_xxx.txt``
            where ``xxx`` is the trial ID.

    Example:

    .. code-block:: python

        # Replaying a result from ray.tune.examples.pbt_convnet_example
        from ray import tune

        from ray.tune.examples.pbt_convnet_example import PytorchTrainable
        from ray.tune.schedulers import PopulationBasedTrainingReplay

        replay = PopulationBasedTrainingReplay(
            "~/ray_results/pbt_test/pbt_policy_XXXXX_00001.txt")

        tune.run(
            PytorchTrainable,
            scheduler=replay,
            stop={"training_iteration": 100})


    """

    def __init__(self, policy_file: str):
        policy_file = os.path.expanduser(policy_file)
        if not os.path.exists(policy_file):
            raise ValueError("Policy file not found: {}".format(policy_file))

        self.policy_file = policy_file

        # Find and read pbt policy file, potentially raise error
        initial_config, self._policy = self._load_policy(self.policy_file)

        self.experiment_tag = "replay_{}".format(os.path.basename(self.policy_file))
        self.config = initial_config
        self.current_config = self.config

        self._trial = None
        self._current_step = 0
        self._num_perturbations = 0

        self._policy_iter = iter(self._policy)
        self._next_policy = next(self._policy_iter, None)

    def _load_policy(self, policy_file: str) -> Tuple[Dict, List[Tuple[int, Dict]]]:
        raw_policy = []
        with open(policy_file, "rt") as fp:
            for row in fp.readlines():
                try:
                    parsed_row = json.loads(row)
                except json.JSONDecodeError:
                    raise ValueError(
                        "Could not read PBT policy file: {}.".format(policy_file)
                    ) from None
                raw_policy.append(tuple(parsed_row))

        # Loop through policy from end to start to obtain changepoints
        policy = []
        last_new_tag = None
        last_old_conf = None
        for (old_tag, new_tag, old_step, new_step, old_conf, new_conf) in reversed(
            raw_policy
        ):
            if last_new_tag and old_tag != last_new_tag:
                # Tag chain ended. This means that previous changes were
                # overwritten by the last change and should be ignored.
                break
            last_new_tag = new_tag
            last_old_conf = old_conf

            policy.append((new_step, new_conf))

        return last_old_conf, list(reversed(policy))

    def on_trial_add(self, trial_runner: "trial_runner.TrialRunner", trial: Trial):
        if self._trial:
            raise ValueError(
                "More than one trial added to PBT replay run. This "
                "means the same schedule will be trained multiple "
                "times. Do you want to set `n_samples=1`?"
            )
        self._trial = trial
        if self._trial.config and self._policy:
            logger.warning(
                "Trial was initialized with a config, which was overwritten. "
                "Did you start the PBT replay with a `config` parameter?"
            )
        elif self._trial.config and not self._policy:
            # Only train with initial policy
            self.config = self._trial.config
        elif not self._trial.config and not self._policy:
            raise ValueError(
                "No replay policy found and trial initialized without a "
                "valid config. Either pass a `config` argument to `tune.run()`"
                "or consider not using PBT replay for this run."
            )
        self._trial.set_config(self.config)

    def on_trial_result(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, result: Dict
    ) -> str:
        if TRAINING_ITERATION not in result:
            # No time reported
            return TrialScheduler.CONTINUE

        if not self._next_policy:
            # No more changes in the config
            return TrialScheduler.CONTINUE

        step = result[TRAINING_ITERATION]
        self._current_step = step

        change_at, new_config = self._next_policy

        if step < change_at:
            # Don't change the policy just yet
            return TrialScheduler.CONTINUE

        logger.info(
            "Population Based Training replay is now at step {}. "
            "Configuration will be changed to {}.".format(step, new_config)
        )

        checkpoint = trial_runner.trial_executor.save(
            trial, CheckpointStorage.MEMORY, result=result
        )

        new_tag = _make_experiment_tag(self.experiment_tag, new_config, new_config)

        trial_executor = trial_runner.trial_executor
        trial_executor.stop_trial(trial)
        trial_executor.set_status(trial, Trial.PAUSED)
        trial.set_experiment_tag(new_tag)
        trial.set_config(new_config)
        trial.on_checkpoint(checkpoint)

        self.current_config = new_config
        self._num_perturbations += 1
        self._next_policy = next(self._policy_iter, None)

        return TrialScheduler.NOOP

    def debug_string(self) -> str:
        return "PopulationBasedTraining replay: Step {}, perturb {}".format(
            self._current_step, self._num_perturbations
        )
