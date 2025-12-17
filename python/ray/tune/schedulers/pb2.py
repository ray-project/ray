import logging
from copy import deepcopy
from typing import TYPE_CHECKING, Callable, Dict, Optional, Tuple, Union

import numpy as np
import pandas as pd

from ray.tune import TuneError
from ray.tune.experiment import Trial
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.schedulers.pbt import _PBTTrialState
from ray.tune.utils.util import flatten_dict, unflatten_dict
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.tune.execution.tune_controller import TuneController


def import_pb2_dependencies():
    try:
        import sklearn
    except ImportError:
        sklearn = None
    return sklearn


has_sklearn = import_pb2_dependencies()

if has_sklearn:
    from sklearn.gaussian_process import GaussianProcessRegressor

    from ray.tune.schedulers.pb2_utils import (
        UCB,
        TV_SquaredExp,
        normalize,
        optimize_acq,
        select_length,
        standardize,
    )

logger = logging.getLogger(__name__)


def _fill_config(
    config: Dict, hyperparam_bounds: Dict[str, Union[dict, list, tuple]]
) -> Dict:
    """Fills missing hyperparameters in config by sampling uniformly from the
    specified `hyperparam_bounds`.
    Recursively fills the config if `hyperparam_bounds` is a nested dict.

    This is a helper used to set initial hyperparameter values if the user doesn't
    specify them in the Tuner `param_space`.

    Returns the dict of filled hyperparameters.
    """
    filled_hyperparams = {}
    for param_name, bounds in hyperparam_bounds.items():
        if isinstance(bounds, dict):
            if param_name not in config:
                config[param_name] = {}
            filled_hyperparams[param_name] = _fill_config(config[param_name], bounds)
        elif isinstance(bounds, (list, tuple)) and param_name not in config:
            if log_once(param_name + "-missing"):
                logger.debug(
                    f"Cannot find {param_name} in config. Initializing by "
                    "sampling uniformly from the provided `hyperparam_bounds`."
                )
            assert len(bounds) == 2
            low, high = bounds
            config[param_name] = filled_hyperparams[param_name] = np.random.uniform(
                low, high
            )
    return filled_hyperparams


def _select_config(
    Xraw: np.array,
    yraw: np.array,
    current: list,
    newpoint: np.array,
    bounds: dict,
    num_f: int,
) -> np.ndarray:
    """Selects the next hyperparameter config to try.

    This function takes the formatted data, fits the GP model and optimizes the
    UCB acquisition function to select the next point.

    Args:
        Xraw: The un-normalized array of hyperparams, Time and
            Reward
        yraw: The un-normalized vector of reward changes.
        current: The hyperparams of trials currently running. This is
            important so we do not select the same config twice. If there is
            data here then we fit a second GP including it
            (with fake y labels). The GP variance doesn't depend on the y
            labels so it is ok.
        newpoint: The Reward and Time for the new point.
            We cannot change these as they are based on the *new weights*.
        bounds: Bounds for the hyperparameters. Used to normalize.
        num_f: The number of fixed params. Almost always 2 (reward+time)

    Return:
        xt: A vector of new hyperparameters.
    """
    length = select_length(Xraw, yraw, bounds, num_f)

    Xraw = Xraw[-length:, :]
    yraw = yraw[-length:]

    base_vals = np.array(list(bounds.values())).T
    oldpoints = Xraw[:, :num_f]
    old_lims = np.concatenate(
        (np.max(oldpoints, axis=0), np.min(oldpoints, axis=0))
    ).reshape(2, oldpoints.shape[1])
    limits = np.concatenate((old_lims, base_vals), axis=1)

    X = normalize(Xraw, limits)
    y = standardize(yraw).reshape(yraw.size, 1)

    fixed = normalize(newpoint, oldpoints)

    kernel = TV_SquaredExp(variance=1.0, lengthscale=1.0, epsilon=0.1)

    try:
        m = GaussianProcessRegressor(
            kernel=kernel, optimizer="fmin_l_bfgs_b", alpha=1e-10
        )
        m.fit(X, y)
    except np.linalg.LinAlgError:
        # add diagonal ** we would ideally make this something more robust...
        X += np.eye(X.shape[0]) * 1e-3
        m = GaussianProcessRegressor(
            kernel=kernel, optimizer="fmin_l_bfgs_b", alpha=1e-10
        )
        m.fit(X, y)

    if current is None:
        m1 = deepcopy(m)
    else:
        # add the current trials to the dataset
        padding = np.array([fixed for _ in range(current.shape[0])])
        current = normalize(current, base_vals)
        current = np.hstack((padding, current))

        Xnew = np.vstack((X, current))
        ypad = np.zeros(current.shape[0])
        ypad = ypad.reshape(-1, 1)
        ynew = np.vstack((y, ypad))

        kernel1 = TV_SquaredExp(variance=1.0, lengthscale=1.0, epsilon=0.1)
        m1 = GaussianProcessRegressor(
            kernel=kernel1, optimizer="fmin_l_bfgs_b", alpha=1e-10
        )
        m1.fit(Xnew, ynew)

    xt = optimize_acq(UCB, m, m1, fixed, num_f)

    # convert back...
    xt = xt * (np.max(base_vals, axis=0) - np.min(base_vals, axis=0)) + np.min(
        base_vals, axis=0
    )

    xt = xt.astype(np.float32)
    return xt


def _explore(
    data: pd.DataFrame,
    bounds: Dict[str, Tuple[float, float]],
    current: list,
    base: Trial,
    old: Trial,
    config: Dict[str, Tuple[float, float]],
) -> Tuple[Dict, pd.DataFrame]:
    """Returns next hyperparameter configuration to use.

    This function primarily processes the data from completed trials
    and then requests the next config from the select_config function.
    It then adds the new trial to the dataframe, so that the reward change
    can be computed using the new weights.
    It returns the new point and the dataframe with the new entry.
    """

    df = data.sort_values(by="Time").reset_index(drop=True)

    # Group by trial ID and hyperparams.
    # Compute change in timesteps and reward.
    df["y"] = df.groupby(["Trial"] + list(bounds.keys()))["Reward"].diff()
    df["t_change"] = df.groupby(["Trial"] + list(bounds.keys()))["Time"].diff()

    # Delete entries without positive change in t.
    df = df[df["t_change"] > 0].reset_index(drop=True)
    df["R_before"] = df.Reward - df.y

    # Normalize the reward change by the update size.
    # For example if trials took diff lengths of time.
    df["y"] = df.y / df.t_change
    df = df[~df.y.isna()].reset_index(drop=True)
    df = df.sort_values(by="Time").reset_index(drop=True)

    # Only use the last 1k datapoints, so the GP is not too slow.
    df = df.iloc[-1000:, :].reset_index(drop=True)

    # We need this to know the T and Reward for the weights.
    dfnewpoint = df[df["Trial"] == str(base)]

    if not dfnewpoint.empty:
        # N ow specify the dataset for the GP.
        y = np.array(df.y.values)
        # Meta data we keep -> episodes and reward.
        # (TODO: convert to curve)
        t_r = df[["Time", "R_before"]]
        hparams = df[bounds.keys()]
        X = pd.concat([t_r, hparams], axis=1).values
        newpoint = df[df["Trial"] == str(base)].iloc[-1, :][["Time", "R_before"]].values
        new = _select_config(X, y, current, newpoint, bounds, num_f=len(t_r.columns))

        new_config = config.copy()
        values = []
        # Cast types for new hyperparameters.
        for i, col in enumerate(hparams.columns):
            # Use the type from the old config. Like this types
            # should be passed on from the first config downwards.
            type_ = type(config[col])
            new_config[col] = type_(new[i])
            values.append(type_(new[i]))

        new_T = df[df["Trial"] == str(base)].iloc[-1, :]["Time"]
        new_Reward = df[df["Trial"] == str(base)].iloc[-1, :].Reward

        lst = [[str(old)] + [new_T] + values + [new_Reward]]
        cols = ["Trial", "Time"] + list(bounds) + ["Reward"]
        new_entry = pd.DataFrame(lst, columns=cols)

        # Create an entry for the new config, with the reward from the
        # copied agent.
        data = pd.concat([data, new_entry]).reset_index(drop=True)

    else:
        new_config = config.copy()

    return new_config, data


class PB2(PopulationBasedTraining):
    """Implements the Population Based Bandit (PB2) algorithm.

    PB2 trains a group of models (or agents) in parallel. Periodically, poorly
    performing models clone the state of the top performers, and the hyper-
    parameters are re-selected using GP-bandit optimization. The GP model is
    trained to predict the improvement in the next training period.

    Like PBT, PB2 adapts hyperparameters during training time. This enables
    very fast hyperparameter discovery and also automatically discovers
    schedules.

    This Tune PB2 implementation is built on top of Tune's PBT implementation.
    It considers all trials added as part of the PB2 population. If the number
    of trials exceeds the cluster capacity, they will be time-multiplexed as to
    balance training progress across the population. To run multiple trials,
    use `tune.TuneConfig(num_samples=<int>)`.

    In {LOG_DIR}/{MY_EXPERIMENT_NAME}/, all mutations are logged in
    `pb2_global.txt` and individual policy perturbations are recorded
    in pb2_policy_{i}.txt. Tune logs: [target trial tag, clone trial tag,
    target trial iteration, clone trial iteration, old config, new config]
    on each perturbation step.

    Args:
        time_attr: The training result attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        metric: The training result objective value attribute. Stopping
            procedures will use this attribute.
        mode: One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        perturbation_interval: Models will be considered for
            perturbation at this interval of `time_attr`. Note that
            perturbation incurs checkpoint overhead, so you shouldn't set this
            to be too frequent.
        hyperparam_bounds: Hyperparameters to mutate. The format is
            as follows: for each key, enter a list of the form [min, max]
            representing the minimum and maximum possible hyperparam values.
            A key can also hold a dict for nested hyperparameters.
            Tune will sample uniformly between the bounds provided by
            `hyperparam_bounds` for the initial hyperparameter values if the
            corresponding hyperparameters are not present in a trial's initial `config`.
        quantile_fraction: Parameters are transferred from the top
            `quantile_fraction` fraction of trials to the bottom
            `quantile_fraction` fraction. Needs to be between 0 and 0.5.
            Setting it to 0 essentially implies doing no exploitation at all.
        custom_explore_fn: You can also specify a custom exploration
            function. This function is invoked as `f(config)`, where the input
            is the new config generated by Bayesian Optimization. This function
            should return the `config` updated as needed.
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

    Example:

        .. code-block:: python

            from ray import tune
            from ray.tune.schedulers.pb2 import PB2
            from ray.tune.examples.pbt_function import pbt_function

            pb2 = PB2(
                metric="mean_accuracy",
                mode="max",
                perturbation_interval=20,
                hyperparam_bounds={"lr": [0.0001, 0.1]},
            )
            tuner = tune.Tuner(
                pbt_function,
                tune_config=tune.TuneConfig(
                    scheduler=pb2,
                    num_samples=8,
                ),
                param_space={"lr": 0.0001},
            )
            tuner.fit()

    """

    def __init__(
        self,
        time_attr: str = "time_total_s",
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        perturbation_interval: float = 60.0,
        hyperparam_bounds: Dict[str, Union[dict, list, tuple]] = None,
        quantile_fraction: float = 0.25,
        log_config: bool = True,
        require_attrs: bool = True,
        synch: bool = False,
        custom_explore_fn: Optional[Callable[[dict], dict]] = None,
    ):

        sklearn_available = import_pb2_dependencies()
        if not sklearn_available:
            raise RuntimeError("Please install scikit-learn to use PB2.")

        hyperparam_bounds = hyperparam_bounds or {}

        if not hyperparam_bounds:
            raise TuneError(
                "`hyperparam_bounds` must be specified to use PB2 scheduler."
            )

        super(PB2, self).__init__(
            time_attr=time_attr,
            metric=metric,
            mode=mode,
            perturbation_interval=perturbation_interval,
            hyperparam_mutations=hyperparam_bounds,
            quantile_fraction=quantile_fraction,
            resample_probability=0,
            custom_explore_fn=custom_explore_fn,
            log_config=log_config,
            require_attrs=require_attrs,
            synch=synch,
        )

        self.last_exploration_time = 0  # when we last explored
        self.data = pd.DataFrame()

        self._hyperparam_bounds = hyperparam_bounds
        self._hyperparam_bounds_flat = flatten_dict(
            hyperparam_bounds, prevent_delimiter=True
        )
        self._validate_hyperparam_bounds(self._hyperparam_bounds_flat)

        # Current = trials running that have already re-started after reaching
        #           the checkpoint. When exploring we care if these trials
        #           are already in or scheduled to be in the next round.
        self.current = None

    def on_trial_add(self, tune_controller: "TuneController", trial: Trial):
        filled_hyperparams = _fill_config(trial.config, self._hyperparam_bounds)
        # Make sure that the params we sampled show up in the CLI output
        trial.evaluated_params.update(flatten_dict(filled_hyperparams))
        super().on_trial_add(tune_controller, trial)

    def _validate_hyperparam_bounds(self, hyperparam_bounds: dict):
        """Check that each hyperparam bound is of the form [low, high].

        Raises:
            ValueError: if any of the hyperparam bounds are of an invalid format.
        """
        for key, value in hyperparam_bounds.items():
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise ValueError(
                    "`hyperparam_bounds` values must either be "
                    f"a list or tuple of size 2, but got {value} "
                    f"instead for the param '{key}'"
                )
            low, high = value
            if low > high:
                raise ValueError(
                    "`hyperparam_bounds` values must be of the form [low, high] "
                    f"where low <= high, but got {value} instead for param '{key}'."
                )

    def _save_trial_state(
        self, state: _PBTTrialState, time: int, result: Dict, trial: Trial
    ):
        score = super(PB2, self)._save_trial_state(state, time, result, trial)

        # Data logging for PB2.

        # Collect hyperparams names and current values for this trial.
        names = list(self._hyperparam_bounds_flat.keys())
        flattened_config = flatten_dict(trial.config)
        values = [flattened_config[key] for key in names]

        # Store trial state and hyperparams in dataframe.
        # this needs to be made more general.
        lst = [[trial, result[self._time_attr]] + values + [score]]
        cols = ["Trial", "Time"] + names + ["Reward"]
        entry = pd.DataFrame(lst, columns=cols)

        self.data = pd.concat([self.data, entry]).reset_index(drop=True)
        self.data.Trial = self.data.Trial.astype("str")

    def _get_new_config(self, trial: Trial, trial_to_clone: Trial) -> Tuple[Dict, Dict]:
        """Gets new config for trial by exploring trial_to_clone's config using
        Bayesian Optimization (BO) to choose the hyperparameter values to explore.

        Overrides `PopulationBasedTraining._get_new_config`.

        Args:
            trial: The current trial that decided to exploit trial_to_clone.
            trial_to_clone: The top-performing trial with a hyperparameter config
                that the current trial will explore.

        Returns:
            new_config: New hyperparameter configuration (after BO).
            operations: Empty dict since PB2 doesn't explore in easily labeled ways
                like PBT does.
        """
        # If we are at a new timestep, we dont want to penalise for trials
        # still going.
        if self.data["Time"].max() > self.last_exploration_time:
            self.current = None

        new_config_flat, data = _explore(
            self.data,
            self._hyperparam_bounds_flat,
            self.current,
            trial_to_clone,
            trial,
            flatten_dict(trial_to_clone.config),
        )

        # Important to replace the old values, since we are copying across
        self.data = data.copy()

        # If the current guy being selecting is at a point that is already
        # done, then append the data to the "current" which contains the
        # points in the current batch.
        new = [new_config_flat[key] for key in self._hyperparam_bounds_flat]

        new = np.array(new)
        new = new.reshape(1, new.size)
        if self.data["Time"].max() > self.last_exploration_time:
            self.last_exploration_time = self.data["Time"].max()
            self.current = new.copy()
        else:
            self.current = np.concatenate((self.current, new), axis=0)
            logger.debug(self.current)

        new_config = unflatten_dict(new_config_flat)

        if self._custom_explore_fn:
            new_config = self._custom_explore_fn(new_config)
            assert (
                new_config is not None
            ), "Custom explore function failed to return a new config"

        return new_config, {}
