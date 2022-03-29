from typing import Dict, Optional
from copy import deepcopy
import logging
import numpy as np
import pandas as pd

from ray.tune import TuneError
from ray.tune.schedulers import PopulationBasedTraining


def import_pb2_dependencies():
    try:
        import GPy
    except ImportError:
        GPy = None
    try:
        import sklearn
    except ImportError:
        sklearn = None
    return GPy, sklearn


GPy, has_sklearn = import_pb2_dependencies()

if GPy and has_sklearn:
    from ray.tune.schedulers.pb2_utils import (
        normalize,
        optimize_acq,
        select_length,
        UCB,
        standardize,
        TV_SquaredExp,
    )

logger = logging.getLogger(__name__)


def select_config(
    Xraw: np.array,
    yraw: np.array,
    current: list,
    newpoint: np.array,
    bounds: dict,
    num_f: int,
):
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

    kernel = TV_SquaredExp(
        input_dim=X.shape[1], variance=1.0, lengthscale=1.0, epsilon=0.1
    )

    try:
        m = GPy.models.GPRegression(X, y, kernel)
    except np.linalg.LinAlgError:
        # add diagonal ** we would ideally make this something more robust...
        X += np.eye(X.shape[0]) * 1e-3
        m = GPy.models.GPRegression(X, y, kernel)

    try:
        m.optimize()
    except np.linalg.LinAlgError:
        # add diagonal ** we would ideally make this something more robust...
        X += np.eye(X.shape[0]) * 1e-3
        m = GPy.models.GPRegression(X, y, kernel)
        m.optimize()

    m.kern.lengthscale.fix(m.kern.lengthscale.clip(1e-5, 1))

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

        # kernel = GPy.kern.RBF(input_dim=X.shape[1], variance=1.,
        # lengthscale=1.)
        kernel = TV_SquaredExp(
            input_dim=X.shape[1], variance=1.0, lengthscale=1.0, epsilon=0.1
        )
        m1 = GPy.models.GPRegression(Xnew, ynew, kernel)
        m1.optimize()

    xt = optimize_acq(UCB, m, m1, fixed, num_f)

    # convert back...
    xt = xt * (np.max(base_vals, axis=0) - np.min(base_vals, axis=0)) + np.min(
        base_vals, axis=0
    )

    xt = xt.astype(np.float32)
    return xt


def explore(data, bounds, current, base, old, config):
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
        new = select_config(X, y, current, newpoint, bounds, num_f=len(t_r.columns))

        new_config = config.copy()
        values = []
        for i, col in enumerate(hparams.columns):
            if isinstance(config[col], int):
                new_config[col] = int(new[i])
                values.append(int(new[i]))
            else:
                new_config[col] = new[i]
                values.append(new[i])

        new_T = df[df["Trial"] == str(base)].iloc[-1, :]["Time"]
        new_Reward = df[df["Trial"] == str(base)].iloc[-1, :].Reward

        lst = [[old] + [new_T] + values + [new_Reward]]
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
    use `tune.run(num_samples=<int>)`.

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
        quantile_fraction: Parameters are transferred from the top
            `quantile_fraction` fraction of trials to the bottom
            `quantile_fraction` fraction. Needs to be between 0 and 0.5.
            Setting it to 0 essentially implies doing no exploitation at all.
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
        >>> from ray import tune
        >>> from ray.tune.schedulers.pb2 import PB2
        >>> from ray.tune.examples.pbt_function import pbt_function
        >>> # run "pip install gpy" to use PB2
        >>> pb2 = PB2( # doctest: +SKIP
        ...     metric="mean_accuracy",
        ...     mode="max",
        ...     perturbation_interval=20,
        ...     hyperparam_bounds={
        ...     "factor": [0.0, 20.0],
        ... })
        >>> tune.run( # doctest: +SKIP
        ...     pbt_function, config={"lr": 0.0001}, num_samples=8, scheduler=pb2
        ... )
    """

    def __init__(
        self,
        time_attr: str = "time_total_s",
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        perturbation_interval: float = 60.0,
        hyperparam_bounds: Dict = None,
        quantile_fraction: float = 0.25,
        log_config: bool = True,
        require_attrs: bool = True,
        synch: bool = False,
    ):

        gpy_available, sklearn_available = import_pb2_dependencies()
        if not gpy_available:
            raise RuntimeError("Please install GPy to use PB2.")

        if not sklearn_available:
            raise RuntimeError("Please install scikit-learn to use PB2.")

        hyperparam_bounds = hyperparam_bounds or {}
        for value in hyperparam_bounds.values():
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise ValueError(
                    "`hyperparam_bounds` values must either be "
                    "a list or tuple of size 2, but got {} "
                    "instead".format(value)
                )

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
            custom_explore_fn=explore,
            log_config=log_config,
            require_attrs=require_attrs,
            synch=synch,
        )

        self.last_exploration_time = 0  # when we last explored
        self.data = pd.DataFrame()

        self._hyperparam_bounds = hyperparam_bounds

        # Current = trials running that have already re-started after reaching
        #           the checkpoint. When exploring we care if these trials
        #           are already in or scheduled to be in the next round.
        self.current = None

    def _save_trial_state(self, state, time, result, trial):

        score = super(PB2, self)._save_trial_state(state, time, result, trial)

        # Data logging for PB2.

        # Collect hyperparams names and current values for this trial.
        names = []
        values = []
        for key in self._hyperparam_bounds:
            names.append(str(key))
            values.append(trial.config[key])

        # Store trial state and hyperparams in dataframe.
        # this needs to be made more general.
        lst = [[trial, result[self._time_attr]] + values + [score]]
        cols = ["Trial", "Time"] + names + ["Reward"]
        entry = pd.DataFrame(lst, columns=cols)

        self.data = pd.concat([self.data, entry]).reset_index(drop=True)
        self.data.Trial = self.data.Trial.astype("str")

    def _get_new_config(self, trial, trial_to_clone):
        # If we are at a new timestep, we dont want to penalise for trials
        # still going.
        if self.data["Time"].max() > self.last_exploration_time:
            self.current = None

        new_config, data = explore(
            self.data,
            self._hyperparam_bounds,
            self.current,
            trial_to_clone,
            trial,
            trial_to_clone.config,
        )

        # Important to replace the old values, since we are copying across
        self.data = data.copy()

        # If the current guy being selecting is at a point that is already
        # done, then append the data to the "current" which contains the
        # points in the current batch.
        new = [new_config[key] for key in self._hyperparam_bounds]

        new = np.array(new)
        new = new.reshape(1, new.size)
        if self.data["Time"].max() > self.last_exploration_time:
            self.last_exploration_time = self.data["Time"].max()
            self.current = new.copy()
        else:
            self.current = np.concatenate((self.current, new), axis=0)
            logger.debug(self.current)

        return new_config
