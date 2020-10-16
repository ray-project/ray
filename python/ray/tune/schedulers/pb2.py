
import copy
from copy import deepcopy
import logging
import json
import math
import os
import random
import shutil
from typing import Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import norm
import GPy
from GPy.kern import Kern
from GPy.core import Param
from sklearn.metrics import pairwise_distances
from sklearn.metrics.pairwise import euclidean_distances

from ray.tune import trial_runner
from ray.tune import trial_executor
from ray.tune.error import TuneError
from ray.tune.result import TRAINING_ITERATION
from ray.tune.logger import _SafeFallbackEncoder
from ray.tune.sample import Domain, Function
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.suggest.variant_generator import format_vars
from ray.tune.trial import Trial, Checkpoint
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


class PBTTrialState:
    """Internal PBT state tracked per-trial."""

    def __init__(self, trial: Trial):
        self.orig_tag = trial.experiment_tag
        self.last_score = None
        self.last_checkpoint = None
        self.last_perturbation_time = 0
        self.last_train_time = 0  # Used for synchronous mode.
        self.last_result = None  # Used for synchronous mode.

    def __repr__(self) -> str:
        return str((self.last_score, self.last_checkpoint,
                    self.last_train_time, self.last_perturbation_time))

## PB2 time varying kernel
class TV_SquaredExp(Kern):
    def __init__(self,input_dim, variance=1.,lengthscale=1.,epsilon=0.,active_dims=None):
        super().__init__(input_dim, active_dims, 'time_se')
        self.variance = Param('variance', variance)
        self.lengthscale = Param('lengthscale', lengthscale)
        self.epsilon = Param('epsilon', epsilon)
        self.link_parameters(self.variance, self.lengthscale, self.epsilon)
        
    def K(self,X,X2):
        # time must be in the far left column
        if self.epsilon > 0.5: # 0.5
            self.epsilon = 0.5
        if X2 is None: X2 = np.copy(X)
        T1 = X[:, 0].reshape(-1, 1)
        T2 = X2[:, 0].reshape(-1, 1)
        dists = pairwise_distances(T1,T2, 'cityblock')
        timekernel=(1-self.epsilon)**(0.5*dists)
        
        X = X[:, 1:]
        X2 = X2[:, 1:]

        RBF = self.variance*np.exp(-np.square(euclidean_distances(X,X2))/self.lengthscale)
        
        return RBF * timekernel
    
    def Kdiag(self,X):
        return self.variance*np.ones(X.shape[0])
    
    def update_gradients_full(self, dL_dK, X, X2):
        if X2 is None: X2 = np.copy(X)
        T1 = X[:, 0].reshape(-1, 1)
        T2 = X2[:, 0].reshape(-1, 1)
        
        X = X[:, 1:]
        X2 = X2[:, 1:]
        dist2 = np.square(euclidean_distances(X,X2))/self.lengthscale
    
        dvar = np.exp(-np.square((euclidean_distances(X,X2))/self.lengthscale))
        dl =  - (2 * euclidean_distances(X,X2)**2 * self.variance * np.exp(-dist2)) * self.lengthscale**(-2)
        n = pairwise_distances(T1,T2, 'cityblock')/2
        deps = -n * (1-self.epsilon)**(n-1)
    
        self.variance.gradient = np.sum(dvar*dL_dK)
        self.lengthscale.gradient = np.sum(dl*dL_dK)
        self.epsilon.gradient = np.sum(deps*dL_dK)


## PB2 data normalizing functions
def normalize(data, wrt):
    # data = data to normalize
    # wrt = data will be normalized with respect to this
    return (data - np.min(wrt, axis=0))/(np.max(wrt,axis=0) - np.min(wrt,axis=0))

def standardize(data):
    data = (data - np.mean(data, axis=0))/(np.std(data, axis=0)+1e-8)
    return np.clip(data, -2, 2)

## UCB acquisition function
def UCB(m, m1, x, fixed, kappa=0.5):
    
    c1 = 0.2
    c2 = 0.4
    beta_t = c1 * np.log(c2 * m.X.shape[0])
    kappa = np.sqrt(beta_t)
    
    xtest = np.concatenate((fixed.reshape(-1, 1), np.array(x).reshape(-1,1))).T
    
    preds = m.predict(xtest)
    mean = preds[0][0][0] 
    
    preds = m1.predict(xtest)
    var = preds[1][0][0]
    return mean + kappa * var

## optimize acquisition function.
def optimize_acq(func, m, m1, fixed, num_f):
    
    print("Optimizing Acquisition Function...\n")
    
    opts = {'maxiter':200, 'maxfun':200, 'disp':False}
    
    T=10
    best_value=-999
    best_theta = m1.X[0,:]
    
    bounds = [(0,1) for _ in range(m.X.shape[1]-num_f)]
    
    for ii in range(T):
        x0 = np.random.uniform(0,1, m.X.shape[1]-num_f)
        
        res = minimize(lambda x: -func(m, m1, x, fixed), x0, bounds=bounds, method="L-BFGS-B", options=opts)
        
        val = func(m, m1, res.x, fixed)
        if val > best_value:
            best_value=val
            best_theta =res.x
    
    return(np.clip(best_theta, 0, 1))

## Select the number of datapoints to keep, using cross validation
def select_length(Xraw, yraw, current, newpoint, bounds, num_f, num):
    
    # use at least 200 rows
    min_len = 200
    
    if Xraw.shape[0] < min_len:
        return(Xraw.shape[0])
    else:
        length = min_len-10   
        scores = []
        while length+10 <= Xraw.shape[0]:
            length += 10
            
            base_vals = np.array(list(bounds.values())).T
            X_len = Xraw[-length:, :]
            y_len = yraw[-length:]
            oldpoints = X_len[:, :num_f]
            old_lims = np.concatenate((np.max(oldpoints, axis=0), np.min(oldpoints, axis=0))).reshape(2, oldpoints.shape[1])
            limits = np.concatenate((old_lims, base_vals),axis=1)
            
            X = normalize(X_len, limits)
            y = standardize(y_len).reshape(y_len.size, 1)
            
            kernel = TV_SquaredExp(input_dim=X.shape[1], variance=1., lengthscale=1., epsilon=0.1)
            m = GPy.models.GPRegression(X, y, kernel)
            m.optimize(messages=True)

            scores.append(m.log_likelihood())
        idx = np.argmax(scores)
        length = (idx+int((min_len/10))) * 10
        return(length)

## select config, given X and y.  
def select_config(Xraw, yraw, current, newpoint, bounds, num_f, num):
    
    length = select_length(Xraw, yraw, current, newpoint, bounds, num_f, num)
    
    Xraw = Xraw[-length:, :]
    yraw = yraw[-length: ]
        
    base_vals = np.array(list(bounds.values())).T
    oldpoints = Xraw[:, :num_f]
    old_lims = np.concatenate((np.max(oldpoints, axis=0), np.min(oldpoints, axis=0))).reshape(2, oldpoints.shape[1])
    limits = np.concatenate((old_lims, base_vals),axis=1)
    
    X = normalize(Xraw, limits)
    y = standardize(yraw).reshape(yraw.size, 1)
    
    fixed = normalize(newpoint, oldpoints)
    
    kernel = TV_SquaredExp(input_dim=X.shape[1], variance=1., lengthscale=1., epsilon=0.1)

    try:
        m = GPy.models.GPRegression(X, y, kernel)
    except np.linalg.LinAlgError:
        # add diagonal ** we would ideally make this something more robust...
        X += np.eye(X.shape[0]) * 1e-3
        m = GPy.models.GPRegression(X, y, kernel)
    
    try:
        m.optimize(messages=True)
    except np.linalg.LinAlgError:
        # add diagonal ** we would ideally make this something more robust...
        X += np.eye(X.shape[0]) * 1e-3
        m = GPy.models.GPRegression(X, y, kernel)
        m.optimize(messages=True)
        
    m.kern.lengthscale.fix(m.kern.lengthscale.clip(1e-5,1))
    
    # m1 is used to reduce redundancy. If there are trials already running (current) we can use this info in the acquisition function
    if current is None:
        m1 = deepcopy(m)
    else:
        # add the current trials to the dataset
        padding = np.array([fixed for _ in range(current.shape[0])])
        current = normalize(current, base_vals)
        current = np.hstack((padding, current))

        Xnew = np.vstack((X, current))
        ypad = np.zeros(current.shape[0])
        ypad = ypad.reshape(-1,1)
        ynew = np.vstack((y, ypad))
        
        #kernel = GPy.kern.RBF(input_dim=X.shape[1], variance=1., lengthscale=1.)
        kernel = TV_SquaredExp(input_dim=X.shape[1], variance=1., lengthscale=1., epsilon=0.1)
        m1 = GPy.models.GPRegression(Xnew, ynew, kernel)
        m1.optimize()
    
    xt = optimize_acq(UCB, m, m1, fixed, num_f)
    
    # convert back...
    xt = xt * (np.max(base_vals,axis=0) - np.min(base_vals,axis=0)) + np.min(base_vals, axis=0)
    
    xt = xt.astype(np.float32)
    return(xt)

## Outer loop: set up data/bounds, pass into select_config function
def explore(data, bounds, current, base, old, config, mutations, resample_probability):
        
    data['Trial'] = data['Trial'].astype(str)
    data.to_csv('old.csv')

    df = data.sort_values(by='T').reset_index(drop=True)

    # group by trial ID and hyperparams. Compute change in timesteps and reward. 
    df['y'] = df.groupby(['Trial'] + [key for key in bounds.keys()])['Reward'].diff()
    df['t_change'] = df.groupby(['Trial'] + [key for key in bounds.keys()])['T'].diff() 

    # delete entries without positive change in t. 
    df = df[df['t_change'] > 0].reset_index(drop=True)
    df['R_before'] = df.Reward - df.y

    # normalize the reward change by the update size. For example if trials took diff lengths of time.
    df['y'] = df.y / df.t_change
    df = df[~df.y.isna()].reset_index(drop=True)
    df = df.sort_values(by='T').reset_index(drop=True)

    # only use the last 1k datapoints, so the GP is not too slow
    df = df.iloc[-1000:, :].reset_index(drop=True) 
    
    # we need this to know the T and Reward for the weights 
    dfnewpoint = df[df['Trial']==str(base)]
    
    if not dfnewpoint.empty:
                
        # now specify the dataset for the GP
        y = np.array(df.y.values)
        t_r = df[['T', 'R_before']] # we use the T and starting reward as features
        h = df[[key for key in bounds.keys()]]
        X = pd.concat([t_r, h], axis=1).values 
        newpoint = df[df['Trial']==str(base)].iloc[-1, :][['T', 'R_before']].values
        new = select_config(X,y, current, newpoint, bounds, num_f = len(t_r.columns), num=len(h.columns))

        new_config = config.copy()
        values = []
        names = []
        for i, col in enumerate(h.columns):
            if type(config[col]) is int:
                new_config[col] = int(new[i])
                values.append(int(new[i]))
            else:
                new_config[col] = new[i]
                values.append(new[i])
                
        new_T = df[df['Trial']==str(base)].iloc[-1, :]['T']
        new_Reward = df[df['Trial']==str(base)].iloc[-1, :].Reward
              
        lst = [[old] + [new_T]  + values + [new_Reward]]
        cols = ['Trial', 'T'] + [key for key in bounds.keys()] + ['Reward']
        new_entry = pd.DataFrame(lst, columns = cols) 

        # create an entry for the new config, with the reward from the copied agent.
        data = pd.concat([data, new_entry]).reset_index(drop=True)

    else:
        new_config = config.copy()

    return new_config, data


def make_experiment_tag(orig_tag, config, mutations):
    """Appends perturbed params to the trial name to show in the console."""

    resolved_vars = {}
    for k in mutations.keys():
        resolved_vars[("config", k)] = config[k]
    return "{}@perturbed[{}]".format(orig_tag, format_vars(resolved_vars))


class PB2(FIFOScheduler):
    """Implements the Population Based Bandit (PB2) algorithm.
    

    PBT trains a group of models (or agents) in parallel. Periodically, poorly
    performing models clone the state of the top performers, and the hyper-
    parameters are re-selected using GP-bandit optimization. The GP model is
    trained to predict the improvement in the next training period.

    Like PBT, PB2 adapts hyperparameters during training time. This enables 
    very fast hyperparameter discovery and also automatically discovers 
    schedules.

    This Tune PBT2 implementation is built on top of the Tune PBT implementation.
    It considers all trials added as part of the PB2 population. If the number 
    of trials exceeds the cluster capacity, they will be time-multiplexed as to 
    balance training progress across the population. To run multiple trials, use 
    `tune.run(num_samples=<int>)`.

    In {LOG_DIR}/{MY_EXPERIMENT_NAME}/, all mutations are logged in
    `pb2_global.txt` and individual policy perturbations are recorded
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
        hyperparam_mutations (dict): Hyperparams to learn. The format is
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

    Example:
        >>> pb2 = PB2(
        >>>     time_attr="training_iteration",
        >>>     metric="episode_reward_mean",
        >>>     mode="max",
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
        >>> tune.run({...}, num_samples=8, scheduler=pb2)
    """

    def __init__(self,
                 time_attr: str = "time_total_s",
                 reward_attr: Optional[str] = None,
                 metric: Optional[str] = None,
                 mode: Optional[str] = None,
                 perturbation_interval: float = 60.0,
                 hyperparam_mutations: Dict = None,
                 quantile_fraction: float = 0.25,
                 resample_probability: float = 0.25,
                 custom_explore_fn: Optional[Callable] = None,
                 log_config: bool = True,
                 require_attrs: bool = True,
                 synch: bool = False):

        hyperparam_mutations = hyperparam_mutations or {}
        for value in hyperparam_mutations.values():
            if not (isinstance(value,
                               (list, dict, Domain)) or callable(value)):
                raise TypeError("`hyperparam_mutation` values must be either "
                                "a List, Dict, a tune search space object, or "
                                "a callable.")
            if isinstance(value, Function):
                raise ValueError("arbitrary tune.sample_from objects are not "
                                 "supported for `hyperparam_mutation` values."
                                 "You must use other built in primitives like"
                                 "tune.uniform, tune.loguniform, etc.")

        if not hyperparam_mutations and not custom_explore_fn:
            raise TuneError(
                "You must specify at least one of `hyperparam_mutations` or "
                "`custom_explore_fn` to use PBT.")

        if quantile_fraction > 0.5 or quantile_fraction < 0:
            raise ValueError(
                "You must set `quantile_fraction` to a value between 0 and"
                "0.5. Current value: '{}'".format(quantile_fraction))

        if perturbation_interval <= 0:
            raise ValueError(
                "perturbation_interval must be a positive number greater "
                "than 0. Current value: '{}'".format(perturbation_interval))

        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."

        if reward_attr is not None:
            mode = "max"
            metric = reward_attr
            logger.warning(
                "`reward_attr` is deprecated and will be removed in a future "
                "version of Tune. "
                "Setting `metric={}` and `mode=max`.".format(reward_attr))

        FIFOScheduler.__init__(self)
        self._metric = metric
        self._mode = mode
        self._metric_op = None
        if self._mode == "max":
            self._metric_op = 1.
        elif self._mode == "min":
            self._metric_op = -1.
        self._time_attr = time_attr
        self._perturbation_interval = perturbation_interval
        self._hyperparam_mutations = hyperparam_mutations
        self._quantile_fraction = quantile_fraction
        self._resample_probability = resample_probability
        self._trial_state = {}
        self._custom_explore_fn = custom_explore_fn
        self._log_config = log_config
        self._require_attrs = require_attrs
        self._synch = synch
        self._next_perturbation_sync = self._perturbation_interval
        
        self.latest = 0 # when we last explored
        self.data = pd.DataFrame()
        
        self.bounds = {}
        for key, distribution in self._hyperparam_mutations.items():
            self.bounds[key] = [np.min([distribution() for _ in range(999999)]),np.max([distribution() for _ in range(999999)])]

        # Metrics
        self._num_checkpoints = 0
        self._num_perturbations = 0

    def on_trial_add(self, trial_runner, trial):
        self._trial_state[trial] = PBTTrialState(trial)

    # same as PBT, but stores the data in one big dataframe.
    def on_trial_result(self, trial_runner: "trial_runner.TrialRunner",
                        trial: Trial, result: Dict) -> str:
        if self._time_attr not in result:
            time_missing_msg = "Cannot find time_attr {} " \
                               "in trial result {}. Make sure that this " \
                               "attribute is returned in the " \
                               "results of your Trainable.".format(
                                self._time_attr, result)
            if self._require_attrs:
                raise RuntimeError(
                    time_missing_msg +
                    "If this error is expected, you can change this to "
                    "a warning message by "
                    "setting PB2(require_attrs=False)")
            else:
                if log_once("pb2-time_attr-error"):
                    logger.warning(time_missing_msg)
        if self._metric not in result:
            metric_missing_msg = "Cannot find metric {} in trial result {}. " \
                                 "Make sure that this attribute is returned " \
                                 "in the " \
                                 "results of your Trainable.".format(
                                    self._metric, result)
            if self._require_attrs:
                raise RuntimeError(
                    metric_missing_msg + "If this error is expected, "
                    "you can change this to a warning message by "
                    "setting PBT(require_attrs=False)")
            else:
                if log_once("pbt-metric-error"):
                    logger.warning(metric_missing_msg)

        if self._metric not in result or self._time_attr not in result:
            return TrialScheduler.CONTINUE

        time = result[self._time_attr]
        state = self._trial_state[trial]

        # Continue training if perturbation interval has not been reached yet.
        if time - state.last_perturbation_time < self._perturbation_interval:
            return TrialScheduler.CONTINUE  # avoid checkpoint overhead

        # This trial has reached its perturbation interval
        score = self._metric_op * result[self._metric]
        state.last_score = score
        state.last_train_time = time
        state.last_result = result

        ## Data logging for PB2
        names = []
        values = []
        for key, distribution in self._hyperparam_mutations.items():
            names.append(str(key))
            values.append(trial.config[key])
        
        # this needs to be made more general
        lst = [[trial, result[self._time_attr]] + values + [score]]
        cols = ['Trial', 'T'] + names + ['Reward']
        entry = pd.DataFrame(lst, columns = cols) 

        self.data = pd.concat([self.data, entry]).reset_index(drop=True)
        self.data.Trial = self.data.Trial.astype('str')
        ####

        if not self._synch:
            state.last_perturbation_time = time
            lower_quantile, upper_quantile = self._quantiles()
            self._perturb_trial(trial, trial_runner, upper_quantile,
                                lower_quantile)
            for trial in trial_runner.get_trials():
                if trial.status in [Trial.PENDING, Trial.PAUSED]:
                    return TrialScheduler.PAUSE  # yield time to other trials

            return TrialScheduler.CONTINUE
        else:
            # Synchronous mode.
            if any(self._trial_state[t].last_train_time <
                   self._next_perturbation_sync and t != trial
                   for t in trial_runner.get_trials()):
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
                    self._perturb_trial(t, trial_runner, upper_quantile,
                                        lower_quantile)

                all_train_times = [
                    self._trial_state[trial].last_train_time
                    for trial in trial_runner.get_trials()
                ]
                max_last_train_time = max(all_train_times)
                self._next_perturbation_sync = max(
                    self._next_perturbation_sync + self._perturbation_interval,
                    max_last_train_time)
            # In sync mode we should pause all trials once result comes in.
            # Once a perturbation step happens for all trials, they should
            # still all be paused.
            # choose_trial_to_run will then pick the next trial to run out of
            # the paused trials.
            return TrialScheduler.PAUSE

    def _perturb_trial(
            self, trial: Trial, trial_runner: "trial_runner.TrialRunner",
            upper_quantile: List[Trial], lower_quantile: List[Trial]):
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
                state.last_checkpoint = trial_runner.trial_executor.save(
                    trial, Checkpoint.MEMORY, result=state.last_result)
            self._num_checkpoints += 1
        else:
            state.last_checkpoint = None  # not a top trial

        if trial in lower_quantile:
            logger.debug("Trial {} is in lower quantile".format(trial))
            trial_to_clone = random.choice(upper_quantile)
            assert trial is not trial_to_clone
            self._exploit(trial_runner.trial_executor, trial, trial_to_clone)

    # same as PBT, but changed filenames.
    def _log_config_on_step(self, trial_state: PBTTrialState,
                            new_state: PBTTrialState, trial: Trial,
                            trial_to_clone: Trial, new_config: Dict):
        """Logs transition during exploit/exploit step.

        For each step, logs: [target trial tag, clone trial tag, target trial
        iteration, clone trial iteration, old config, new config].
        """
        trial_name, trial_to_clone_name = (trial_state.orig_tag,
                                           new_state.orig_tag)
        trial_id = trial.trial_id
        trial_to_clone_id = trial_to_clone.trial_id
        trial_path = os.path.join(trial.local_dir,
                                  "pb2_policy_" + trial_id + ".txt")
        trial_to_clone_path = os.path.join(
            trial_to_clone.local_dir,
            "pb2_policy_" + trial_to_clone_id + ".txt")
        policy = [
            trial_name, trial_to_clone_name,
            trial.last_result.get(TRAINING_ITERATION, 0),
            trial_to_clone.last_result.get(TRAINING_ITERATION, 0),
            trial_to_clone.config, new_config
        ]
        # Log to global file.
        with open(os.path.join(trial.local_dir, "pb2_global.txt"), "a+") as f:
            print(json.dumps(policy, cls=_SafeFallbackEncoder), file=f)
        # Overwrite state in target trial from trial_to_clone.
        if os.path.exists(trial_to_clone_path):
            shutil.copyfile(trial_to_clone_path, trial_path)
        # Log new exploit in target trial log.
        with open(trial_path, "a+") as f:
            f.write(json.dumps(policy, cls=_SafeFallbackEncoder) + "\n")


    def _exploit(self, trial_executor: "trial_executor.TrialExecutor",
                 trial: Trial, trial_to_clone: Trial):
        """Transfers perturbed state from trial_to_clone -> trial.

        If specified, also logs the updated hyperparam state.
        """
        trial_state = self._trial_state[trial]
        new_state = self._trial_state[trial_to_clone]
        if not new_state.last_checkpoint:
            logger.info("[pbt]: no checkpoint for trial."
                        " Skip exploit for Trial {}".format(trial))
            return

                # if we are at a new timestep, we dont want to penalise for trials still going
        if self.data['T'].max() > self.latest:
            self.current = None
        
        print("\n Replacing the weights of: \n{} \n with:{} \n".format(str(trial), str(trial_to_clone)))
        new_config, data = explore(self.data, self.bounds,
                             self.current,
                             trial_to_clone,
                             trial,
                             trial_to_clone.config,
                             self._hyperparam_mutations,
                             self._resample_probability)
        
        # important to replace the old values, since we are copying across
        self.data = data.copy()
        
        # if the current guy youre selecting is at a point youve already done, 
        # then append the data to the "current" which is the points in the current batch
        
        new = []
        for key in self._hyperparam_mutations.keys():
            new.append(new_config[key])
    
        new  = np.array(new)
        new = new.reshape(1, new.size)
        if self.data['T'].max() > self.latest:
            self.latest = self.data['T'].max()
            self.current = new.copy()
        else:
            self.current = np.concatenate((self.current, new), axis=0)
            print(self.current)
        
        logger.info("[exploit] transferring weights from trial "
                    "{} (score {}) -> {} (score {})".format(
                        trial_to_clone, new_state.last_score, trial,
                        trial_state.last_score))
        # Only log mutated hyperparameters and not entire config.
        old_hparams = {
            k: v
            for k, v in trial_to_clone.config.items()
            if k in self._hyperparam_mutations
        }
        new_hparams = {
            k: v
            for k, v in new_config.items() if k in self._hyperparam_mutations
        }
        logger.info("[explore] perturbed config from {} -> {}".format(
            old_hparams, new_hparams))

        if self._log_config:
            self._log_config_on_step(trial_state, new_state, trial,
                                     trial_to_clone, new_config)

        new_tag = make_experiment_tag(trial_state.orig_tag, new_config,
                                      self._hyperparam_mutations)
        if trial.status == Trial.PAUSED:
            # If trial is paused we update it with a new checkpoint.
            # When the trial is started again, the new checkpoint is used.
            if not self._synch:
                raise TuneError("Trials should be paused here only if in "
                                "synchronous mode. If you encounter this error"
                                " please raise an issue on Ray Github.")
            trial.config = new_config
            trial.experiment_tag = new_tag
            trial.on_checkpoint(new_state.last_checkpoint)
        else:
            # If trial is running, we first try to reset it.
            # If that is unsuccessful, then we have to stop it and start it
            # again with a new checkpoint.
            reset_successful = trial_executor.reset_trial(
                trial, new_config, new_tag)
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
                math.ceil(len(trials) * self._quantile_fraction))
            if num_trials_in_quantile > len(trials) / 2:
                num_trials_in_quantile = int(math.floor(len(trials) / 2))
            return (trials[:num_trials_in_quantile],
                    trials[-num_trials_in_quantile:])

    def choose_trial_to_run(
            self, trial_runner: "trial_runner.TrialRunner") -> Optional[Trial]:
        """Ensures all trials get fair share of time (as defined by time_attr).

        This enables the PBT scheduler to support a greater number of
        concurrent trials than can fit in the cluster at any given time.
        """
        candidates = []
        for trial in trial_runner.get_trials():
            if trial.status in [Trial.PENDING, Trial.PAUSED] and \
                    trial_runner.has_resources(trial.resources):
                if not self._synch:
                    candidates.append(trial)
                elif self._trial_state[trial].last_train_time < \
                        self._next_perturbation_sync:
                    candidates.append(trial)
        candidates.sort(
            key=lambda trial: self._trial_state[trial].last_train_time)
        return candidates[0] if candidates else None

    def reset_stats(self):
        self._num_perturbations = 0
        self._num_checkpoints = 0

    def last_scores(self, trials: List[Trial]) -> List[float]:
        scores = []
        for trial in trials:
            state = self._trial_state[trial]
            if state.last_score is not None and not trial.is_finished():
                scores.append(state.last_score)
        return scores

    def debug_string(self) -> str:
        return "PopulationBasedTraining: {} checkpoints, {} perturbs".format(
            self._num_checkpoints, self._num_perturbations)
