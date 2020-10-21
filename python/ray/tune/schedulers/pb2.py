
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
from scipy.stats import norm
import GPy

from ray.tune import trial_runner
from ray.tune import trial_executor
from ray.tune.error import TuneError
from ray.tune.result import TRAINING_ITERATION
from ray.tune.logger import _SafeFallbackEncoder
from ray.tune.sample import Domain, Function
from ray.tune.schedulers import PopulationBasedTraining, TrialScheduler
from ray.tune.schedulers.pbt import make_experiment_tag
from ray.tune.suggest.variant_generator import format_vars
from ray.tune.trial import Trial, Checkpoint
from ray.util.debug import log_once
from ray.tune.schedulers.pb2_utils import *

logger = logging.getLogger(__name__)


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
        # meta data we keep -> episodes and reward (TODO: convert to curve)
        t_r = df[['T', 'R_before']] # df[['T', 'R_before']]
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


class PB2(PopulationBasedTraining):
    """Implements the Population Based Bandit (PB2) algorithm.
    

    PB2 trains a group of models (or agents) in parallel. Periodically, poorly
    performing models clone the state of the top performers, and the hyper-
    parameters are re-selected using GP-bandit optimization. The GP model is
    trained to predict the improvement in the next training period.

    Like PBT, PB2 adapts hyperparameters during training time. This enables 
    very fast hyperparameter discovery and also automatically discovers 
    schedules.

    This Tune PB2 implementation is built on top of the Tune PBT implementation.
    It considers all trials added as part of the PB2 population. If the number 
    of trials exceeds the cluster capacity, they will be time-multiplexed as to 
    balance training progress across the population. To run multiple trials, use 
    `tune.run(num_samples=<int>)`.

    In {LOG_DIR}/{MY_EXPERIMENT_NAME}/, all mutations are logged in
    `pb2_global.txt` and individual policy perturbations are recorded
    in pb2_policy_{i}.txt. Tune logs: [target trial tag, clone trial tag,
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
        >>>     time_attr="timesteps_total",
        >>>     metric="episode_reward_mean",
        >>>     mode="max",
        >>>     perturbation_interval=10000,  # every 10000 `time_attr` units
        >>>                                # (timesteps_total in this case)
        >>>     hyperparam_mutations={
        >>>         ## These must be continuous, currently a limitation.
        >>>         "factor_1": lambda: random.uniform(0.0, 20.0),
        >>>         "factor_2": [1, 10000]
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
                 custom_explore_fn: Optional[Callable] = None,
                 log_config: bool = True,
                 require_attrs: bool = True,
                 synch: bool = False):

        resample_probability = 0

        PopulationBasedTraining.__init__(self, 
                                    time_attr, 
                                    reward_attr, 
                                    metric, 
                                    mode, 
                                    perturbation_interval, 
                                    hyperparam_mutations,
                                    quantile_fraction,
                                    resample_probability,
                                    custom_explore_fn,
                                    log_config,
                                    require_attrs,
                                    synch)

        self._name = "pb2"
        
        self.latest = 0 # when we last explored
        self.data = pd.DataFrame()
        
        self.bounds = {}
        for key, distribution in self._hyperparam_mutations.items():
            self.bounds[key] = [np.min([distribution() for _ in range(999999)]),np.max([distribution() for _ in range(999999)])]

        # Metrics
        self._num_checkpoints = 0
        self._num_perturbations = 0


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
