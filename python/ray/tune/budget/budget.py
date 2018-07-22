from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import numpy as np

from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial

from bayesian_util import BeliefModel
from acqn_util import longreg

np.warnings.filterwarnings('ignore')
# todo!
global_step = 5
global_bounds = [[0.1, 5],
              [1, 10],
              [80, 120], #[40, 120],
              [np.log(0.0001), np.log(0.8)],
              [1, 20]]

class BudgetedScheduler(FIFOScheduler):
    """Implements the Budget Constrained tuning / early stopping algorithm.

    BudgetedScheduler suspends and resumes trials based on a budgeted
    hyperparameter optimization algorithm. It uses a Bayesian belief model
    to predict the future performance of a configuration, and selects the
    next trial based on the future prediction and the remaining budget.

    To use this implementation of BudgetedScheduler, please specify a total
    budget `t_budget`, the total number of resources (for example timesteps, corehours)
    that all configurations share, the time units `time_attr`, and the name of
    the reported objective value `reward_attr`.

    For example, to limit trials to a total budget of 300 iterations and
    suspend and resume based on the `mean_accuracy` attr, construct:

    ``BudgetedScheduler('timesteps_this_iter', 'mean_accuracy', 300)``


    Args:
        time_attr (str): The TrainingResult attr to use for resource consumption.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        reward_attr (str): The TrainingResult objective value attribute. As
            with `time_attr`, this may refer to any objective value. Suspension
            and resume procedures will be based on this attribute.
        t_budget (int): total time units shared from all configurations per trial. 
            Trials will run for exactly t_budget time units (determined by time_attr) have passed.
            The scheduler will terminate trials after this time has passed.
            Note that this is different from the semantics of `max_t` as
            mentioned in the original HyperBand paper.
    """

    #TODO Other parameters for the Bayesian model
    #TODO result attributes are hard coded

    def __init__(self,
                 time_attr='training_iteration',
                 reward_attr='mean_accuracy',
                 t_budget=200):
        assert t_budget > 0, "t_budget (time_attr) not valid!"
        FIFOScheduler.__init__(self)

        self._t_budget = t_budget
        self._reward_attr = reward_attr
        self._time_attr = time_attr
        # setup belief model, currently use Freeze-Thaw
        self._setup_belief()

        self._cur_pool = []  # stores list of trials
        self._all_trials = dict() # Stores config -> episode record
        self._state_iter = dict() # dict(config, iter)
        self._state_acc = dict() # dict(config, acc))
        self._t_remain = t_budget
        self._idx_pool = []
        self._pool_size = 0

    # TODO hard code the GP hyperparams, and sampling bound
    def _setup_belief(self, alpha=2.0, beta=5, scale=100,
                      log_noise=np.log(0.001), x_scale=10,
                      tscale=0.005,   # for data resnet
                      bounds=global_bounds):
        # Set up freeze thaw parameters
        self.hyp0 = [alpha, beta, scale, log_noise, x_scale]

        print('&&&& SCHEDULER: SETUP BAYES: bound=%s.' %
              (" ".join(["%.2f/(%.2f, %.2f)" % (self.hyp0[s], bounds[s][0], bounds[s][1]) for s in range(len(self.hyp0))])))
        self.belief = BeliefModel(x_kernel_params=dict(scale=x_scale),
                                  t_kernel_params=dict(alpha=alpha, beta=beta, scale=scale, 
                                                       log_noise=log_noise, tscale=tscale))
        # sampler for GP hyperparameter
        self.sampler = dict(bounds=bounds)

    def _update_trial_stats(self, trial, result):
        """Update result for trial. Called after trial has finished
        an iteration - will decrement remaining budget count."""

        assert trial in self._cur_pool
        assert self._get_result_time(result) >= 0

        key = result.config['index']
        # assert(key == trial.config['index'])
        old_iter = self._state_iter[key]
        new_iter = self._get_result_time(result)

        delta = new_iter - old_iter
        assert delta >= 0
        # update remaining budget
        self._t_remain -= delta
        print('\n### SCHEDULER ###: remain budget', self._t_remain, '; last iter', delta, 'spent on index', key)
        # update all trial and state
        self._state_iter[key] = new_iter
        self._state_acc[key] = result.mean_accuracy
        self._all_trials[key] = 1-result.episodes_total
        #TODO delete trials when overfitting!

    def _update_belief(self):
        # if maxL > 0:
        #     predtlist = np.minimum(predtlist_, maxL).tolist()
        # else:
        # predtlist = predtlist_
        #TODO GP_config can be something other than _idx_pool
        GP_config = self._idx_pool
        GP_epoch = [None] * self._pool_size
        yobs = [None] * self._pool_size
        predt = [None] * self._pool_size
        for idx, arm in enumerate(self._idx_pool):
            GP_epoch[idx] = list(range(self._state_iter[arm]*global_step))
            predt[idx] = (self._t_remain + 1 + self._state_iter[arm])*global_step
            yobs[idx] = self._all_trials[arm]
        # belief model for future predictions
        t_star, mu, var, self.hyp0, fail = self.belief.predict_xiid(GP_config, GP_epoch, yobs, predt,
                                                               self.hyp0, self.sampler, debug=False)
        Tpred, ymu, yv = self.belief.convergence(t_star, mu, var)
        return Tpred, ymu, yv

    # def _check_overfitting(self, ):
    def _get_result_time(self, result):
        if result is None:
            print('get result time is None???')
            return 0
        return getattr(result, self._time_attr)

    def on_trial_add(self, trial_runner, trial):
        """Adds new trial.
        """
        self._cur_pool.append(trial)
        key = trial.config['index']
        self._idx_pool.append(key)
        self._pool_size += 1
        self._state_iter[key] = 0
        self._state_acc[key] = 0
        self._all_trials[key] = np.array([])

    def on_trial_result(self, trial_runner, trial, result):
        self._update_trial_stats(trial, result)
        # TODO delete overfitting trial
        if self._t_remain <= 0:
            action = TrialScheduler.STOP
        else:
            action = TrialScheduler.PAUSE
        return action

    def on_trial_remove(self, trial_runner, trial):
        """Notification when trial terminates.
        """
        # trial_runner.stop_trial(trial)
        key = trial.config['index']
        del self._state_iter[key], self._state_acc[key], self._all_trials[key]
        self._cur_pool.remove(trial)
        self._idx_pool.remove(key)
        self._pool_size -= 1

    def on_trial_complete(self, trial_runner, trial, result):
        """Cleans up trial info from bracket if trial completed early."""
        self.on_trial_remove(trial_runner, trial)

    def on_trial_error(self, trial_runner, trial):
        """Cleans up trial info from bracket if trial errored early."""
        self.on_trial_remove(trial_runner, trial)

    def choose_trial_to_run(self, trial_runner):
        """budgeted tuning
        """
        # print(trial_runner.debug_string())
        Tpred, ymu, yv = self._update_belief()
        # select next action
        assert len(Tpred) == self._pool_size
        bestidx = np.argmin(ymu)  # from 0 to M
        bestpredarm = self._idx_pool[bestidx]

        if Tpred[bestidx] < (self._t_remain+self._state_iter[bestpredarm])*global_step:
            score = longreg(ymu, yv, 'th1')
            # nextidx = np.argmax(score)
            nextidx = np.random.choice(np.where(score == score.max())[0])
            stage = 'explore'
        else:  # commit
            nextidx = bestidx
            stage = 'commit'

        trial = self._cur_pool[nextidx]
        print('\n### SCHEDULER ###: PLANNING remain T', self._t_remain, '### ')
        print('### SCHEDULER ###: PLANNING NEXT index', self._idx_pool[nextidx], stage, 'predt', Tpred[bestidx], 
              'with predicted perf', ymu[bestidx], '###\n')
        if self._t_remain > 0 and (trial.status in [Trial.PAUSED, Trial.PENDING]
                and trial_runner.has_resources(trial.resources)):
            return trial
        # terminate when budget exhausts
        elif self._t_remain <= 0:
          for t in self._cur_pool:
            trial_runner.stop_trial(t)
        return None

    def debug_string(self):
        """This provides a progress notification for the algorithm.

        For each bracket, the algorithm will output a string as follows:

            Bracket(Max Size (n)=5, Milestone (r)=33, completed=14.6%):
            {PENDING: 2, RUNNING: 3, TERMINATED: 2}

        "Max Size" indicates the max number of pending/running experiments
        set according to the Hyperband algorithm.

        "Milestone" indicates the iterations a trial will run for before
        the next halving will occur.

        "Completed" indicates an approximate progress metric. Some brackets,
        like ones that are unfilled, will not reach 100%.
        """
        out = "Using Budgeted Tuning: "
        out += "num_trials={}".format(len(self._cur_pool))
        # for i, band in enumerate(self._all_trials):
        #     out += "\nRound #{}:".format(i)
        #     for bracket in band:
        #         out += "\n  {}".format(bracket)
        return out