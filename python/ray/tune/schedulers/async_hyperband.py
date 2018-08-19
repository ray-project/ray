from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler


class AsyncHyperBandScheduler(FIFOScheduler):
    """Implements the Async Successive Halving.

    This should provide similar theoretical performance as HyperBand but
    avoid straggler issues that HyperBand faces. One implementation detail
    is when using multiple brackets, trial allocation to bracket is done
    randomly with over a softmax probability.

    See https://openreview.net/forum?id=S1Y7OOlRZ

    Args:
        time_attr (str): A training result attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        reward_attr (str): The training result objective value attribute. As
            with `time_attr`, this may refer to any objective value. Stopping
            procedures will use this attribute.
        max_t (float): max time units per trial. Trials will be stopped after
            max_t time units (determined by time_attr) have passed.
        grace_period (float): Only stop trials at least this old in time.
            The units are the same as the attribute named by `time_attr`.
        reduction_factor (float): Used to set halving rate and amount. This
            is simply a unit-less scalar.
        brackets (int): Number of brackets. Each bracket has a different
            halving rate, specified by the reduction factor.
    """

    def __init__(self,
                 time_attr='training_iteration',
                 reward_attr='episode_reward_mean',
                 max_t=100,
                 grace_period=10,
                 reduction_factor=3,
                 brackets=3):
        assert max_t > 0, "Max (time_attr) not valid!"
        assert max_t >= grace_period, "grace_period must be <= max_t!"
        assert grace_period > 0, "grace_period must be positive!"
        assert reduction_factor > 1, "Reduction Factor not valid!"
        assert brackets > 0, "brackets must be positive!"
        FIFOScheduler.__init__(self)
        self._reduction_factor = reduction_factor
        self._max_t = max_t

        self._trial_info = {}  # Stores Trial -> Bracket

        # Tracks state for new trial add
        self._brackets = [
            _Bracket(grace_period, max_t, reduction_factor, s)
            for s in range(brackets)
        ]
        self._counter = 0  # for
        self._num_stopped = 0
        self._reward_attr = reward_attr
        self._time_attr = time_attr

    def on_trial_add(self, trial_runner, trial):
        sizes = np.array([len(b._rungs) for b in self._brackets])
        probs = np.e**(sizes - sizes.max())
        normalized = probs / probs.sum()
        idx = np.random.choice(len(self._brackets), p=normalized)
        self._trial_info[trial.trial_id] = self._brackets[idx]

    def on_trial_result(self, trial_runner, trial, result):
        action = TrialScheduler.CONTINUE
        if result[self._time_attr] >= self._max_t:
            action = TrialScheduler.STOP
        else:
            bracket = self._trial_info[trial.trial_id]
            action = bracket.on_result(trial, result[self._time_attr],
                                       result[self._reward_attr])
        if action == TrialScheduler.STOP:
            self._num_stopped += 1
        return action

    def on_trial_complete(self, trial_runner, trial, result):
        bracket = self._trial_info[trial.trial_id]
        bracket.on_result(trial, result[self._time_attr],
                          result[self._reward_attr])
        del self._trial_info[trial.trial_id]

    def on_trial_remove(self, trial_runner, trial):
        del self._trial_info[trial.trial_id]

    def debug_string(self):
        out = "Using AsyncHyperBand: num_stopped={}".format(self._num_stopped)
        out += "\n" + "\n".join([b.debug_str() for b in self._brackets])
        return out


class _Bracket():
    """Bookkeeping system to track the cutoffs.

    Rungs are created in reversed order so that we can more easily find
    the correct rung corresponding to the current iteration of the result.

    Example:
        >>> b = _Bracket(1, 10, 2, 3)
        >>> b.on_result(trial1, 1, 2)  # CONTINUE
        >>> b.on_result(trial2, 1, 4)  # CONTINUE
        >>> b.cutoff(b._rungs[-1][1]) == 3.0  # rungs are reversed
        >>> b.on_result(trial3, 1, 1)  # STOP
        >>> b.cutoff(b._rungs[0][1]) == 2.0
    """

    def __init__(self, min_t, max_t, reduction_factor, s):
        self.rf = reduction_factor
        MAX_RUNGS = int(np.log(max_t / min_t) / np.log(self.rf) - s + 1)
        self._rungs = [(min_t * self.rf**(k + s), {})
                       for k in reversed(range(MAX_RUNGS))]

    def cutoff(self, recorded):
        if not recorded:
            return None
        return np.percentile(list(recorded.values()), (1 - 1 / self.rf) * 100)

    def on_result(self, trial, cur_iter, cur_rew):
        action = TrialScheduler.CONTINUE
        for milestone, recorded in self._rungs:
            if cur_iter < milestone or trial.trial_id in recorded:
                continue
            else:
                cutoff = self.cutoff(recorded)
                if cutoff is not None and cur_rew < cutoff:
                    action = TrialScheduler.STOP
                if cur_rew is None:
                    print("Reward attribute is None! Consider"
                          " reporting using a different field.")
                else:
                    recorded[trial.trial_id] = cur_rew
                break
        return action

    def debug_str(self):
        iters = " | ".join([
            "Iter {:.3f}: {}".format(milestone, self.cutoff(recorded))
            for milestone, recorded in self._rungs
        ])
        return "Bracket: " + iters


if __name__ == '__main__':
    sched = AsyncHyperBandScheduler(
        grace_period=1, max_t=10, reduction_factor=2)
    print(sched.debug_string())
    bracket = sched._brackets[0]
    print(bracket.cutoff({str(i): i for i in range(20)}))
