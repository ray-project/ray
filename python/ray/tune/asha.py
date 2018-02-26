from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler


class ASHAScheduler(FIFOScheduler):
    """Implements the Async Successive Halving.

    This should provide similar theoretical performance as HyperBand but
    avoid straggler issues that HyperBand faces.

    See https://openreview.net/forum?id=S1Y7OOlRZ

    Args:
        time_attr (str): The TrainingResult attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        reward_attr (str): The TrainingResult objective value attribute. As
            with `time_attr`, this may refer to any objective value. Stopping
            procedures will use this attribute.
        max_t (float): max time units per trial. Trials will be stopped after
            max_t time units (determined by time_attr) have passed.
        grace_period (float): Only stop trials at least this old in time.
            The units are the same as the attribute named by `time_attr`.
        reduction_factor (float): Used to set halving rate and amount. This
            is simply a unit-less scalar.
    """

    def __init__(
            self, time_attr='training_iteration',
            reward_attr='episode_reward_mean', max_t=100,
            grace_period=10, reduction_factor=3):
        assert max_t > 0, "Max (time_attr) not valid!"
        assert max_t > grace_period, "grace_period must be less than max_t!"
        assert grace_period > 0, "Max (time_attr) not valid!"
        assert reduction_factor > 1, "Reduction Factor not valid!"
        FIFOScheduler.__init__(self)
        self._reduction_factor = reduction_factor
        self._max_t = max_t

        self._trial_info = {}  # Stores Trial -> Bracket

        # Tracks state for new trial add
        self._brackets = [_Bracket(
            grace_period, max_t, reduction_factor, s) for s in range(5)]
        self._num_stopped = 0
        self._reward_attr = reward_attr
        self._time_attr = time_attr

    def on_trial_add(self, trial_runner, trial):
        self._trial_info[trial] = np.random.choice(self._brackets)

    def on_trial_result(self, trial_runner, trial, result):
        if getattr(result, self._time_attr) >= self._max_t:
            self._num_stopped += 1
            return TrialScheduler.STOP

        bracket = self._trial_info[trial]
        action = bracket.on_result(
            trial,
            getattr(result, self._time_attr),
            getattr(result, self._reward_attr))

        return action

    def debug_string(self):
        out = "Using ASHA: num_stopped={}".format(self._num_stopped)
        out += "\n" + "\n".join([b.debug_str() for b in self._brackets])
        return out


class _Bracket():
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
            if cur_iter < milestone:
                continue
            elif trial.trial_id in recorded:
                continue
            else:
                recorded[trial.trial_id] = cur_rew
                if cur_rew < self.cutoff(recorded):
                    action = TrialScheduler.STOP
                break

        return action

    def debug_str(self):
        iters = " ".join(
            ["Iter {:.3f}: {}".format(milestone, self.cutoff(recorded))
             for milestone, recorded in self._rungs])
        return "Bracket: " + iters


if __name__ == '__main__':
    sched = ASHAScheduler(grace_period=1.2, max_t=2123.3, reduction_factor=3)
    bracket = sched._brackets[0]
    print(bracket.cutoff({str(i): i for i in range(20)}))
