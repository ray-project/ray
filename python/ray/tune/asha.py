from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import numpy as np

from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial

class ASHAScheduler(FIFOScheduler):
    """Implements the Async Successive Halving.

    Args:
        time_attr (str): The TrainingResult attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        reward_attr (str): The TrainingResult objective value attribute. As
            with `time_attr`, this may refer to any objective value. Stopping
            procedures will use this attribute.
        max_t (int): max time units per trial. Trials will be stopped after
            max_t time units (determined by time_attr) have passed.
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

        self._trial_info = {}  # Stores Trial -> Bracket

        # Tracks state for new trial add
        self._state = [_Bracket(
            grace_period, max_t, reduction_factor, s) for s in range(5)]
        self._num_stopped = 0
        self._reward_attr = reward_attr
        self._time_attr = time_attr

    def on_trial_add(self, trial_runner, trial):
        self._trial_info[trial] = np.random.choice(self._state)

    def on_trial_result(self, trial_runner, trial, result):
        if getattr(result, self._time_attr) >= max_t:
            self._num_stopped += 1
            return TrialScheduler.STOP

        bracket = self._trial_info[trial]
        action = bracket.on_result(
            getattr(result, self._time_attr),
            getattr(result, self._reward_attr))

        return action

    def debug_string(self):
        out = "Using ASHA: num_stopped={}".format(self._num_stopped)
        return out


class _Bracket():
    def __init__(self, min_t, max_t, reduction_factor, s):
        MAX_RUNGS = int(np.log(max_t / min_t) / np.log(reduction_factor) - s + 1)
        self._rungs = [(min_t * reduction_factor**(k + s), {})
                       for k in reversed(range(MAX_RUNGS))]

    def on_result(self, trial, cur_iter, cur_rew):
        action = TrialScheduler.CONTINUE
        for milestone, recorded in self._rungs:
            if cur_iter < milestone:
                continue
            elif trial.trial_id in recorded:
                continue
            else:
                recorded[trial.trial_id] = cur_rew
                if cur_rew < np.percentile(
                    recorded.values(), 1 / reduction_factor):
                    action = Trial.STOP
                break

        return action
