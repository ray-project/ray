from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial


# Implementation notes:
#    This implementation contains 3 logical levels.
#    Each HyperBand iteration is a "band". There can be multiple
#    bands running at once, and there can be 1 band that is incomplete.
#
#    In each band, there are at most `s` + 1 brackets.
#    `s` is a value determined by given parameters, and assigned on
#    a cyclic basis.
#
#    In each bracket, there are at most `n(s)` trials, indicating that
#    `n` is a function of `s`. These trials go through a series of
#    halving procedures, dropping lowest performers. Multiple
#    brackets are running at once.
#
#    Trials added will be inserted into the most recent bracket
#    and band and will spill over to new brackets/bands accordingly.
#
#    This maintains the bracket size and max trial count per band
#    to 5 and 117 respectively, which correspond to that of
#    `max_attr=81, eta=3` from the blog post. Trials will fill up
#    from smallest bracket to largest, with largest
#    having the most rounds of successive halving.
class HyperBandScheduler(FIFOScheduler):
    """Implements the HyperBand early stopping algorithm.

    HyperBandScheduler early stops trials using the HyperBand optimization
    algorithm. It divides trials into brackets of varying sizes, and
    periodically early stops low-performing trials within each bracket.

    To use this implementation of HyperBand with Ray.tune, all you need
    to do is specify the max length of time a trial can run `max_t`, the time
    units `time_attr`, and the name of the reported objective value
    `reward_attr`. We automatically determine reasonable values for the other
    HyperBand parameters based on the given values.

    For example, to limit trials to 10 minutes and early stop based on the
    `episode_mean_reward` attr, construct:

    ``HyperBand('time_total_s', 'episode_reward_mean', 600)``

    See also: https://people.eecs.berkeley.edu/~kjamieson/hyperband.html

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
            The HyperBand scheduler automatically tries to determine a
            reasonable number of brackets based on this.
    """

    def __init__(
            self, time_attr='training_iteration',
            reward_attr='episode_reward_mean', max_t=81):
        assert max_t > 0, "Max (time_attr) not valid!"
        FIFOScheduler.__init__(self)
        self._eta = 3
        self._s_max_1 = 5
        # bracket max trials
        self._get_n0 = lambda s: int(
            np.ceil(self._s_max_1/(s+1) * self._eta**s))
        # bracket initial iterations
        self._get_r0 = lambda s: int((max_t*self._eta**(-s)))
        self._hyperbands = [[]]  # list of hyperband iterations
        self._trial_info = {}  # Stores Trial -> Bracket, Band Iteration

        # Tracks state for new trial add
        self._state = {"bracket": None,
                       "band_idx": 0}
        self._num_stopped = 0
        self._reward_attr = reward_attr
        self._time_attr = time_attr

    def on_trial_add(self, trial_runner, trial):
        """On a new trial add, if current bracket is not filled,
        add to current bracket. Else, if current band is not filled,
        create new bracket, add to current bracket.
        Else, create new iteration, create new bracket, add to bracket."""

        cur_bracket = self._state["bracket"]
        cur_band = self._hyperbands[self._state["band_idx"]]
        if cur_bracket is None or cur_bracket.filled():
            retry = True
            while retry:
                # if current iteration is filled, create new iteration
                if self._cur_band_filled():
                    cur_band = []
                    self._hyperbands.append(cur_band)
                    self._state["band_idx"] += 1

                # cur_band will always be less than s_max_1 or else filled
                s = len(cur_band)
                assert s < self._s_max_1, "Current band is filled!"
                if self._get_r0(s) == 0:
                    print("Bracket too small - Retrying...")
                    cur_bracket = None
                else:
                    retry = False
                    cur_bracket = Bracket(
                        self._time_attr, self._get_n0(s), self._get_r0(s),
                        self._eta, s)
                cur_band.append(cur_bracket)
                self._state["bracket"] = cur_bracket

        self._state["bracket"].add_trial(trial)
        self._trial_info[trial] = cur_bracket, self._state["band_idx"]

    def _cur_band_filled(self):
        """Checks if the current band is filled.

        The size of the current band should be equal to s_max_1"""

        cur_band = self._hyperbands[self._state["band_idx"]]
        return len(cur_band) == self._s_max_1

    def on_trial_result(self, trial_runner, trial, result):
        """If bracket is finished, all trials will be stopped.

        If a given trial finishes and bracket iteration is not done,
        the trial will be paused and resources will be given up.

        This scheduler will not start trials but will stop trials.
        The current running trial will not be handled,
        as the trialrunner will be given control to handle it.

        # TODO(rliaw) should be only called if trial has not errored"""
        bracket, _ = self._trial_info[trial]
        bracket.update_trial_stats(trial, result)

        if bracket.continue_trial(trial):
            return TrialScheduler.CONTINUE

        action = self._process_bracket(trial_runner, bracket, trial)
        return action

    def _process_bracket(self, trial_runner, bracket, trial):
        """This is called whenever a trial makes progress.

        When all live trials in the bracket have no more iterations left,
        Trials will be successively halved. If bracket is done, all
        non-running trials will be stopped and cleaned up,
        and during each halving phase, bad trials will be stopped while good
        trials will return to "PENDING"."""

        action = TrialScheduler.PAUSE
        if bracket.cur_iter_done():
            if bracket.finished():
                self._cleanup_bracket(trial_runner, bracket)
                return TrialScheduler.CONTINUE

            good, bad = bracket.successive_halving(self._reward_attr)
            # kill bad trials
            for t in bad:
                if t.status == Trial.PAUSED:
                    self._cleanup_trial(trial_runner, t, bracket, hard=True)
                elif t.status == Trial.RUNNING:
                    self._cleanup_trial(trial_runner, t, bracket, hard=False)
                    action = TrialScheduler.STOP
                else:
                    raise Exception("Trial with unexpected status encountered")

            # ready the good trials - if trial is too far ahead, don't continue
            for t in good:
                if t.status not in [Trial.PAUSED, Trial.RUNNING]:
                    raise Exception("Trial with unexpected status encountered")
                if bracket.continue_trial(t):
                    if t.status == Trial.PAUSED:
                        t.unpause()
                    elif t.status == Trial.RUNNING:
                        action = TrialScheduler.CONTINUE
        return action

    def _cleanup_trial(self, trial_runner, t, bracket, hard=False):
        """Bookkeeping for trials finished. If `hard=True`, then
        this scheduler will force the trial_runner to release resources.

        Otherwise, only clean up trial information locally."""
        self._num_stopped += 1
        if hard:
            trial_runner._stop_trial(t)
        bracket.cleanup_trial(t)

    def _cleanup_bracket(self, trial_runner, bracket):
        """Cleans up bracket after bracket is completely finished.
        Lets the last trial continue to run until termination condition
        kicks in."""
        for trial in bracket.current_trials():
            if (trial.status == Trial.PAUSED):
                self._cleanup_trial(
                    trial_runner, trial, bracket,
                    hard=True)

    def on_trial_complete(self, trial_runner, trial, result):
        """Cleans up trial info from bracket if trial completed early."""

        bracket, _ = self._trial_info[trial]
        self._cleanup_trial(trial_runner, trial, bracket, hard=False)
        self._process_bracket(trial_runner, bracket, trial)

    def on_trial_error(self, trial_runner, trial):
        """Cleans up trial info from bracket if trial errored early."""

        bracket, _ = self._trial_info[trial]
        self._cleanup_trial(trial_runner, trial, bracket, hard=False)
        self._process_bracket(trial_runner, bracket, trial)

    def choose_trial_to_run(self, trial_runner, *args):
        """Fair scheduling within iteration by completion percentage.
        List of trials not used since all trials are tracked as state
        of scheduler.

        If iteration is occupied (ie, no trials to run), then look into
        next iteration."""

        for hyperband in self._hyperbands:
            for bracket in sorted(hyperband,
                                  key=lambda b: b.completion_percentage()):
                for trial in bracket.current_trials():
                    if (trial.status == Trial.PENDING and
                            trial_runner.has_resources(trial.resources)):
                        return trial
        return None

    def debug_string(self):
        brackets = [
            "({0}/{1})".format(
                len(bracket._live_trials), len(bracket._all_trials))
            for band in self._hyperbands for bracket in band]
        return " ".join([
            "Using HyperBand:",
            "num_stopped={}".format(self._num_stopped),
            "total_brackets={}".format(
                sum(len(band) for band in self._hyperbands)),
            " ".join(brackets)
            ])


class Bracket():
    """Logical object for tracking Hyperband bracket progress. Keeps track
    of proper parameters as designated by HyperBand.

    Also keeps track of progress to ensure good scheduling.
    """
    def __init__(self, time_attr, max_trials, init_t_attr, eta, s):
        self._live_trials = {}  # maps trial -> current result
        self._all_trials = []
        self._time_attr = time_attr  # attribute to

        self._n = self._n0 = max_trials
        self._r = self._r0 = init_t_attr
        self._cumul_r = self._r0

        self._eta = eta
        self._halves = s

        self._total_work = self._calculate_total_work(self._n0, self._r0, s)
        self._completed_progress = 0

    def add_trial(self, trial):
        """Add trial to bracket assuming bracket is not filled.

        At a later iteration, a newly added trial will be given equal
        opportunity to catch up."""
        assert not self.filled(), "Cannot add trial to filled bracket!"
        self._live_trials[trial] = None
        self._all_trials.append(trial)

    def cur_iter_done(self):
        """Checks if all iterations have completed.

        TODO(rliaw): also check that `t.iterations == self._r`"""
        return all(self._get_result_time(result) >= self._cumul_r
                   for result in self._live_trials.values())

    def finished(self):
        return self._halves == 0 and self.cur_iter_done()

    def current_trials(self):
        return list(self._live_trials)

    def continue_trial(self, trial):
        result = self._live_trials[trial]
        if self._get_result_time(result) < self._cumul_r:
            return True
        else:
            return False

    def filled(self):
        """We will only let new trials be added at current level,
        minimizing the need to backtrack and bookkeep previous medians"""
        return len(self._live_trials) == self._n

    def successive_halving(self, reward_attr):
        assert self._halves > 0
        self._halves -= 1
        self._n /= self._eta
        self._n = int(np.ceil(self._n))
        self._r *= self._eta
        self._r = int((self._r))
        self._cumul_r += self._r
        sorted_trials = sorted(
            self._live_trials,
            key=lambda t: getattr(self._live_trials[t], reward_attr))

        good, bad = sorted_trials[-self._n:], sorted_trials[:-self._n]
        return good, bad

    def update_trial_stats(self, trial, result):
        """Update result for trial. Called after trial has finished
        an iteration - will decrement iteration count.

        TODO(rliaw): The other alternative is to keep the trials
        in and make sure they're not set as pending later."""

        assert trial in self._live_trials
        assert self._get_result_time(result) >= 0

        delta = self._get_result_time(result) - \
            self._get_result_time(self._live_trials[trial])
        assert delta >= 0
        self._completed_progress += delta
        self._live_trials[trial] = result

    def cleanup_trial(self, trial):
        """Clean up statistics tracking for terminated trials (either by force
        or otherwise).

        This may cause bad trials to continue for a long time, in the case
        where all the good trials finish early and there are only bad trials
        left in a bracket with a large max-iteration."""
        assert trial in self._live_trials
        del self._live_trials[trial]

    def completion_percentage(self):
        """Returns a progress metric.

        This will not be always finish with 100 since dead trials
        are dropped."""
        return self._completed_progress / self._total_work

    def _get_result_time(self, result):
        if result is None:
            return 0
        return getattr(result, self._time_attr)

    def _calculate_total_work(self, n, r, s):
        work = 0
        for i in range(s+1):
            work += int(n) * int(r)
            n /= self._eta
            n = int(np.ceil(n))
            r *= self._eta
            r = int(r)
        return work

    def __repr__(self):
        status = ", ".join([
            "n={}".format(self._n),
            "r={}".format(self._r),
            "progress={}".format(self.completion_percentage())
            ])
        trials = ", ".join([t.status for t in self._live_trials])
        return "Bracket({})[{}]".format(status, trials)
