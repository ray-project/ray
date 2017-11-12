from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial


def calculate_bracket_count(max_iter, eta):
    return int(np.log(max_iter)/np.log(eta)) + 1


class HyperBandScheduler(FIFOScheduler):
    """Implements HyperBand.

    This implementation contains 3 logical levels.
    Each HyperBand iteration is a "band". There can be multiple
    bands running at once, and there can be 1 band that is incomplete.

    In each band, there are at most `s` + 1 brackets.
    `s` is a value determined by given parameters, and assigned on
    a cyclic basis.

    In each bracket, there are at most `n(s)` trials, indicating that
    `n` is a function of `s`. These trials go through a series of
    halving procedures, dropping lowest performers. Multiple
    brackets are running at once.

    Trials added will be inserted into the most recent bracket
    and band and will spill over to new brackets/bands accordingly.
    """

    def __init__(self, max_iter=200, eta=3):
        """
        args:
            max_iter (int): maximum iterations per configuration
            eta (int): # defines downsampling rate (default=3)
        """
        assert max_iter > 0, "Max Iterations not valid!"
        assert eta > 1, "Downsampling rate (eta) not valid!"

        FIFOScheduler.__init__(self)
        self._eta = eta
        self._s_max_1 = s_max_1 = calculate_bracket_count(max_iter, eta)
        # total number of iterations per execution of Succesive Halving (n,r)
        B = s_max_1 * max_iter
        # bracket trial count total
        self._get_n0 = lambda s: int(np.ceil(B/max_iter/(s+1)*eta**s))
        # bracket initial iterations
        self._get_r0 = lambda s: int(max_iter*eta**(-s))
        self._hyperbands = [[]]  # list of hyperband iterations
        self._trial_info = {}  # Stores Trial -> Bracket, Band Iteration

        # Tracks state for new trial add
        self._state = {"bracket": None,
                       "band_idx": 0}
        self._num_stopped = 0

    def on_trial_add(self, trial_runner, trial):
        """On a new trial add, if current bracket is not filled,
        add to current bracket. Else, if current hp iteration is not filled,
        create new bracket, add to current bracket.
        Else, create new iteration, create new bracket, add to bracket.

        TODO(rliaw): This is messy."""

        cur_bracket = self._state["bracket"]
        cur_band = self._hyperbands[self._state["band_idx"]]
        if cur_bracket is None or cur_bracket.filled():

            # if current iteration is filled, create new iteration
            if self._cur_band_filled():
                cur_band = []
                self._hyperbands.append(cur_band)
                self._state["band_idx"] += 1

            # cur_band will always be less than s_max or else filled
            s = self._s_max_1 - len(cur_band) - 1
            assert s >= 0, "Current band is filled but adding bracket!"

            # create new bracket
            cur_bracket = Bracket(self._get_n0(s),
                                  self._get_r0(s), self._eta, s)
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
        When bracket iteration is done, Trials will be successively halved,
        and during each halving phase, bad trials will be stopped while good
        trials will return to "PENDING". This scheduler will not start trials
        but will stop trials. The current running trial will not be handled,
        as the trialrunner will be given control to handle it.

        # TODO(rliaw) should be only called if trial has not errored"""
        bracket, _ = self._trial_info[trial]
        bracket.update_trial_stats(trial, result)
        if bracket.continue_trial(trial):
            return TrialScheduler.CONTINUE

        signal = TrialScheduler.PAUSE

        if bracket.cur_iter_done():
            if bracket.finished():
                self._cleanup_bracket(trial_runner, bracket)
                return TrialScheduler.STOP
            # what if bracket is done and trial not completed?
            good, bad = bracket.successive_halving()
            # kill bad trials
            for t in bad:
                self._num_stopped += 1
                if t.status == Trial.PAUSED:
                    trial_runner._stop_trial(t)
                    bracket.cleanup_trial_early(t)
                elif t is trial:
                    signal = TrialScheduler.STOP
                else:
                    raise Exception("Trial with unexpected status encountered")

            # ready the good trials
            for t in good:
                if t.status == Trial.PAUSED:
                    t.unpause()
                elif t is trial:
                    signal = TrialScheduler.CONTINUE
                else:
                    raise Exception("Trial with unexpected status encountered")

        return signal

    def _cleanup_bracket(self, trial_runner, bracket):
        """Cleans up bracket after bracket is completely finished.

        Bracket information will only be cleaned up after the trialrunner has
        finished its bookkeeping."""
        for t in bracket.current_trials():
            if t.status == Trial.PAUSED:
                trial_runner._stop_trial(t)
                bracket.cleanup_trial_early(t)

    def on_trial_complete(self, trial_runner, trial, result):
        """Cleans up trial info from bracket if trial completed early.

        Bracket information will only be cleaned up after the trialrunner has
        finished its bookkeeping."""
        bracket, _ = self._trial_info[trial]
        bracket.cleanup_trial_early(trial)

    def on_trial_error(self, trial_runner, trial):
        """Cleans up trial info from bracket if trial errored early.

        Bracket information will only be cleaned up after the trialrunner has
        finished its bookkeeping."""
        bracket, _ = self._trial_info[trial]
        bracket.cleanup_trial_early(trial)

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
        return " ".join([
            "Using HyperBand:",
            "num_stopped={}".format(self._num_stopped),
            "brackets={}".format(sum(len(band) for band in self._hyperbands))])


class Bracket():
    """Logical object for tracking Hyperband bracket progress. Keeps track
    of proper parameters as designated by HyperBand.

    Also keeps track of progress to ensure good scheduling.
    """
    def __init__(self, max_trials, init_iters, eta, s):
        self._live_trials = {}  # stores (result, itrs left before halving)
        self._all_trials = []
        self._n = self._n0 = max_trials
        self._r = self._r0 = init_iters
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
        self._live_trials[trial] = (None, self._cumul_r)
        self._all_trials.append(trial)

    def cur_iter_done(self):
        """Checks if all iterations have completed.

        TODO(rliaw): also check that `t.iterations == self._r`"""
        all_done = all(itr == 0 for _, itr in self._live_trials.values())
        return all_done

    def finished(self):
        return self._halves == 0 and self.cur_iter_done()

    def current_trials(self):
        return list(self._live_trials)

    def continue_trial(self, trial):
        _, itr = self._live_trials[trial]
        if itr > 0:
            return True
        else:
            return False

    def filled(self):
        """We will only let new trials be added at current level,
        minimizing the need to backtrack and bookkeep previous medians"""
        return len(self._live_trials) == self._n

    def successive_halving(self):
        assert self._halves > 0
        self._halves -= 1
        self._n /= self._eta
        self._n = int(np.ceil(self._n))
        self._r *= self._eta
        self._r = int(np.ceil(self._r))
        self._cumul_r += self._r
        sorted_trials = sorted(
            self._live_trials,
            key=lambda t: self._live_trials[t][0].episode_reward_mean)

        good, bad = sorted_trials[-self._n:], sorted_trials[:-self._n]

        # reset good trials to track updated iterations
        for t in good:
            res, old_itr = self._live_trials[t]
            self._live_trials[t] = (res, self._r)
        return good, bad

    def update_trial_stats(self, trial, result):
        """Update result for trial. Called after trial has finished
        an iteration - will decrement iteration count.

        TODO(rliaw): The other alternative is to keep the trials
        in and make sure they're not set as pending later."""

        assert trial in self._live_trials
        _, itr = self._live_trials[trial]
        assert itr > 0
        self._live_trials[trial] = (result, itr - 1)
        self._completed_progress += 1

    def cleanup_trial_early(self, trial):
        """Clean up statistics tracking for trial that terminated early.

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

    def _calculate_total_work(self, n, r, s):
        work = 0
        for i in range(s+1):
            work += int(n) * int(r)
            n /= self._eta
            n = int(np.ceil(n))
            r *= self._eta
        return work

    def __repr__(self):
        status = ", ".join([
            "n={}".format(self._n),
            "r={}".format(self._r),
            "progress={}".format(self.completion_percentage())
            ])
        trials = ", ".join([t.status for t in self._live_trials])
        return "Bracket({})[{}]".format(status, trials)
