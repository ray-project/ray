from __future__ import absolute_import
from __future__ import division

import collections
import numpy as np

from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial


class HyperBandScheduler(FIFOScheduler):
    """ Contains 3 levels for a logical hierarchy
    all_bracket_sets -> hyperband_iteration
        hyperband_iteration -> bracket
            bracket -> bracket_iteration
    """

    def __init__(self, max_iter, eta=3):
        """
        args:
            max_iter (int): maximum iterations per configuration
            eta (int): # defines downsampling rate (default=3)
        """

        FIFOScheduler.__init__(self)
        self._eta = eta

        logeta = lambda x: log(x)/log(eta)

        # number of brackets per hyperband iteration
        self._s_max_1 = s_max_1 = int(logeta(max_iter)) + 1

        # total number of iterations per execution of Succesive Halving (n,r)
        B = s_max_1 * max_iter

        # bracket trial count total
        self._get_n0 = lambda s: int(np.ceil(B/max_iter/(s+1)*eta**s))

        # bracket initial iterations
        self._get_r0 = lambda s: max_iter*eta**(-s)

        self._all_hb_iters = []  # list of hyperband iterations
        self._trial_info = {}

        self._state = {"bracket": None,
                       "s": 0, # TODO(rliaw): This may be hard to follow
                       "cur_hb_iter": None}

    def on_trial_add(self, trial):
        """On a new trial add,
        if current bracket is not filled, add to current bracket
        else, if current hp iteration is not filled,
            create new bracket, add to current bracket
        else,
            create new iteration, create new bracket, add to bracket
        """

        if self._state["bracket"] is None or self._state["bracket"].filled():

            # if current iteration is filled
            if self._state["cur_hb_iter"] is None or self._cur_iter_filled():
                self._all_hb_iters.append([])
                self._state["cur_hb_iter"] = self._all_hb_iters[-1]

            # create new bracket
            # _state["s"] starts at 0, -1 % (s_max_1) will set to smax_1 - 1
            self._state["s"] = (self._state["s"] - 1) % self._s_max_1
            s = self._state["s"]
            new_brkt = Bracket(self._get_n0(s), self._get_r0(s), self._eta, s)
            self._state["cur_hb_iter"].append(new_brkt)
            self._state["bracket"] = new_brkt

        self._state["bracket"].add_trial(trial) # trial to bracket
        self._trial_info[trial] = bracket, hb_iteration

    def _cur_iter_filled(self):
        if len(self._state["cur_hb_iter"]) == self._s_max_1:
            assert self._state["s"] == 0:
            return True
        else:
            return False


    def on_trial_result(self, trial_runner, trial, result):
        # assert trial.iteration < max_iter, should
        bracket, _ = self._trial_info[trial]
        bracket.update(trial, result)
        if bracket.continue_trial(trial):
            return TrialScheduler.CONTINUE

        if bracket.next_iter_ready():
            good, bad = bracket.successive_halving()
            for t in bad:
                trial_runner._stop_trial(t)
            for t in good:
                if t.status == Trial.PAUSED:
                    trial_runner.set_pending(t)
            # for current trial, continue so that no checkpointing is needed
            return TrialScheduler.CONTINUE

        else:
            return TrialScheduler.PAUSE

    def on_trial_complete(self, trial_runner, trial, result):
        bracket, _ = self._trial_info[trial]
        bracket.cleanup_trial_early(trial)

    def on_trial_error(self, trial_runner, trial):
        bracket, _ = self._trial_info[trial]
        bracket.cleanup_trial_early(trial)

    def choose_trial_to_run(self, *args):
        """Fair scheduling within iteration by completion percentage.
        If iteration is occupied (ie, no trials to run),
            then look into next iteration
        """
        for hyperband_iteration in all_hyperband_iterations:
            for bracket in sorted(hyperband_iteration,
                                  key=lambda b: b.completion_percentage()):
                for trial in bracket.cur_trials:
                    if (trial.status == Trial.PENDING and
                            self._has_resources(trial.resources)):
                        return trial
        return None


    def debug_string(self):
        return "Using HyperBand: num_stopped={}.".format(
            self._num_stopped)

class Bracket():
    def __init__(self, max_trials, init_iters, eta, s):
        self._live_trials = {}  # stores (result, itrs left before halving)
        self._all_trials = []
        self._n = self._n0 = max_trials
        self._r = self._r0 = init_iters
        self._eta = eta
        self._halves = s

        self._total_work = self._calculate_total_work(self._n0, self._r0, s)
        self._completed_progress = 0

    def add_trial(self, trial):
        assert not self.filled()
        self._live_trials[trial] = (None, self._r0)
        self._all_trials.append(trial)

    def next_iter_ready(self):
        """
        TODO(rliaw): also check that t.iterations == self._r
        """
        return all(t.PAUSED for t in self._live_trials)

    def continue_trial(self, trial):
        result, itr = self._live_trials[trial]
        if itr > 0:
            self._live_trials[trial] = (result, itr - 1)
            return True
        else:
            return False

    def filled(self):
        return len(self._all_trials) == self._n0

    def successive_halving(self):
        assert self._halves > 0
        self._halves -= 1
        self._n /= self._eta
        self._n = int(np.ceil(self._n))
        self._r *= self._eta
        sorted_trials = sorted(self.cur_trials,
                               key=lambda t: self.cur_trials[t].reward)

        good, bad = sorted_trials[:int(self._n)], sorted_trials[int(self._n):]
        for trial in bad:
            del self.cur_trials[bad]

        # reset good trials to track updated iterations
        for t in self.cur_trials:
            res, old_itr = self.cur_trials[t]
            self.cur_trials[t] = (res, self._r)
        return good, bad

    def update(self, trial, result):
        """Update result for trial; if error or completed, drop;
        this may cause bad trials to continue for a long time.
        The other alternative is to keep the trials in and make sure
        they're not set as pending later """

        assert trial in self._live_trials
        if end:
            del self._live_trials[trial]
        else:
            self._live_trials[trial] = result
            self._completed_progress += 1

    def cleanup_trial_early(self, trial):
        """
        Clean up statistics tracking for trial that terminated early
        """
        assert trial in self._live_trials
        del self._live_trials[trial]

    def completion_percentage(self):
        """ Will not be 100 since dead trials are dropped """
        return self._completed_progress / self._total_work

    def _calculate_total_work(self, n, r, s):
        work = 0
        for i in range(s+1):
            work += int(n) * int(r)
            n /= self._eta
            n = int(np.ceil(n))
            r *= self._eta
        return work
