from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import time

from ray.tune.result import TrainingResult
from ray.tune.trial import Trial
from ray.tune.trial_scheduler import MedianStoppingRule, TrialScheduler
from ray.tune.hyperband import HyperBandScheduler


def result(t, rew):
    return TrainingResult(time_total_s=t, episode_reward_mean=rew)


class EarlyStoppingSuite(unittest.TestCase):
    def basicSetup(self, rule):
        t1 = Trial("t1", "PPO")  # mean is 450, max 900, t_max=10
        t2 = Trial("t2", "PPO")  # mean is 450, max 450, t_max=5
        for i in range(10):
            self.assertEqual(
                rule.on_trial_result(None, t1, result(i, i * 100)),
                TrialScheduler.CONTINUE)
        for i in range(5):
            self.assertEqual(
                rule.on_trial_result(None, t2, result(i, 450)),
                TrialScheduler.CONTINUE)
        return t1, t2

    def testMedianStoppingConstantPerf(self):
        rule = MedianStoppingRule(grace_period=0, min_samples_required=1)
        t1, t2 = self.basicSetup(rule)
        rule.on_trial_complete(None, t1, result(10, 1000))
        self.assertEqual(
            rule.on_trial_result(None, t2, result(5, 450)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t2, result(6, 0)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t2, result(10, 450)),
            TrialScheduler.STOP)

    def testMedianStoppingOnCompleteOnly(self):
        rule = MedianStoppingRule(grace_period=0, min_samples_required=1)
        t1, t2 = self.basicSetup(rule)
        self.assertEqual(
            rule.on_trial_result(None, t2, result(100, 0)),
            TrialScheduler.CONTINUE)
        rule.on_trial_complete(None, t1, result(10, 1000))
        self.assertEqual(
            rule.on_trial_result(None, t2, result(101, 0)),
            TrialScheduler.STOP)

    def testMedianStoppingGracePeriod(self):
        rule = MedianStoppingRule(grace_period=2.5, min_samples_required=1)
        t1, t2 = self.basicSetup(rule)
        rule.on_trial_complete(None, t1, result(10, 1000))
        rule.on_trial_complete(None, t2, result(10, 1000))
        t3 = Trial("t3", "PPO")
        self.assertEqual(
            rule.on_trial_result(None, t3, result(1, 10)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t3, result(2, 10)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t3, result(3, 10)),
            TrialScheduler.STOP)

    def testMedianStoppingMinSamples(self):
        rule = MedianStoppingRule(grace_period=0, min_samples_required=2)
        t1, t2 = self.basicSetup(rule)
        rule.on_trial_complete(None, t1, result(10, 1000))
        t3 = Trial("t3", "PPO")
        self.assertEqual(
            rule.on_trial_result(None, t3, result(3, 10)),
            TrialScheduler.CONTINUE)
        rule.on_trial_complete(None, t2, result(10, 1000))
        self.assertEqual(
            rule.on_trial_result(None, t3, result(3, 10)),
            TrialScheduler.STOP)

    def testMedianStoppingUsesMedian(self):
        rule = MedianStoppingRule(grace_period=0, min_samples_required=1)
        t1, t2 = self.basicSetup(rule)
        rule.on_trial_complete(None, t1, result(10, 1000))
        rule.on_trial_complete(None, t2, result(10, 1000))
        t3 = Trial("t3", "PPO")
        self.assertEqual(
            rule.on_trial_result(None, t3, result(1, 260)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t3, result(2, 260)),
            TrialScheduler.STOP)

    def testAlternateMetrics(self):
        def result2(t, rew):
            return TrainingResult(training_iteration=t, neg_mean_loss=rew)

        rule = MedianStoppingRule(
            grace_period=0, min_samples_required=1,
            time_attr='training_iteration', reward_attr='neg_mean_loss')
        t1 = Trial("t1", "PPO")  # mean is 450, max 900, t_max=10
        t2 = Trial("t2", "PPO")  # mean is 450, max 450, t_max=5
        for i in range(10):
            self.assertEqual(
                rule.on_trial_result(None, t1, result2(i, i * 100)),
                TrialScheduler.CONTINUE)
        for i in range(5):
            self.assertEqual(
                rule.on_trial_result(None, t2, result2(i, 450)),
                TrialScheduler.CONTINUE)
        rule.on_trial_complete(None, t1, result2(10, 1000))
        self.assertEqual(
            rule.on_trial_result(None, t2, result2(5, 450)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t2, result2(6, 0)),
            TrialScheduler.CONTINUE)


class _MockTrialRunner():
    def _stop_trial(self, trial):
        trial.stop()

    def has_resources(self, resources):
        return True

    def _pause_trial(self, trial):
        trial.status = Trial.PAUSED

    def _launch_trial(self, trial):
        trial.status = Trial.RUNNING


class HyperbandSuite(unittest.TestCase):

    def schedulerSetup(self, num_trials):
        """Setup a scheduler and Runner with max Iter = 9

        (9, 1) -> (3, 3) -> (1, 9)"""
        sched = HyperBandScheduler(9, eta=3)
        for i in range(num_trials):
            t = Trial("t%d" % i, "__fake")
            sched.on_trial_add(None, t)
        runner = _MockTrialRunner()
        return sched, runner

    def basicSetup(self):
        """Setup and verify full band.
        """

        sched, _ = self.schedulerSetup(17)

        self.assertEqual(len(sched._hyperbands), 1)
        self.assertEqual(sched._cur_band_filled(), True)

        filled_band = sched._hyperbands[0]
        for bracket in filled_band:
            self.assertEqual(bracket.filled(), True)
        return sched

    def advancedSetup(self):
        sched = self.basicSetup()
        for i in range(3):
            t = Trial("t%d" % (i + 20), "__fake")
            sched.on_trial_add(None, t)

        self.assertEqual(sched._cur_band_filled(), False)

        unfilled_band = sched._hyperbands[1]
        self.assertEqual(len(unfilled_band), 1)
        self.assertEqual(len(sched._hyperbands[1]), 1)
        bracket = unfilled_band[0]
        self.assertEqual(bracket.filled(), False)
        self.assertEqual(len(bracket.current_trials()), 3)

        return sched

    def stopTrial(self, trial, mock_runner):
        self.assertNotEqual(trial.status, Trial.TERMINATED)
        mock_runner._stop_trial(trial)


    def testSuccessiveHalving(self):
        sched, mock_runner = self.schedulerSetup(9)
        filled_band = sched._hyperbands[0][0]
        big_bracket = filled_band

        current_length = len(big_bracket.current_trials())
        for i in range(current_length):
            trl = sched.choose_trial_to_run(mock_runner)
            mock_runner._launch_trial(trl)

        # Provides results from 0 to 8 in order, keeping the last one running
        for i, trl in enumerate(big_bracket.current_trials()):
            status = sched.on_trial_result(mock_runner, trl, result(1, i))
            if status == TrialScheduler.CONTINUE:
                continue
            elif status == TrialScheduler.PAUSE:
                mock_runner._pause_trial(trl)
            elif status == TrialScheduler.STOP:
                self.assertNotEqual(trial.status, Trial.TERMINATED)
                self.stopTrial(trl, mock_runner)

        current_length = len(big_bracket.current_trials())
        self.assertEqual(status, TrialScheduler.CONTINUE)
        self.assertEqual(current_length, 3)

        for i in range(current_length - 1):
            trl = sched.choose_trial_to_run(mock_runner)
            mock_runner._launch_trial(trl)

        # Provides results from 2 to 0 in order, killing the last one
        for i, trl in reversed(list(enumerate(big_bracket.current_trials()))):
            for j in range(3):
                status = sched.on_trial_result(mock_runner, trl, result(1, i))
            if status == TrialScheduler.CONTINUE:
                continue
            elif status == TrialScheduler.PAUSE:
                mock_runner._pause_trial(trl)
            elif status == TrialScheduler.STOP:
                self.stopTrial(trl, mock_runner)

        self.assertEqual(status, TrialScheduler.STOP)
        trl = sched.choose_trial_to_run(mock_runner)
        for i in range(9):
            status = sched.on_trial_result(mock_runner, trl, result(1, i))
        self.assertEqual(status, TrialScheduler.STOP)
        self.assertEqual(len(big_bracket.current_trials()), 0)
        self.assertEqual(sched._num_stopped, 9)

    def testBasicRun(self):
        sched = self.advancedSetup()
        mock_runner = _MockTrialRunner()
        trl = sched.choose_trial_to_run(mock_runner)
        while trl:
            if sched._trial_info[trl][1] > 0:
                first_band = sched._hyperbands[0]
                trials = [t for b in first_band for t in b._live_trials]
                self.assertEqual(
                    all(t.status == Trial.RUNNING for t in trials),
                    True)
            mock_runner._launch_trial(trl)
            res = sched.on_trial_result(mock_runner, trl, result(1, 10))
            if res is TrialScheduler.PAUSE:
                mock_runner._pause_trial(trl)
            trl = sched.choose_trial_to_run(mock_runner)

        self.assertEqual(
                    all(t.status == Trial.RUNNING for t in trials), True)

    def testTrialErrored(self):
        sched, mock_runner = self.schedulerSetup(2)
        t1, t2 = list(sched._state["bracket"].current_trials())
        mock_runner._launch_trial(t1)
        mock_runner._launch_trial(t2)

        sched.on_trial_error(mock_runner, t2)
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(mock_runner, t1, result(1, 10)))

    def testTrialErrored2(self):
        """Check successive halving happened even when last trial failed"""
        sched, mock_runner = self.schedulerSetup(9)
        trials = list(sched._state["bracket"].current_trials())
        for t in trials[:-1]:
            mock_runner._launch_trial(t)
            sched.on_trial_result(mock_runner, t, result(1, 10))

        mock_runner._launch_trial(trials[-1])
        sched.on_trial_error(mock_runner, trials[-1])
        self.assertEqual(len(sched._state["bracket"].current_trials()), 3)

    def testTrialEndedEarly(self):
        sched, mock_runner = self.schedulerSetup(2)
        trials = list(sched._state["bracket"].current_trials())
        for t in trials:
            mock_runner._launch_trial(t)

        sched.on_trial_complete(mock_runner, trials[-1], result(1, 12))
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(mock_runner, trials[0], result(1, 12)))

    def testTrialEndedEarly2(self):
        """Check successive halving happened even when last trial finished"""
        sched, mock_runner = self.schedulerSetup(9)
        trials = list(sched._state["bracket"].current_trials())
        for t in trials[:-1]:
            mock_runner._launch_trial(t)
            sched.on_trial_result(mock_runner, t, result(1, 10))

        mock_runner._launch_trial(trials[-1])
        sched.on_trial_complete(mock_runner, trials[-1], result(1, 12))
        self.assertEqual(len(sched._state["bracket"].current_trials()), 3)

    def testAddAfterHalving(self):
        sched, mock_runner = self.schedulerSetup(2)
        bracket_trials = sched._state["bracket"].current_trials()

        for t in bracket_trials:
            mock_runner._launch_trial(t)

        for i, t in enumerate(bracket_trials):
            res = sched.on_trial_result(
                mock_runner, t, result(1, i))
        self.assertEqual(res, TrialScheduler.CONTINUE)
        t = Trial("t%d" % 5, "__fake")
        sched.on_trial_add(None, t)
        self.assertEqual(3 + 1, sched._state["bracket"]._live_trials[t][1])

    def testTiming(self):
        sched = HyperBandScheduler(3, max_hours=10/3600, eta=3)
        mock_runner = _MockTrialRunner()
        trials = [Trial("t%d" % i, "__fake") for i in range(2)]
        for t in trials:
            sched.on_trial_add(None, t)
        filled_band = sched._hyperbands[0]
        brack = filled_band[0]
        bracket_trials = brack.current_trials()
        for t in bracket_trials:
            mock_runner._launch_trial(t)
        time.sleep(10)
        self.assertEqual(TrialScheduler.STOP, sched.on_trial_result(
                mock_runner, bracket_trials[-1], result(1, 10)))
        self.assertIsNone(sched.choose_trial_to_run(mock_runner))
        self.assertIsNone(sched.on_trial_add(None, Trial("t6", "__fake")))


if __name__ == "__main__":
    unittest.main(verbosity=2)
