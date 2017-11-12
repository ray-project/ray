from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from ray.tune.hyperband import HyperBandScheduler
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.result import TrainingResult
from ray.tune.trial import Trial
from ray.tune.trial_scheduler import TrialScheduler


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

    def testMedianStoppingSoftStop(self):
        rule = MedianStoppingRule(
            grace_period=0, min_samples_required=1, hard_stop=False)
        t1, t2 = self.basicSetup(rule)
        rule.on_trial_complete(None, t1, result(10, 1000))
        rule.on_trial_complete(None, t2, result(10, 1000))
        t3 = Trial("t3", "PPO")
        self.assertEqual(
            rule.on_trial_result(None, t3, result(1, 260)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t3, result(2, 260)),
            TrialScheduler.PAUSE)

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
    def basicSetup(self):
        """s_max_1 = 3;
        brackets: iter (n, r) | iter (n, r) | iter (n, r)
            (9, 1) -> (3, 3) -> (1, 9)
            (9, 1) -> (3, 3) -> (1, 9)
        """

        sched = HyperBandScheduler(9, eta=3)
        for i in range(17):
            t = Trial("t%d" % i, "__fake")
            sched.on_trial_add(None, t)

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

    def testBasicHalving(self):
        sched = self.advancedSetup()
        mock_runner = _MockTrialRunner()
        filled_band = sched._hyperbands[0]
        big_bracket = filled_band[0]
        bracket_trials = big_bracket.current_trials()

        for t in bracket_trials:
            mock_runner._launch_trial(t)

        for i, t in enumerate(bracket_trials):
            if i == len(bracket_trials) - 1:
                break
            self.assertEqual(
                TrialScheduler.PAUSE,
                sched.on_trial_result(mock_runner, t, result(i, 10)))
            mock_runner._pause_trial(t)
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(
                mock_runner, bracket_trials[-1], result(7, 12)))

    def testSuccessiveHalving(self):
        sched = HyperBandScheduler(9, eta=3)
        for i in range(9):
            t = Trial("t%d" % i, "__fake")
            sched.on_trial_add(None, t)
        filled_band = sched._hyperbands[0]
        big_bracket = filled_band[0]
        mock_runner = _MockTrialRunner()

        current_length = len(big_bracket.current_trials())
        for i in range(current_length):
            trl = sched.choose_trial_to_run(mock_runner)
            mock_runner._launch_trial(trl)
            while True:
                status = sched.on_trial_result(mock_runner, trl, result(1, 10))
                if status == TrialScheduler.CONTINUE:
                    continue
                elif status == TrialScheduler.PAUSE:
                    mock_runner._pause_trial(trl)
                    break

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
        sched = HyperBandScheduler(9, eta=3)
        t1 = Trial("t1", "__fake")
        t2 = Trial("t2", "__fake")
        sched.on_trial_add(None, t1)
        sched.on_trial_add(None, t2)
        mock_runner = _MockTrialRunner()
        filled_band = sched._hyperbands[0]
        big_bracket = filled_band[0]
        bracket_trials = big_bracket.current_trials()

        for t in bracket_trials:
            mock_runner._launch_trial(t)

        sched.on_trial_error(mock_runner, t2)
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(mock_runner, t1, result(3, 10)))

    def testTrialEndedEarly(self):
        sched = HyperBandScheduler(9, eta=3)
        t1 = Trial("t1", "__fake")
        t2 = Trial("t2", "__fake")
        sched.on_trial_add(None, t1)
        sched.on_trial_add(None, t2)
        mock_runner = _MockTrialRunner()
        filled_band = sched._hyperbands[0]
        big_bracket = filled_band[0]
        bracket_trials = big_bracket.current_trials()

        for t in bracket_trials:
            mock_runner._launch_trial(t)

        sched.on_trial_complete(mock_runner, t2, result(5, 10))
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(mock_runner, t1, result(3, 12)))

    def testAddAfterHalf(self):
        sched = HyperBandScheduler(9, eta=3)
        for i in range(2):
            t = Trial("t%d" % i, "__fake")
            sched.on_trial_add(None, t)
        mock_runner = _MockTrialRunner()
        filled_band = sched._hyperbands[0]
        big_bracket = filled_band[0]
        bracket_trials = big_bracket.current_trials()

        for t in bracket_trials:
            mock_runner._launch_trial(t)

        for i, t in enumerate(bracket_trials):
            if i == len(bracket_trials) - 1:
                break
            self.assertEqual(
                TrialScheduler.PAUSE,
                sched.on_trial_result(mock_runner, t, result(i, 10)))
            mock_runner._pause_trial(t)
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(
                mock_runner, bracket_trials[-1], result(7, 12)))
        t = Trial("t%d" % 5, "__fake")
        sched.on_trial_add(None, t)
        self.assertEqual(4, big_bracket._live_trials[t][1])

    def testDone(self):
        sched = HyperBandScheduler(3, eta=3)
        mock_runner = _MockTrialRunner()
        trials = [Trial("t%d" % i, "__fake") for i in range(5)]
        for t in trials:
            sched.on_trial_add(None, t)

        filled_band = sched._hyperbands[0]
        brack = filled_band[1]
        bracket_trials = brack.current_trials()
        for t in bracket_trials:
            mock_runner._launch_trial(t)
        for i in range(3):
            res = sched.on_trial_result(
                mock_runner, bracket_trials[-1], result(i, 10))
        self.assertEqual(res, TrialScheduler.PAUSE)
        mock_runner._pause_trial(bracket_trials[-1])
        for i in range(3):
            res = sched.on_trial_result(
                mock_runner, bracket_trials[-2], result(i, 10))
        self.assertEqual(res, TrialScheduler.STOP)
        self.assertEqual(len(brack.current_trials()), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
