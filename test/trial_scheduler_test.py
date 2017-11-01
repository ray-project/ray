from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import os

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


class HyperbandSuite(unittest.TestCase):
    def basicSetup(self):
        sched = HyperBandScheduler(9, eta=3)
        for i in range(17):
            t = Trial("t%d" % i, "PPO")
            sched.on_trial_add(None, t)

        self.assertEqual(len(sched._hyperbands), 1)
        unfilled_band = sched._hyperbands[0]
        self.assertEqual(self._cur_band_filled(), True)

        for i in range(3):
            t = Trial("t%d" % (i + 10), "PPO")
            sched.on_trial_add(None, t)
        self.assertEqual(self._cur_band_filled(), False)

        filled_band = sched._hyperbands[0]
        unfilled_band = sched._hyperbands[1]

        self.assertEqual(len(filled_band), 3)
        self.assertEqual(len(unfilled), 1)

        for bracket in filled_band:
            self.assertEqual(bracket.filled(), True)

        self.assertEqual(len(sched._hyperbands[1]), 1)
        for bracket in unfilled_band:
            self.assertEqual(bracket.filled(), False)
            self.assertEqual(len(bracket.current_trials()), 3)

        return sched

    def advancedSetup(self):
        pass

    def testSuccessiveHalving(self):
        sched = self.basicSetup(sched)
        # progress

    def testTrialErrored(self):
        # rule = MedianStoppingRule(grace_period=0, min_samples_required=1)
        # t1, t2 = self.basicSetup(rule)
        # rule.on_trial_complete(None, t1, result(10, 1000))
        # self.assertEqual(
        #     rule.on_trial_result(None, t2, result(5, 450)),
        #     TrialScheduler.CONTINUE)
        # self.assertEqual(
        #     rule.on_trial_result(None, t2, result(6, 0)),
        #     TrialScheduler.CONTINUE)
        # self.assertEqual(
        #     rule.on_trial_result(None, t2, result(10, 450)),
        #     TrialScheduler.STOP)
        pass

    def testTrialEndedEarly(self):
        pass
        # rule = MedianStoppingRule(grace_period=0, min_samples_required=1)
        # t1, t2 = self.basicSetup(rule)
        # rule.on_trial_complete(None, t1, result(10, 1000))
        # self.assertEqual(
        #     rule.on_trial_result(None, t2, result(5, 450)),
        #     TrialScheduler.CONTINUE)
        # self.assertEqual(
        #     rule.on_trial_result(None, t2, result(6, 0)),
        #     TrialScheduler.CONTINUE)
        # self.assertEqual(
        #     rule.on_trial_result(None, t2, result(10, 450)),
        #     TrialScheduler.STOP)

    def testTrialAddedAfterHalving(self):
        pass
        # rule = MedianStoppingRule(grace_period=0, min_samples_required=1)
        # t1, t2 = self.basicSetup(rule)
        # rule.on_trial_complete(None, t1, result(10, 1000))
        # self.assertEqual(
        #     rule.on_trial_result(None, t2, result(5, 450)),
        #     TrialScheduler.CONTINUE)
        # self.assertEqual(
        #     rule.on_trial_result(None, t2, result(6, 0)),
        #     TrialScheduler.CONTINUE)
        # self.assertEqual(
        #     rule.on_trial_result(None, t2, result(10, 450)),
        #     TrialScheduler.STOP)
if __name__ == "__main__":
    unittest.main(verbosity=2)
