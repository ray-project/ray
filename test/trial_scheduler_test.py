from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import numpy as np

from ray.tune.hyperband import HyperBandScheduler
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.result import TrainingResult
from ray.tune.trial import Trial
from ray.tune.trial_scheduler import TrialScheduler


def result(t, rew):
    return TrainingResult(time_total_s=t,
                          episode_reward_mean=rew,
                          training_iteration=int(t))


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

    def schedulerSetup(self, num_trials):
        """Setup a scheduler and Runner with max Iter = 9

        Bracketing is placed as follows:
        (5, 81);
        (8, 27) -> (3, 81);
        (15, 9) -> (5, 27) -> (2, 81);
        (34, 3) -> (12, 9) -> (4, 27) -> (2, 81);
        (81, 1) -> (27, 3) -> (9, 9) -> (3, 27) -> (1, 81);"""
        sched = HyperBandScheduler()
        for i in range(num_trials):
            t = Trial("t%d" % i, "__fake")
            sched.on_trial_add(None, t)
        runner = _MockTrialRunner()
        return sched, runner

    def default_statistics(self):
        """Default statistics for HyperBand"""
        sched = HyperBandScheduler()
        res = {
            str(s): {"n": sched._get_n0(s), "r": sched._get_r0(s)}
            for s in range(sched._s_max_1)
        }
        res["max_trials"] = sum(v["n"] for v in res.values())
        res["brack_count"] = sched._s_max_1
        res["s_max"] = sched._s_max_1 - 1
        return res

    def downscale(self, n, sched):
        return int(np.ceil(n / sched._eta))

    def process(self, trl, mock_runner, action):
        if action == TrialScheduler.CONTINUE:
            pass
        elif action == TrialScheduler.PAUSE:
            mock_runner._pause_trial(trl)
        elif action == TrialScheduler.STOP:
            self.stopTrial(trl, mock_runner)

    def basicSetup(self):
        """Setup and verify full band.
        """
        stats = self.default_statistics()
        sched, _ = self.schedulerSetup(stats["max_trials"])

        self.assertEqual(len(sched._hyperbands), 1)
        self.assertEqual(sched._cur_band_filled(), True)

        filled_band = sched._hyperbands[0]
        for bracket in filled_band:
            self.assertEqual(bracket.filled(), True)
        return sched

    def advancedSetup(self):
        sched = self.basicSetup()
        for i in range(4):
            t = Trial("t%d" % (i + 20), "__fake")
            sched.on_trial_add(None, t)

        self.assertEqual(sched._cur_band_filled(), False)

        unfilled_band = sched._hyperbands[-1]
        self.assertEqual(len(unfilled_band), 2)
        bracket = unfilled_band[-1]
        self.assertEqual(bracket.filled(), False)
        self.assertEqual(len(bracket.current_trials()), 7)

        return sched

    def stopTrial(self, trial, mock_runner):
        self.assertNotEqual(trial.status, Trial.TERMINATED)
        mock_runner._stop_trial(trial)

    def testConfigSameEta(self):
        sched = HyperBandScheduler()
        i = 0
        while not sched._cur_band_filled():
            t = Trial("t%d" % (i), "__fake")
            sched.on_trial_add(None, t)
            i += 1
        self.assertEqual(len(sched._hyperbands[0]), 5)
        self.assertEqual(sched._hyperbands[0][0]._n, 5)
        self.assertEqual(sched._hyperbands[0][0]._r, 81)
        self.assertEqual(sched._hyperbands[0][-1]._n, 81)
        self.assertEqual(sched._hyperbands[0][-1]._r, 1)

        sched = HyperBandScheduler(max_t=810)
        i = 0
        while not sched._cur_band_filled():
            t = Trial("t%d" % (i), "__fake")
            sched.on_trial_add(None, t)
            i += 1
        self.assertEqual(len(sched._hyperbands[0]), 5)
        self.assertEqual(sched._hyperbands[0][0]._n, 5)
        self.assertEqual(sched._hyperbands[0][0]._r, 810)
        self.assertEqual(sched._hyperbands[0][-1]._n, 81)
        self.assertEqual(sched._hyperbands[0][-1]._r, 10)

    def testConfigSameEtaSmall(self):
        sched = HyperBandScheduler(max_t=1)
        i = 0
        while len(sched._hyperbands) < 2:
            t = Trial("t%d" % (i), "__fake")
            sched.on_trial_add(None, t)
            i += 1
        self.assertEqual(len(sched._hyperbands[0]), 5)
        self.assertTrue(all(v is None for v in sched._hyperbands[0][1:]))

    def testSuccessiveHalving(self):
        """Setup full band, then iterate through last bracket (n=81)
        to make sure successive halving is correct."""
        stats = self.default_statistics()
        sched, mock_runner = self.schedulerSetup(stats["max_trials"])
        big_bracket = sched._state["bracket"]
        cur_units = stats[str(stats["s_max"])]["r"]
        # The last bracket will downscale 4 times
        for x in range(stats["brack_count"] - 1):
            trials = big_bracket.current_trials()
            current_length = len(trials)
            for trl in trials:
                mock_runner._launch_trial(trl)

            # Provides results from 0 to 8 in order, keeping last one running
            for i, trl in enumerate(trials):
                action = sched.on_trial_result(
                    mock_runner, trl, result(cur_units, i))
                if i < current_length - 1:
                    self.assertEqual(action, TrialScheduler.PAUSE)
                self.process(trl, mock_runner, action)

            self.assertEqual(action, TrialScheduler.CONTINUE)
            new_length = len(big_bracket.current_trials())
            self.assertEqual(new_length, self.downscale(current_length, sched))
            cur_units += int(cur_units * sched._eta)
        self.assertEqual(len(big_bracket.current_trials()), 1)

    def testHalvingStop(self):
        stats = self.default_statistics()
        num_trials = stats[str(0)]["n"] + stats[str(1)]["n"]
        sched, mock_runner = self.schedulerSetup(num_trials)
        big_bracket = sched._state["bracket"]
        for trl in big_bracket.current_trials():
            mock_runner._launch_trial(trl)

        # # Provides result in reverse order, killing the last one
        cur_units = stats[str(1)]["r"]
        for i, trl in reversed(list(enumerate(big_bracket.current_trials()))):
            action = sched.on_trial_result(
                mock_runner, trl, result(cur_units, i))
            self.process(trl, mock_runner, action)

        self.assertEqual(action, TrialScheduler.STOP)

    def testContinueLastOne(self):
        stats = self.default_statistics()
        num_trials = stats[str(0)]["n"]
        sched, mock_runner = self.schedulerSetup(num_trials)
        big_bracket = sched._state["bracket"]
        for trl in big_bracket.current_trials():
            mock_runner._launch_trial(trl)

        # # Provides result in reverse order, killing the last one
        cur_units = stats[str(0)]["r"]
        for i, trl in enumerate(big_bracket.current_trials()):
            action = sched.on_trial_result(
                mock_runner, trl, result(cur_units, i))
            self.process(trl, mock_runner, action)

        self.assertEqual(action, TrialScheduler.CONTINUE)

        for x in range(100):
            action = sched.on_trial_result(
                mock_runner, trl, result(cur_units + x, 10))
            self.assertEqual(action, TrialScheduler.CONTINUE)

    def testTrialErrored(self):
        """If a trial errored, make sure successive halving still happens"""
        stats = self.default_statistics()
        trial_count = stats[str(0)]["n"] + 3
        sched, mock_runner = self.schedulerSetup(trial_count)
        t1, t2, t3 = sched._state["bracket"].current_trials()
        for t in [t1, t2, t3]:
            mock_runner._launch_trial(t)

        sched.on_trial_error(mock_runner, t3)
        self.assertEqual(
            TrialScheduler.PAUSE,
            sched.on_trial_result(
                mock_runner, t1, result(stats[str(1)]["r"], 10)))
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(
                mock_runner, t2, result(stats[str(1)]["r"], 10)))

    def testTrialErrored2(self):
        """Check successive halving happened even when last trial failed"""
        stats = self.default_statistics()
        trial_count = stats[str(0)]["n"] + stats[str(1)]["n"]
        sched, mock_runner = self.schedulerSetup(trial_count)
        trials = sched._state["bracket"].current_trials()
        for t in trials[:-1]:
            mock_runner._launch_trial(t)
            sched.on_trial_result(
                mock_runner, t, result(stats[str(1)]["r"], 10))

        mock_runner._launch_trial(trials[-1])
        sched.on_trial_error(mock_runner, trials[-1])
        self.assertEqual(len(sched._state["bracket"].current_trials()),
                         self.downscale(stats[str(1)]["n"], sched))

    def testTrialEndedEarly(self):
        """Check successive halving happened even when one trial failed"""
        stats = self.default_statistics()
        trial_count = stats[str(0)]["n"] + 3
        sched, mock_runner = self.schedulerSetup(trial_count)

        t1, t2, t3 = sched._state["bracket"].current_trials()
        for t in [t1, t2, t3]:
            mock_runner._launch_trial(t)

        sched.on_trial_complete(mock_runner, t3, result(1, 12))
        self.assertEqual(
            TrialScheduler.PAUSE,
            sched.on_trial_result(
                mock_runner, t1, result(stats[str(1)]["r"], 10)))
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(
                mock_runner, t2, result(stats[str(1)]["r"], 10)))

    def testTrialEndedEarly2(self):
        """Check successive halving happened even when last trial failed"""
        stats = self.default_statistics()
        trial_count = stats[str(0)]["n"] + stats[str(1)]["n"]
        sched, mock_runner = self.schedulerSetup(trial_count)
        trials = sched._state["bracket"].current_trials()
        for t in trials[:-1]:
            mock_runner._launch_trial(t)
            sched.on_trial_result(
                mock_runner, t, result(stats[str(1)]["r"], 10))

        mock_runner._launch_trial(trials[-1])
        sched.on_trial_complete(mock_runner, trials[-1], result(100, 12))
        self.assertEqual(len(sched._state["bracket"].current_trials()),
                         self.downscale(stats[str(1)]["n"], sched))

    def testAddAfterHalving(self):
        stats = self.default_statistics()
        trial_count = stats[str(0)]["n"] + 1
        sched, mock_runner = self.schedulerSetup(trial_count)
        bracket_trials = sched._state["bracket"].current_trials()
        init_units = stats[str(1)]["r"]

        for t in bracket_trials:
            mock_runner._launch_trial(t)

        for i, t in enumerate(bracket_trials):
            status = sched.on_trial_result(
                mock_runner, t, result(init_units, i))
        self.assertEqual(status, TrialScheduler.CONTINUE)
        t = Trial("t%d" % 100, "__fake")
        sched.on_trial_add(None, t)
        mock_runner._launch_trial(t)
        self.assertEqual(len(sched._state["bracket"].current_trials()), 2)

        # Make sure that newly added trial gets fair computation (not just 1)
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(mock_runner, t, result(init_units, 12)))
        new_units = init_units + int(init_units * sched._eta)
        self.assertEqual(
            TrialScheduler.PAUSE,
            sched.on_trial_result(mock_runner, t, result(new_units, 12)))

    def testAlternateMetrics(self):
        """Checking that alternate metrics will pass."""

        def result2(t, rew):
            return TrainingResult(time_total_s=t, neg_mean_loss=rew)

        sched = HyperBandScheduler(
            time_attr='time_total_s', reward_attr='neg_mean_loss')
        stats = self.default_statistics()

        for i in range(stats["max_trials"]):
            t = Trial("t%d" % i, "__fake")
            sched.on_trial_add(None, t)
        runner = _MockTrialRunner()

        big_bracket = sched._hyperbands[0][-1]

        for trl in big_bracket.current_trials():
            runner._launch_trial(trl)
        current_length = len(big_bracket.current_trials())

        # Provides results from 0 to 8 in order, keeping the last one running
        for i, trl in enumerate(big_bracket.current_trials()):
            status = sched.on_trial_result(runner, trl, result2(1, i))
            if status == TrialScheduler.CONTINUE:
                continue
            elif status == TrialScheduler.PAUSE:
                runner._pause_trial(trl)
            elif status == TrialScheduler.STOP:
                self.assertNotEqual(trl.status, Trial.TERMINATED)
                self.stopTrial(trl, runner)

        new_length = len(big_bracket.current_trials())
        self.assertEqual(status, TrialScheduler.CONTINUE)
        self.assertEqual(new_length, self.downscale(current_length, sched))

    def testJumpingTime(self):
        sched, mock_runner = self.schedulerSetup(81)
        big_bracket = sched._hyperbands[0][-1]

        for trl in big_bracket.current_trials():
            mock_runner._launch_trial(trl)

        # Provides results from 0 to 8 in order, keeping the last one running
        main_trials = big_bracket.current_trials()[:-1]
        jump = big_bracket.current_trials()[-1]
        for i, trl in enumerate(main_trials):
            status = sched.on_trial_result(mock_runner, trl, result(1, i))
            if status == TrialScheduler.CONTINUE:
                continue
            elif status == TrialScheduler.PAUSE:
                mock_runner._pause_trial(trl)
            elif status == TrialScheduler.STOP:
                self.assertNotEqual(trl.status, Trial.TERMINATED)
                self.stopTrial(trl, mock_runner)

        status = sched.on_trial_result(mock_runner, jump, result(4, i))
        self.assertEqual(status, TrialScheduler.PAUSE)

        current_length = len(big_bracket.current_trials())
        self.assertLess(current_length, 27)


if __name__ == "__main__":
    unittest.main(verbosity=2)
