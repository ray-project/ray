from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import unittest
import numpy as np

import ray
from ray.tune.hyperband import HyperBandScheduler
from ray.tune.async_hyperband import AsyncHyperBandScheduler
from ray.tune.pbt import PopulationBasedTraining, explore
from ray.tune.median_stopping_rule import MedianStoppingRule
from ray.tune.trial import Trial, Resources
from ray.tune.trial_scheduler import TrialScheduler

from ray.rllib import _register_all
_register_all()


def result(t, rew):
    return dict(
        time_total_s=t, episode_reward_mean=rew, training_iteration=int(t))


class EarlyStoppingSuite(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def basicSetup(self, rule):
        t1 = Trial("PPO")  # mean is 450, max 900, t_max=10
        t2 = Trial("PPO")  # mean is 450, max 450, t_max=5
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
        t3 = Trial("PPO")
        self.assertEqual(
            rule.on_trial_result(None, t3, result(1, 10)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t3, result(2, 10)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t3, result(3, 10)), TrialScheduler.STOP)

    def testMedianStoppingMinSamples(self):
        rule = MedianStoppingRule(grace_period=0, min_samples_required=2)
        t1, t2 = self.basicSetup(rule)
        rule.on_trial_complete(None, t1, result(10, 1000))
        t3 = Trial("PPO")
        self.assertEqual(
            rule.on_trial_result(None, t3, result(3, 10)),
            TrialScheduler.CONTINUE)
        rule.on_trial_complete(None, t2, result(10, 1000))
        self.assertEqual(
            rule.on_trial_result(None, t3, result(3, 10)), TrialScheduler.STOP)

    def testMedianStoppingUsesMedian(self):
        rule = MedianStoppingRule(grace_period=0, min_samples_required=1)
        t1, t2 = self.basicSetup(rule)
        rule.on_trial_complete(None, t1, result(10, 1000))
        rule.on_trial_complete(None, t2, result(10, 1000))
        t3 = Trial("PPO")
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
        t3 = Trial("PPO")
        self.assertEqual(
            rule.on_trial_result(None, t3, result(1, 260)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(None, t3, result(2, 260)),
            TrialScheduler.PAUSE)

    def testAlternateMetrics(self):
        def result2(t, rew):
            return dict(training_iteration=t, neg_mean_loss=rew)

        rule = MedianStoppingRule(
            grace_period=0,
            min_samples_required=1,
            time_attr='training_iteration',
            reward_attr='neg_mean_loss')
        t1 = Trial("PPO")  # mean is 450, max 900, t_max=10
        t2 = Trial("PPO")  # mean is 450, max 450, t_max=5
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
    def __init__(self, scheduler):
        self._scheduler_alg = scheduler
        self.trials = []

    def process_action(self, trial, action):
        if action == TrialScheduler.CONTINUE:
            pass
        elif action == TrialScheduler.PAUSE:
            self._pause_trial(trial)
        elif action == TrialScheduler.STOP:
            trial.stop()

    def stop_trial(self, trial):
        if trial.status in [Trial.ERROR, Trial.TERMINATED]:
            return
        elif trial.status in [Trial.PENDING, Trial.PAUSED]:
            self._scheduler_alg.on_trial_remove(self, trial)
        else:

            self._scheduler_alg.on_trial_complete(self, trial, result(100, 10))

    def add_trial(self, trial):
        self.trials.append(trial)
        self._scheduler_alg.on_trial_add(self, trial)

    def get_trials(self):
        return self.trials

    def has_resources(self, resources):
        return True

    def _pause_trial(self, trial):
        trial.status = Trial.PAUSED

    def _launch_trial(self, trial):
        trial.status = Trial.RUNNING


class HyperbandSuite(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def schedulerSetup(self, num_trials):
        """Setup a scheduler and Runner with max Iter = 9

        Bracketing is placed as follows:
        (5, 81);
        (8, 27) -> (3, 54);
        (15, 9) -> (5, 27) -> (2, 45);
        (34, 3) -> (12, 9) -> (4, 27) -> (2, 42);
        (81, 1) -> (27, 3) -> (9, 9) -> (3, 27) -> (1, 41);"""
        sched = HyperBandScheduler()
        for i in range(num_trials):
            t = Trial("__fake")
            sched.on_trial_add(None, t)
        runner = _MockTrialRunner(sched)
        return sched, runner

    def default_statistics(self):
        """Default statistics for HyperBand"""
        sched = HyperBandScheduler()
        res = {
            str(s): {
                "n": sched._get_n0(s),
                "r": sched._get_r0(s)
            }
            for s in range(sched._s_max_1)
        }
        res["max_trials"] = sum(v["n"] for v in res.values())
        res["brack_count"] = sched._s_max_1
        res["s_max"] = sched._s_max_1 - 1
        return res

    def downscale(self, n, sched):
        return int(np.ceil(n / sched._eta))

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
            t = Trial("__fake")
            sched.on_trial_add(None, t)

        self.assertEqual(sched._cur_band_filled(), False)

        unfilled_band = sched._hyperbands[-1]
        self.assertEqual(len(unfilled_band), 2)
        bracket = unfilled_band[-1]
        self.assertEqual(bracket.filled(), False)
        self.assertEqual(len(bracket.current_trials()), 7)

        return sched

    def testConfigSameEta(self):
        sched = HyperBandScheduler()
        i = 0
        while not sched._cur_band_filled():
            t = Trial("__fake")
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
            t = Trial("__fake")
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
            t = Trial("__fake")
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
                action = sched.on_trial_result(mock_runner, trl,
                                               result(cur_units, i))
                if i < current_length - 1:
                    self.assertEqual(action, TrialScheduler.PAUSE)
                mock_runner.process_action(trl, action)

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
            action = sched.on_trial_result(mock_runner, trl,
                                           result(cur_units, i))
            mock_runner.process_action(trl, action)

        self.assertEqual(action, TrialScheduler.STOP)

    def testStopsLastOne(self):
        stats = self.default_statistics()
        num_trials = stats[str(0)]["n"]  # setup one bracket
        sched, mock_runner = self.schedulerSetup(num_trials)
        big_bracket = sched._state["bracket"]
        for trl in big_bracket.current_trials():
            mock_runner._launch_trial(trl)

        # # Provides result in reverse order, killing the last one
        cur_units = stats[str(0)]["r"]
        for i, trl in enumerate(big_bracket.current_trials()):
            action = sched.on_trial_result(mock_runner, trl,
                                           result(cur_units, i))
            mock_runner.process_action(trl, action)

        self.assertEqual(action, TrialScheduler.STOP)

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
            sched.on_trial_result(mock_runner, t1,
                                  result(stats[str(1)]["r"], 10)))
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(mock_runner, t2,
                                  result(stats[str(1)]["r"], 10)))

    def testTrialErrored2(self):
        """Check successive halving happened even when last trial failed"""
        stats = self.default_statistics()
        trial_count = stats[str(0)]["n"] + stats[str(1)]["n"]
        sched, mock_runner = self.schedulerSetup(trial_count)
        trials = sched._state["bracket"].current_trials()
        for t in trials[:-1]:
            mock_runner._launch_trial(t)
            sched.on_trial_result(mock_runner, t, result(
                stats[str(1)]["r"], 10))

        mock_runner._launch_trial(trials[-1])
        sched.on_trial_error(mock_runner, trials[-1])
        self.assertEqual(
            len(sched._state["bracket"].current_trials()),
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
            sched.on_trial_result(mock_runner, t1,
                                  result(stats[str(1)]["r"], 10)))
        self.assertEqual(
            TrialScheduler.CONTINUE,
            sched.on_trial_result(mock_runner, t2,
                                  result(stats[str(1)]["r"], 10)))

    def testTrialEndedEarly2(self):
        """Check successive halving happened even when last trial failed"""
        stats = self.default_statistics()
        trial_count = stats[str(0)]["n"] + stats[str(1)]["n"]
        sched, mock_runner = self.schedulerSetup(trial_count)
        trials = sched._state["bracket"].current_trials()
        for t in trials[:-1]:
            mock_runner._launch_trial(t)
            sched.on_trial_result(mock_runner, t, result(
                stats[str(1)]["r"], 10))

        mock_runner._launch_trial(trials[-1])
        sched.on_trial_complete(mock_runner, trials[-1], result(100, 12))
        self.assertEqual(
            len(sched._state["bracket"].current_trials()),
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
            action = sched.on_trial_result(mock_runner, t, result(
                init_units, i))
        self.assertEqual(action, TrialScheduler.CONTINUE)
        t = Trial("__fake")
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
            return dict(time_total_s=t, neg_mean_loss=rew)

        sched = HyperBandScheduler(
            time_attr='time_total_s', reward_attr='neg_mean_loss')
        stats = self.default_statistics()

        for i in range(stats["max_trials"]):
            t = Trial("__fake")
            sched.on_trial_add(None, t)
        runner = _MockTrialRunner(sched)

        big_bracket = sched._hyperbands[0][-1]

        for trl in big_bracket.current_trials():
            runner._launch_trial(trl)
        current_length = len(big_bracket.current_trials())

        # Provides results from 0 to 8 in order, keeping the last one running
        for i, trl in enumerate(big_bracket.current_trials()):
            action = sched.on_trial_result(runner, trl, result2(1, i))
            runner.process_action(trl, action)

        new_length = len(big_bracket.current_trials())
        self.assertEqual(action, TrialScheduler.CONTINUE)
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
            action = sched.on_trial_result(mock_runner, trl, result(1, i))
            mock_runner.process_action(trl, action)

        action = sched.on_trial_result(mock_runner, jump, result(4, i))
        self.assertEqual(action, TrialScheduler.PAUSE)

        current_length = len(big_bracket.current_trials())
        self.assertLess(current_length, 27)

    def testRemove(self):
        """Test with 4: start 1, remove 1 pending, add 2, remove 1 pending"""
        sched, runner = self.schedulerSetup(4)
        trials = sorted(list(sched._trial_info), key=lambda t: t.trial_id)
        runner._launch_trial(trials[0])
        sched.on_trial_result(runner, trials[0], result(1, 5))
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        bracket, _ = sched._trial_info[trials[1]]
        self.assertTrue(trials[1] in bracket._live_trials)
        sched.on_trial_remove(runner, trials[1])
        self.assertFalse(trials[1] in bracket._live_trials)

        for i in range(2):
            trial = Trial("__fake")
            sched.on_trial_add(None, trial)

        bracket, _ = sched._trial_info[trial]
        self.assertTrue(trial in bracket._live_trials)
        sched.on_trial_remove(runner, trial)  # where trial is not running
        self.assertFalse(trial in bracket._live_trials)


class _MockTrial(Trial):
    def __init__(self, i, config):
        self.trainable_name = "trial_{}".format(i)
        self.config = config
        self.experiment_tag = "tag"
        self.logger_running = False
        self.restored_checkpoint = None
        self.resources = Resources(1, 0)

    def checkpoint(self, to_object_store=False):
        return self.trainable_name

    def start(self, checkpoint=None):
        self.logger_running = True
        self.restored_checkpoint = checkpoint

    def stop(self, stop_logger=False):
        if stop_logger:
            self.logger_running = False


class PopulationBasedTestingSuite(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def basicSetup(self, resample_prob=0.0, explore=None):
        pbt = PopulationBasedTraining(
            time_attr="training_iteration",
            perturbation_interval=10,
            resample_probability=resample_prob,
            hyperparam_mutations={
                "id_factor": [100],
                "float_factor": lambda: 100.0,
                "int_factor": lambda: 10,
            },
            custom_explore_fn=explore)
        runner = _MockTrialRunner(pbt)
        for i in range(5):
            trial = _MockTrial(
                i, {
                    "id_factor": i,
                    "float_factor": 2.0,
                    "const_factor": 3,
                    "int_factor": 10
                })
            runner.add_trial(trial)
            trial.status = Trial.RUNNING
            self.assertEqual(
                pbt.on_trial_result(runner, trial, result(10, 50 * i)),
                TrialScheduler.CONTINUE)
        pbt.reset_stats()
        return pbt, runner

    def testCheckpointsMostPromisingTrials(self):
        pbt, runner = self.basicSetup()
        trials = runner.get_trials()

        # no checkpoint: haven't hit next perturbation interval yet
        self.assertEqual(pbt.last_scores(trials), [0, 50, 100, 150, 200])
        self.assertEqual(
            pbt.on_trial_result(runner, trials[0], result(15, 200)),
            TrialScheduler.CONTINUE)
        self.assertEqual(pbt.last_scores(trials), [0, 50, 100, 150, 200])
        self.assertEqual(pbt._num_checkpoints, 0)

        # checkpoint: both past interval and upper quantile
        self.assertEqual(
            pbt.on_trial_result(runner, trials[0], result(20, 200)),
            TrialScheduler.CONTINUE)
        self.assertEqual(pbt.last_scores(trials), [200, 50, 100, 150, 200])
        self.assertEqual(pbt._num_checkpoints, 1)
        self.assertEqual(
            pbt.on_trial_result(runner, trials[1], result(30, 201)),
            TrialScheduler.CONTINUE)
        self.assertEqual(pbt.last_scores(trials), [200, 201, 100, 150, 200])
        self.assertEqual(pbt._num_checkpoints, 2)

        # not upper quantile any more
        self.assertEqual(
            pbt.on_trial_result(runner, trials[4], result(30, 199)),
            TrialScheduler.CONTINUE)
        self.assertEqual(pbt._num_checkpoints, 2)
        self.assertEqual(pbt._num_perturbations, 0)

    def testPerturbsLowPerformingTrials(self):
        pbt, runner = self.basicSetup()
        trials = runner.get_trials()

        # no perturbation: haven't hit next perturbation interval
        self.assertEqual(
            pbt.on_trial_result(runner, trials[0], result(15, -100)),
            TrialScheduler.CONTINUE)
        self.assertEqual(pbt.last_scores(trials), [0, 50, 100, 150, 200])
        self.assertTrue("@perturbed" not in trials[0].experiment_tag)
        self.assertEqual(pbt._num_perturbations, 0)

        # perturb since it's lower quantile
        self.assertEqual(
            pbt.on_trial_result(runner, trials[0], result(20, -100)),
            TrialScheduler.CONTINUE)
        self.assertEqual(pbt.last_scores(trials), [-100, 50, 100, 150, 200])
        self.assertTrue("@perturbed" in trials[0].experiment_tag)
        self.assertIn(trials[0].restored_checkpoint, ["trial_3", "trial_4"])
        self.assertEqual(pbt._num_perturbations, 1)

        # also perturbed
        self.assertEqual(
            pbt.on_trial_result(runner, trials[2], result(20, 40)),
            TrialScheduler.CONTINUE)
        self.assertEqual(pbt.last_scores(trials), [-100, 50, 40, 150, 200])
        self.assertEqual(pbt._num_perturbations, 2)
        self.assertIn(trials[0].restored_checkpoint, ["trial_3", "trial_4"])
        self.assertTrue("@perturbed" in trials[2].experiment_tag)

    def testPerturbWithoutResample(self):
        pbt, runner = self.basicSetup(resample_prob=0.0)
        trials = runner.get_trials()
        self.assertEqual(
            pbt.on_trial_result(runner, trials[0], result(20, -100)),
            TrialScheduler.CONTINUE)
        self.assertIn(trials[0].restored_checkpoint, ["trial_3", "trial_4"])
        self.assertIn(trials[0].config["id_factor"], [100])
        self.assertIn(trials[0].config["float_factor"], [2.4, 1.6])
        self.assertEqual(type(trials[0].config["float_factor"]), float)
        self.assertIn(trials[0].config["int_factor"], [8, 12])
        self.assertEqual(type(trials[0].config["int_factor"]), int)
        self.assertEqual(trials[0].config["const_factor"], 3)

    def testPerturbWithResample(self):
        pbt, runner = self.basicSetup(resample_prob=1.0)
        trials = runner.get_trials()
        self.assertEqual(
            pbt.on_trial_result(runner, trials[0], result(20, -100)),
            TrialScheduler.CONTINUE)
        self.assertIn(trials[0].restored_checkpoint, ["trial_3", "trial_4"])
        self.assertEqual(trials[0].config["id_factor"], 100)
        self.assertEqual(trials[0].config["float_factor"], 100.0)
        self.assertEqual(type(trials[0].config["float_factor"]), float)
        self.assertEqual(trials[0].config["int_factor"], 10)
        self.assertEqual(type(trials[0].config["int_factor"]), int)
        self.assertEqual(trials[0].config["const_factor"], 3)

    def testPerturbationValues(self):
        def assertProduces(fn, values):
            random.seed(0)
            seen = set()
            for _ in range(100):
                seen.add(fn()["v"])
            self.assertEqual(seen, values)

        # Categorical case
        assertProduces(
            lambda: explore({"v": 4}, {"v": [3, 4, 8, 10]}, 0.0, lambda x: x),
            {3, 8})
        assertProduces(
            lambda: explore({"v": 3}, {"v": [3, 4, 8, 10]}, 0.0, lambda x: x),
            {3, 4})
        assertProduces(
            lambda: explore({"v": 10}, {"v": [3, 4, 8, 10]}, 0.0, lambda x: x),
            {8, 10})
        assertProduces(
            lambda: explore({"v": 7}, {"v": [3, 4, 8, 10]}, 0.0, lambda x: x),
            {3, 4, 8, 10})
        assertProduces(
            lambda: explore({"v": 4}, {"v": [3, 4, 8, 10]}, 1.0, lambda x: x),
            {3, 4, 8, 10})

        # Continuous case
        assertProduces(
            lambda: explore(
                {"v": 100}, {"v": lambda: random.choice([10, 100])}, 0.0,
                lambda x: x),
            {80, 120})
        assertProduces(
            lambda: explore(
                {"v": 100.0}, {"v": lambda: random.choice([10, 100])}, 0.0,
                lambda x: x),
            {80.0, 120.0})
        assertProduces(
            lambda: explore(
                {"v": 100.0}, {"v": lambda: random.choice([10, 100])}, 1.0,
                lambda x: x),
            {10.0, 100.0})

    def testYieldsTimeToOtherTrials(self):
        pbt, runner = self.basicSetup()
        trials = runner.get_trials()
        trials[0].status = Trial.PENDING  # simulate not enough resources

        self.assertEqual(
            pbt.on_trial_result(runner, trials[1], result(20, 1000)),
            TrialScheduler.PAUSE)
        self.assertEqual(pbt.last_scores(trials), [0, 1000, 100, 150, 200])
        self.assertEqual(pbt.choose_trial_to_run(runner), trials[0])

    def testSchedulesMostBehindTrialToRun(self):
        pbt, runner = self.basicSetup()
        trials = runner.get_trials()
        pbt.on_trial_result(runner, trials[0], result(800, 1000))
        pbt.on_trial_result(runner, trials[1], result(700, 1001))
        pbt.on_trial_result(runner, trials[2], result(600, 1002))
        pbt.on_trial_result(runner, trials[3], result(500, 1003))
        pbt.on_trial_result(runner, trials[4], result(700, 1004))
        self.assertEqual(pbt.choose_trial_to_run(runner), None)
        for i in range(5):
            trials[i].status = Trial.PENDING
        self.assertEqual(pbt.choose_trial_to_run(runner), trials[3])

    def testPerturbationResetsLastPerturbTime(self):
        pbt, runner = self.basicSetup()
        trials = runner.get_trials()
        pbt.on_trial_result(runner, trials[0], result(10000, 1005))
        pbt.on_trial_result(runner, trials[1], result(10000, 1004))
        pbt.on_trial_result(runner, trials[2], result(600, 1003))
        self.assertEqual(pbt._num_perturbations, 0)
        pbt.on_trial_result(runner, trials[3], result(500, 1002))
        self.assertEqual(pbt._num_perturbations, 1)
        pbt.on_trial_result(runner, trials[3], result(600, 100))
        self.assertEqual(pbt._num_perturbations, 1)
        pbt.on_trial_result(runner, trials[3], result(11000, 100))
        self.assertEqual(pbt._num_perturbations, 2)

    def testPostprocessingHook(self):
        def explore(new_config):
            new_config["id_factor"] = 42
            new_config["float_factor"] = 43
            return new_config

        pbt, runner = self.basicSetup(resample_prob=0.0, explore=explore)
        trials = runner.get_trials()
        self.assertEqual(
            pbt.on_trial_result(runner, trials[0], result(20, -100)),
            TrialScheduler.CONTINUE)
        self.assertEqual(trials[0].config["id_factor"], 42)
        self.assertEqual(trials[0].config["float_factor"], 43)


class AsyncHyperBandSuite(unittest.TestCase):
    def setUp(self):
        ray.init()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def basicSetup(self, scheduler):
        t1 = Trial("PPO")  # mean is 450, max 900, t_max=10
        t2 = Trial("PPO")  # mean is 450, max 450, t_max=5
        scheduler.on_trial_add(None, t1)
        scheduler.on_trial_add(None, t2)
        for i in range(10):
            self.assertEqual(
                scheduler.on_trial_result(None, t1, result(i, i * 100)),
                TrialScheduler.CONTINUE)
        for i in range(5):
            self.assertEqual(
                scheduler.on_trial_result(None, t2, result(i, 450)),
                TrialScheduler.CONTINUE)
        return t1, t2

    def testAsyncHBOnComplete(self):
        scheduler = AsyncHyperBandScheduler(max_t=10, brackets=1)
        t1, t2 = self.basicSetup(scheduler)
        t3 = Trial("PPO")
        scheduler.on_trial_add(None, t3)
        scheduler.on_trial_complete(None, t3, result(10, 1000))
        self.assertEqual(
            scheduler.on_trial_result(None, t2, result(101, 0)),
            TrialScheduler.STOP)

    def testAsyncHBGracePeriod(self):
        scheduler = AsyncHyperBandScheduler(
            grace_period=2.5, reduction_factor=3, brackets=1)
        t1, t2 = self.basicSetup(scheduler)
        scheduler.on_trial_complete(None, t1, result(10, 1000))
        scheduler.on_trial_complete(None, t2, result(10, 1000))
        t3 = Trial("PPO")
        scheduler.on_trial_add(None, t3)
        self.assertEqual(
            scheduler.on_trial_result(None, t3, result(1, 10)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            scheduler.on_trial_result(None, t3, result(2, 10)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            scheduler.on_trial_result(None, t3, result(3, 10)),
            TrialScheduler.STOP)

    def testAsyncHBAllCompletes(self):
        scheduler = AsyncHyperBandScheduler(max_t=10, brackets=10)
        trials = [Trial("PPO") for i in range(10)]
        for t in trials:
            scheduler.on_trial_add(None, t)

        for t in trials:
            self.assertEqual(
                scheduler.on_trial_result(None, t, result(10, -2)),
                TrialScheduler.STOP)

    def testAsyncHBUsesPercentile(self):
        scheduler = AsyncHyperBandScheduler(
            grace_period=1, max_t=10, reduction_factor=2, brackets=1)
        t1, t2 = self.basicSetup(scheduler)
        scheduler.on_trial_complete(None, t1, result(10, 1000))
        scheduler.on_trial_complete(None, t2, result(10, 1000))
        t3 = Trial("PPO")
        scheduler.on_trial_add(None, t3)
        self.assertEqual(
            scheduler.on_trial_result(None, t3, result(1, 260)),
            TrialScheduler.STOP)
        self.assertEqual(
            scheduler.on_trial_result(None, t3, result(2, 260)),
            TrialScheduler.STOP)

    def testAlternateMetrics(self):
        def result2(t, rew):
            return dict(training_iteration=t, neg_mean_loss=rew)

        scheduler = AsyncHyperBandScheduler(
            grace_period=1,
            time_attr='training_iteration',
            reward_attr='neg_mean_loss',
            brackets=1)
        t1 = Trial("PPO")  # mean is 450, max 900, t_max=10
        t2 = Trial("PPO")  # mean is 450, max 450, t_max=5
        scheduler.on_trial_add(None, t1)
        scheduler.on_trial_add(None, t2)
        for i in range(10):
            self.assertEqual(
                scheduler.on_trial_result(None, t1, result2(i, i * 100)),
                TrialScheduler.CONTINUE)
        for i in range(5):
            self.assertEqual(
                scheduler.on_trial_result(None, t2, result2(i, 450)),
                TrialScheduler.CONTINUE)
        scheduler.on_trial_complete(None, t1, result2(10, 1000))
        self.assertEqual(
            scheduler.on_trial_result(None, t2, result2(5, 450)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            scheduler.on_trial_result(None, t2, result2(6, 0)),
            TrialScheduler.CONTINUE)


if __name__ == "__main__":
    unittest.main(verbosity=2)
