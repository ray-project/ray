from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import ray

from ray.tune.schedulers import (MedianStoppingResult as MedianStoppingRule,
                                 TrialScheduler)

from ray.tune.trial import Trial, Checkpoint
from ray.tune.trial_executor import TrialExecutor

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
        runner = _MockTrialRunner(rule)
        t1 = Trial("PPO")
        t2 = Trial("PPO")
        runner.add_trial(t1)
        runner.add_trial(t2)

        return t1, t2, runner

    def testMedianStoppingConstantPerf(self):

        rule = MedianStoppingRule(
            grace_period=0, min_samples_required=1, eval_interval=1)
        t1, t2, runner = self.basicSetup(rule)
        # t1 mean is 450, max 900, t_max=10
        # t2 mean is 450, max 450, t_max=5
        # median at t==5 is 350, at t==10 is 475
        for i in range(11):
            self.assertEqual(
                rule.on_trial_result(runner, t1, result(i, i * 100)),
                TrialScheduler.CONTINUE)
        for i in range(5):
            self.assertEqual(
                rule.on_trial_result(runner, t2, result(i, 450)),
                TrialScheduler.CONTINUE)

        self.assertEqual(
            rule.on_trial_result(runner, t2, result(5, 450)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(runner, t2, result(10, 450)),
            TrialScheduler.STOP)

    # I don't believe this is relevant any more, as trials
    # can stop prior to completion

    # def testMedianStoppingOnCompleteOnly(self):
    #     rule = MedianStoppingRule(grace_period=0, min_samples_required=1)
    #     t1, t2, runner = self.basicSetup(rule)
    #     self.assertEqual(
    #         rule.on_trial_result(runner, t2, result(100, 0)),
    #         TrialScheduler.CONTINUE)
    #     rule.on_trial_complete(runner, t1, result(10, 1000))
    #     self.assertEqual(
    #         rule.on_trial_result(runner, t2, result(101, 0)),
    #         TrialScheduler.STOP)

    def testMedianStoppingGracePeriod(self):
        rule = MedianStoppingRule(
            grace_period=2.5, min_samples_required=1, eval_interval=1)
        t1, t2, runner = self.basicSetup(rule)
        rule.on_trial_result(runner, t1, result(0, 1000))
        rule.on_trial_result(runner, t1, result(1, 1000))
        rule.on_trial_result(runner, t1, result(2, 1000))
        rule.on_trial_result(runner, t1, result(3, 1000))
        rule.on_trial_result(runner, t2, result(0, 10))
        self.assertEqual(
            rule.on_trial_result(runner, t2, result(1, 10)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(runner, t2, result(2, 10)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(runner, t2, result(3, 10)),
            TrialScheduler.STOP)

    def testMedianStoppingMinSamples(self):
        rule = MedianStoppingRule(
            grace_period=0, min_samples_required=3, eval_interval=1)
        t1, t2, runner = self.basicSetup(rule)
        rule.on_trial_result(runner, t1, result(1, 1000))
        self.assertEqual(
            rule.on_trial_result(runner, t2, result(1, 500)),
            TrialScheduler.CONTINUE)
        t3 = Trial("PPO")
        self.assertEqual(
            rule.on_trial_result(runner, t3, result(1, 10)),
            TrialScheduler.STOP)

    def testMedianStoppingUsesRunningWindow(self):
        rule = MedianStoppingRule(
            grace_period=0,
            min_samples_required=1,
            eval_interval=1,
            running_window_size=1)
        t1, t2, runner = self.basicSetup(rule)
        rule.on_trial_result(runner, t1, result(1, 1000))
        rule.on_trial_result(runner, t2, result(1, 1000))
        rule.on_trial_result(runner, t1, result(2, 1000))
        rule.on_trial_result(runner, t2, result(2, 1000))
        t3 = Trial("PPO")
        self.assertEqual(
            rule.on_trial_result(runner, t3, result(1, 1000)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(runner, t3, result(2, 260)),
            TrialScheduler.STOP)

    def testMedianStoppingSoftStop(self):
        rule = MedianStoppingRule(
            grace_period=0,
            min_samples_required=1,
            hard_stop=False,
            eval_interval=1,
            running_window_size=1)
        t1, t2, runner = self.basicSetup(rule)
        rule.on_trial_result(runner, t1, result(1, 1000))
        rule.on_trial_result(runner, t2, result(1, 1000))
        rule.on_trial_result(runner, t1, result(2, 1000))
        rule.on_trial_result(runner, t2, result(2, 1000))
        t3 = Trial("PPO")
        self.assertEqual(
            rule.on_trial_result(runner, t3, result(1, 1000)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(runner, t3, result(2, 260)),
            TrialScheduler.PAUSE)

    def _test_metrics(self, result_func, metric, mode):
        rule = MedianStoppingRule(
            grace_period=0,
            min_samples_required=1,
            time_attr="training_iteration",
            metric=metric,
            mode=mode,
            eval_interval=1)
        t1, t2, runner = self.basicSetup(rule)
        for i in range(11):
            self.assertEqual(
                rule.on_trial_result(runner, t1, result_func(i, i * 100)),
                TrialScheduler.CONTINUE)
        for i in range(5):
            self.assertEqual(
                rule.on_trial_result(runner, t2, result_func(i, 450)),
                TrialScheduler.CONTINUE)

        self.assertEqual(
            rule.on_trial_result(runner, t2, result_func(5, 450)),
            TrialScheduler.CONTINUE)
        self.assertEqual(
            rule.on_trial_result(runner, t2, result_func(10, 450)),
            TrialScheduler.STOP)

    def testAlternateMetrics(self):
        def result2(t, rew):
            return dict(training_iteration=t, neg_mean_loss=rew)

        self._test_metrics(result2, "neg_mean_loss", "max")

    def testAlternateMetricsMin(self):
        def result2(t, rew):
            return dict(training_iteration=t, mean_loss=-rew)

        self._test_metrics(result2, "mean_loss", "min")


class _MockTrialExecutor(TrialExecutor):
    def start_trial(self, trial, checkpoint_obj=None):
        trial.logger_running = True
        trial.restored_checkpoint = checkpoint_obj.value
        trial.status = Trial.RUNNING

    def stop_trial(self, trial, error=False, error_msg=None, stop_logger=True):
        trial.status = Trial.ERROR if error else Trial.TERMINATED
        if stop_logger:
            trial.logger_running = False

    def restore(self, trial, checkpoint=None):
        pass

    def save(self, trial, type=Checkpoint.DISK):
        return trial.trainable_name

    def reset_trial(self, trial, new_config, new_experiment_tag):
        return False


class _MockTrialRunner():
    def __init__(self, scheduler):
        self._scheduler_alg = scheduler
        self.trials = []
        self.trial_executor = _MockTrialExecutor()

    def process_action(self, trial, action):
        if action == TrialScheduler.CONTINUE:
            pass
        elif action == TrialScheduler.PAUSE:
            self._pause_trial(trial)
        elif action == TrialScheduler.STOP:
            self.trial_executor.stop_trial(trial)

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
