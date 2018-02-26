from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat


class LocalSyncReplayOptimizer(Optimizer):
    """Variation of the local sync optimizer that supports replay (for DQN).

    Note that this assumes there are no remote evaluators."""

    def _init(self):
        self.sample_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.throughput = RunningStat()

        # Set up replay buffer
        self.replay_starts = self.config["learning_starts"]
        self.buffer_size = self.config["buffer_size"]
        if self.config["prioritized_replay"]:
            self.replay_buffer = PrioritizedReplayBuffer(
                self.buffer_size,
                alpha=self.config["prioritized_replay_alpha"])
        else:
            self.replay_buffer = ReplayBuffer(self.buffer_size)

        assert not self.remote_evaluators
        assert self.buffer_size > self.replay_starts

    def step(self):
        with self.sample_timer:
            batch = self.local_evaluator.sample()
            for row in batch.rows():
                self.replay_buffer.add(
                    row["obs"], row["actions"], row["rewards"], row["new_obs"],
                    row["dones"], row["weights"])

        if len(self.replay_buffer) >= self.replay_starts:
            self._optimize()

        return batch.count

    def _optimize(self):
        with self.replay_timer:
            if self.config["prioritized_replay"]:
                (obses_t, actions, rewards, obses_tp1,
                    dones, weights, batch_indexes) = self.replay_buffer.sample(
                        self.config["train_batch_size"],
                        beta=self.config["prioritized_replay_beta"])
            else:
                (obses_t, actions, rewards, obses_tp1,
                    dones) = self.replay_buffer.sample(
                        self.config["train_batch_size"])
                weights = np.ones_like(rewards)
                batch_indexes = - np.ones_like(rewards)

            samples = SampleBatch({
                "obs": obses_t, "actions": actions, "rewards": rewards,
                "new_obs": obses_tp1, "dones": dones, "weights": weights,
                "batch_indexes": batch_indexes})

        with self.grad_timer:
            td_error = self.local_evaluator.compute_apply(samples)
            if self.config["prioritized_replay"]:
                self.replay_buffer.update_priorities(
                    samples["batch_indexes"], td_error)
            self.grad_timer.push_units_processed(samples.count)

    def stats(self):
        return {
            "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
            "replay_time_ms": round(1000 * self.replay_timer.mean, 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "opt_peak_throughput": round(self.grad_timer.mean_throughput, 3),
            "opt_samples": round(self.grad_timer.mean_units_processed, 3),
        }
