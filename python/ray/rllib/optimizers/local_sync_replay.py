from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.rllib.optimizers.replay_buffer import ReplayBuffer, \
    PrioritizedReplayBuffer
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat


class LocalSyncReplayOptimizer(PolicyOptimizer):
    """Variant of the local sync optimizer that supports replay (for DQN).

    This optimizer requires that policy evaluators return an additional
    "td_error" array in the info return of compute_gradients(). This error
    term will be used for sample prioritization."""

    def _init(
            self, learning_starts=1000, buffer_size=10000,
            prioritized_replay=True, prioritized_replay_alpha=0.6,
            prioritized_replay_beta=0.4, prioritized_replay_eps=1e-6,
            train_batch_size=32, sample_batch_size=4, clip_rewards=True):

        self.replay_starts = learning_starts
        self.prioritized_replay_beta = prioritized_replay_beta
        self.prioritized_replay_eps = prioritized_replay_eps
        self.train_batch_size = train_batch_size

        # Stats
        self.update_weights_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.throughput = RunningStat()

        # Set up replay buffer
        if prioritized_replay:
            self.replay_buffer = PrioritizedReplayBuffer(
                buffer_size, alpha=prioritized_replay_alpha,
                clip_rewards=clip_rewards)
        else:
            self.replay_buffer = ReplayBuffer(buffer_size, clip_rewards)

        assert buffer_size >= self.replay_starts

    def step(self):
        with self.update_weights_timer:
            if self.remote_evaluators:
                weights = ray.put(self.local_evaluator.get_weights())
                for e in self.remote_evaluators:
                    e.set_weights.remote(weights)

        with self.sample_timer:
            if self.remote_evaluators:
                batch = SampleBatch.concat_samples(
                    ray.get(
                        [e.sample.remote() for e in self.remote_evaluators]))
            else:
                batch = self.local_evaluator.sample()
            for row in batch.rows():
                self.replay_buffer.add(
                    row["obs"], row["actions"], row["rewards"], row["new_obs"],
                    row["dones"], row["weights"])

        if len(self.replay_buffer) >= self.replay_starts:
            self._optimize()

        self.num_steps_sampled += batch.count

    def _optimize(self):
        with self.replay_timer:
            if isinstance(self.replay_buffer, PrioritizedReplayBuffer):
                (obses_t, actions, rewards, obses_tp1,
                    dones, weights, batch_indexes) = self.replay_buffer.sample(
                        self.train_batch_size,
                        beta=self.prioritized_replay_beta)
            else:
                (obses_t, actions, rewards, obses_tp1,
                    dones) = self.replay_buffer.sample(
                        self.train_batch_size)
                weights = np.ones_like(rewards)
                batch_indexes = - np.ones_like(rewards)

            samples = SampleBatch({
                "obs": obses_t, "actions": actions, "rewards": rewards,
                "new_obs": obses_tp1, "dones": dones, "weights": weights,
                "batch_indexes": batch_indexes})

        with self.grad_timer:
            td_error = self.local_evaluator.compute_apply(samples)["td_error"]
            new_priorities = (
                np.abs(td_error) + self.prioritized_replay_eps)
            if isinstance(self.replay_buffer, PrioritizedReplayBuffer):
                self.replay_buffer.update_priorities(
                    samples["batch_indexes"], new_priorities)
            self.grad_timer.push_units_processed(samples.count)

        self.num_steps_trained += samples.count

    def stats(self):
        return dict(PolicyOptimizer.stats(self), **{
            "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
            "replay_time_ms": round(1000 * self.replay_timer.mean, 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "update_time_ms": round(1000 * self.update_weights_timer.mean, 3),
            "opt_peak_throughput": round(self.grad_timer.mean_throughput, 3),
            "opt_samples": round(self.grad_timer.mean_units_processed, 3),
        })
