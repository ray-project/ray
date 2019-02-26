from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import numpy as np

import ray
from ray.rllib.optimizers.replay_buffer import ReplayBuffer, \
    PrioritizedReplayBuffer
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.evaluation.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.compression import pack_if_needed
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.schedules import LinearSchedule


class SyncReplayOptimizer(PolicyOptimizer):
    """Variant of the local sync optimizer that supports replay (for DQN).

    This optimizer requires that policy evaluators return an additional
    "td_error" array in the info return of compute_gradients(). This error
    term will be used for sample prioritization."""

    @override(PolicyOptimizer)
    def _init(self,
              learning_starts=1000,
              buffer_size=10000,
              prioritized_replay=True,
              prioritized_replay_alpha=0.6,
              prioritized_replay_beta=0.4,
              schedule_max_timesteps=100000,
              beta_annealing_fraction=0.2,
              final_prioritized_replay_beta=0.4,
              prioritized_replay_eps=1e-6,
              train_batch_size=32,
              sample_batch_size=4):

        self.replay_starts = learning_starts
        # linearly annealing beta used in Rainbow paper
        self.prioritized_replay_beta = LinearSchedule(
            schedule_timesteps=int(
                schedule_max_timesteps * beta_annealing_fraction),
            initial_p=prioritized_replay_beta,
            final_p=final_prioritized_replay_beta)
        self.prioritized_replay_eps = prioritized_replay_eps
        self.train_batch_size = train_batch_size

        # Stats
        self.update_weights_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.learner_stats = {}

        # Set up replay buffer
        if prioritized_replay:

            def new_buffer():
                return PrioritizedReplayBuffer(
                    buffer_size, alpha=prioritized_replay_alpha)
        else:

            def new_buffer():
                return ReplayBuffer(buffer_size)

        self.replay_buffers = collections.defaultdict(new_buffer)

        assert buffer_size >= self.replay_starts

    @override(PolicyOptimizer)
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

            # Handle everything as if multiagent
            if isinstance(batch, SampleBatch):
                batch = MultiAgentBatch({
                    DEFAULT_POLICY_ID: batch
                }, batch.count)

            for policy_id, s in batch.policy_batches.items():
                for row in s.rows():
                    self.replay_buffers[policy_id].add(
                        pack_if_needed(row["obs"]),
                        row["actions"],
                        row["rewards"],
                        pack_if_needed(row["new_obs"]),
                        row["dones"],
                        weight=None)

        if self.num_steps_sampled >= self.replay_starts:
            self._optimize()

        self.num_steps_sampled += batch.count

    @override(PolicyOptimizer)
    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
                "replay_time_ms": round(1000 * self.replay_timer.mean, 3),
                "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
                "update_time_ms": round(1000 * self.update_weights_timer.mean,
                                        3),
                "opt_peak_throughput": round(self.grad_timer.mean_throughput,
                                             3),
                "opt_samples": round(self.grad_timer.mean_units_processed, 3),
                "learner": self.learner_stats,
            })

    def _optimize(self):
        samples = self._replay()

        with self.grad_timer:
            info_dict = self.local_evaluator.learn_on_batch(samples)
            for policy_id, info in info_dict.items():
                if "stats" in info:
                    self.learner_stats[policy_id] = info["stats"]
                replay_buffer = self.replay_buffers[policy_id]
                if isinstance(replay_buffer, PrioritizedReplayBuffer):
                    td_error = info["td_error"]
                    new_priorities = (
                        np.abs(td_error) + self.prioritized_replay_eps)
                    replay_buffer.update_priorities(
                        samples.policy_batches[policy_id]["batch_indexes"],
                        new_priorities)
            self.grad_timer.push_units_processed(samples.count)

        self.num_steps_trained += samples.count

    def _replay(self):
        samples = {}
        with self.replay_timer:
            for policy_id, replay_buffer in self.replay_buffers.items():
                if isinstance(replay_buffer, PrioritizedReplayBuffer):
                    (obses_t, actions, rewards, obses_tp1, dones, weights,
                     batch_indexes) = replay_buffer.sample(
                         self.train_batch_size,
                         beta=self.prioritized_replay_beta.value(
                             self.num_steps_trained))
                else:
                    (obses_t, actions, rewards, obses_tp1,
                     dones) = replay_buffer.sample(self.train_batch_size)
                    weights = np.ones_like(rewards)
                    batch_indexes = -np.ones_like(rewards)
                samples[policy_id] = SampleBatch({
                    "obs": obses_t,
                    "actions": actions,
                    "rewards": rewards,
                    "new_obs": obses_tp1,
                    "dones": dones,
                    "weights": weights,
                    "batch_indexes": batch_indexes
                })
        return MultiAgentBatch(samples, self.train_batch_size)
