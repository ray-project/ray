from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import numpy as np

import ray
from ray.rllib.optimizers.replay_buffer import ReplayBuffer, \
    PrioritizedReplayBuffer
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.evaluation.sample_batch import SampleBatchBuilder, \
    SampleBatch, DEFAULT_POLICY_ID, MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.compression import pack_if_needed
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.schedules import LinearSchedule


class OfflineOptimizer(PolicyOptimizer):
    """Standard procedure for learning an agent from historical data

    Read samples from files with required preprocessing, store 
    the samples in replay buffer, and traverse the samples for
    several epochs."""

    @override(PolicyOptimizer)
    def _init(self,
              buffer_size=10000,
              train_batch_size=32,
              sample_batch_size=4,
              gamma=0.99):

        # Configurations
        self.buffer_size = buffer_size
        self.train_batch_size = train_batch_size
        self.sample_batch_size = sample_batch_size
        self.discount_factor = gamma

        # Stats
        self.sample_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.learner_stats = {}

        # Set up replay buffer
        def new_buffer():
            return ReplayBuffer(buffer_size)
        self.replay_buffers = collections.defaultdict(new_buffer)

        # Preprocessing
        self.cur_batch = SampleBatchBuilder()

    @override(PolicyOptimizer)
    def step(self):
        if self.buffer_size <= self.num_steps_sampled:
            self._optimize()
        else:
            with self.sample_timer:
                batch = self.local_evaluator.sample()
                terminal_indices = self._get_terminal_indices(batch)
                if terminal_indices:
                    i, j = 0, 0
                    for row in batch.rows():
                        self.cur_batch.add_values(**row)
                        if j == len(terminal_indices) or i < terminal_indices[j]:
                            i += 1
                            continue

                        i += 1
                        j += 1
                        cur_episode = self.cur_batch.build_and_reset()
                        # Calculate delayed reward
                        self._calc_delayed_reward(cur_episode)
                        # Handle everything as if multiagent
                        if isinstance(cur_episode, SampleBatch):
                            cur_episode = MultiAgentBatch({
                                DEFAULT_POLICY_ID: cur_episode
                            }, cur_episode.count)
                        for policy_id, s in cur_episode.policy_batches.items():
                            for row in s.rows():
                                self.replay_buffers[policy_id].add(
                                    pack_if_needed(row["obs"]),
                                    row["actions"],
                                    row["rewards"],
                                    pack_if_needed(row["new_obs"]),
                                    row["dones"],
                                    weight=None)
                        self.num_steps_sampled += cur_episode.count
                else:
                    self.cur_batch.add_batch(batch)

    def _optimize(self):
        samples = self._replay()

        with self.grad_timer:
            info_dict = self.local_evaluator.compute_apply(samples)
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

    def _get_terminal_indices(self, batch):
        done_mask = batch.columns(["dones"])[0]
        return [i for i, done in enumerate(done_mask) if done]

    def _calc_delayed_reward(self, batch):
        reward = batch.columns(["rewards"])[0]
        for i in range(1, len(reward)):
            reward[len(reward)-i-1] += self.discount_factor + reward[len(reward)-i]
