from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import logging

from ray.rllib.evaluation.metrics import get_learner_stats
from .replay_buffer import CustomReplayBuffer as ReplayBuffer
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.optimizers.sync_replay_optimizer import SyncReplayOptimizer
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch, DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.memory import ray_get_and_free

logger = logging.getLogger(__name__)


class MultiAgentSyncReplayOptimizer(SyncReplayOptimizer, PolicyOptimizer):
    def __init__(self,
                 workers,
                 learning_starts=1000,
                 buffer_size=10000,
                 train_batch_size=32):
        PolicyOptimizer.__init__(self, workers)

        self.replay_starts = learning_starts
        self.train_batch_size = train_batch_size

        # Stats
        self.update_weights_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.learner_stats = {}

        self.replay_buffer = ReplayBuffer(buffer_size)

        if buffer_size < self.replay_starts:
            logger.warning("buffer_size={} < replay_starts={}".format(
                buffer_size, self.replay_starts))

    @override(PolicyOptimizer)
    def step(self):
        with self.update_weights_timer:
            if self.workers.remote_workers():
                weights = ray.put(self.workers.local_worker().get_weights())
                for e in self.workers.remote_workers():
                    e.set_weights.remote(weights)

        with self.sample_timer:
            if self.workers.remote_workers():
                batch = SampleBatch.concat_samples(
                    ray_get_and_free([
                        e.sample.remote()
                        for e in self.workers.remote_workers()
                    ]))
            else:
                batch = self.workers.local_worker().sample()

            # Handle everything as if multiagent
            if isinstance(batch, SampleBatch):
                batch = MultiAgentBatch({DEFAULT_POLICY_ID: batch}, batch.count)

            batch_dict = dict()
            for pid, s in batch.policy_batches.items():
                i = int(pid.split("_")[1])
                batch_dict.update({
                    "obs_%d" % i: s.data["obs"],
                    "act_%d" % i: s.data["actions"],
                    "rew_%d" % i: s.data["rewards"],
                    "new_obs_%d" % i: s.data["new_obs"],
                    "done_%d" % i: s.data["dones"],
                })
            self.replay_buffer.add(**batch_dict)

        if self.num_steps_sampled >= self.replay_starts:
            self._optimize()

        self.num_steps_sampled += batch.count

    @override(SyncReplayOptimizer)
    def stats(self):
        return SyncReplayOptimizer.stats(self)

    @override(SyncReplayOptimizer)
    def _optimize(self):
        samples = self._replay()

        with self.grad_timer:
            info_dict = self.workers.local_worker().learn_on_batch(samples)
            for policy_id, info in info_dict.items():
                self.learner_stats[policy_id] = get_learner_stats(info)
            self.grad_timer.push_units_processed(samples.count)

        self.num_steps_trained += samples.count

    @override(SyncReplayOptimizer)
    def _replay(self):
        samples = {}
        with self.replay_timer:
            batch_dict = self.replay_buffer.sample(self.train_batch_size)

            target_act_sampler_n = list()
            new_obs_ph_n = list()

            for policy in self.workers.local_worker().policy_map.values():
                target_act_sampler_n.append(policy.target_act_sampler)
                new_obs_ph_n.append(policy.new_obs_ph)

            new_obs_n = list()
            for k, v in batch_dict.items():
                if "new_obs" in k:
                    new_obs_n.append(v)

            new_act_n = policy.sess.run(target_act_sampler_n, dict(zip(new_obs_ph_n, new_obs_n)))
            batch_dict.update({"new_act_%d" % i: new_act for i, new_act in enumerate(new_act_n)})

            for policy_id in self.workers.local_worker().policy_map.keys():
                samples[policy_id] = batch_dict
                samples[policy_id] = SampleBatch(samples[policy_id])

        return MultiAgentBatch(samples, self.train_batch_size)
