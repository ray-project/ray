from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.rllib.dqn.dqn_evaluator import DQNEvaluator
from ray.rllib.dqn.common.schedules import LinearSchedule
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.optimizers import SampleBatch


class DQNReplayEvaluator(DQNEvaluator):
    """Wraps DQNEvaluators to provide replay buffer functionality.

    This has two modes:
        If config["num_workers"] == 1:
            Samples will be collected locally.
        If config["num_workers"] > 1:
            Samples will be collected from a number of remote workers.
    """

    def __init__(self, registry, env_creator, config, logdir):
        DQNEvaluator.__init__(self, registry, env_creator, config, logdir)

        # Create extra workers if needed
        if self.config["num_workers"] > 1:
            remote_cls = ray.remote(num_cpus=1)(DQNEvaluator)
            self.workers = [
                remote_cls.remote(env_creator, config, logdir)
                for _ in range(self.config["num_workers"])]
        else:
            self.workers = []

        # Create the replay buffer
        if config["prioritized_replay"]:
            self.replay_buffer = PrioritizedReplayBuffer(
                config["buffer_size"],
                alpha=config["prioritized_replay_alpha"])
            prioritized_replay_beta_iters = \
                config["prioritized_replay_beta_iters"]
            if prioritized_replay_beta_iters is None:
                prioritized_replay_beta_iters = \
                    config["schedule_max_timesteps"]
            self.beta_schedule = LinearSchedule(
                prioritized_replay_beta_iters,
                initial_p=config["prioritized_replay_beta0"],
                final_p=1.0)
        else:
            self.replay_buffer = ReplayBuffer(config["buffer_size"])
            self.beta_schedule = None

        self.samples_to_prioritize = None

    def sample(self, no_replay=False):
        # First seed the replay buffer with a few new samples
        if self.workers:
            weights = ray.put(self.get_weights())
            for w in self.workers:
                w.set_weights.remote(weights)
            samples = ray.get([w.sample.remote() for w in self.workers])
        else:
            samples = [DQNEvaluator.sample(self)]

        for s in samples:
            for row in s.rows():
                self.replay_buffer.add(
                    row["obs"], row["actions"], row["rewards"], row["new_obs"],
                    row["dones"])

        if no_replay:
            return SampleBatch.concat_samples(samples)

        # Then return a batch sampled from the buffer
        if self.config["prioritized_replay"]:
            (obses_t, actions, rewards, obses_tp1,
                dones, weights, batch_indexes) = self.replay_buffer.sample(
                    self.config["train_batch_size"],
                    beta=self.beta_schedule.value(self.global_timestep))
            self._update_priorities_if_needed()
            batch = SampleBatch({
                "obs": obses_t, "actions": actions, "rewards": rewards,
                "new_obs": obses_tp1, "dones": dones, "weights": weights,
                "batch_indexes": batch_indexes})
            self.samples_to_prioritize = batch
        else:
            obses_t, actions, rewards, obses_tp1, dones = \
                self.replay_buffer.sample(self.config["train_batch_size"])
            batch = SampleBatch({
                "obs": obses_t, "actions": actions, "rewards": rewards,
                "new_obs": obses_tp1, "dones": dones,
                "weights": np.ones_like(rewards)})
        return batch

    def compute_gradients(self, samples):
        td_errors, grad = self.dqn_graph.compute_gradients(
            self.sess, samples["obs"], samples["actions"], samples["rewards"],
            samples["new_obs"], samples["dones"], samples["weights"])
        if self.config["prioritized_replay"]:
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            self.replay_buffer.update_priorities(
                samples["batch_indexes"], new_priorities)
            self.samples_to_prioritize = None
        return grad

    def _update_priorities_if_needed(self):
        """Manually updates replay buffer priorities on the last batch.

        Note that this is only needed when not computing gradients on this
        Evaluator (e.g. when using local multi-GPU). Otherwise, priorities
        can be updated more efficiently as part of computing gradients.
        """

        if not self.samples_to_prioritize:
            return

        batch = self.samples_to_prioritize
        td_errors = self.dqn_graph.compute_td_error(
            self.sess, batch["obs"], batch["actions"], batch["rewards"],
            batch["new_obs"], batch["dones"], batch["weights"])

        new_priorities = (
            np.abs(td_errors) + self.config["prioritized_replay_eps"])
        self.replay_buffer.update_priorities(
            batch["batch_indexes"], new_priorities)
        self.samples_to_prioritize = None

    def stats(self):
        if self.workers:
            return ray.get([s.stats.remote() for s in self.workers])
        else:
            return DQNEvaluator.stats(self)

    def save(self):
        return [
            DQNEvaluator.save(self),
            ray.get([w.save.remote() for w in self.workers]),
            self.beta_schedule,
            self.replay_buffer]

    def restore(self, data):
        DQNEvaluator.restore(self, data[0])
        for (w, d) in zip(self.workers, data[1]):
            w.restore.remote(d)
        self.beta_schedule = data[2]
        self.replay_buffer = data[3]
