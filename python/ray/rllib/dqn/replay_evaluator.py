from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer


class DQNReplayEvaluator(DQNEvaluator):
    """Wraps DQNEvaluators to provide replay buffer functionality.

    This has two modes:
        If config["num_workers"] == 1:
            Samples will be collected locally.
        If config["num_workers"] > 1:
            Samples will be collected from a number of remote workers.
    """

    def __init__(self, env_creator, config, logdir):
        DQNEvaluator.__init__(self, env_creator, config, logdir)

        # Create extra workers if needed
        if self.config["num_workers"] > 1:
            self.workers = [
                DQNEvaluator.remote(env_creator, config, logdir)
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

    def sample(self):
        # First seed the replay buffer with a few new samples
        if self.workers:
            weights = ray.put(self.get_weights())
            for w in self.workers:
                w.set_weights.remote(weights)
            samples = ray.get([w.sample.remote() for w in self.workers])
        else:
            samples = [DQNEvaluator.sample(self)]

        for s in samples:
            for obs, action, rew, new_obs, done in s:
                self.replay_buffer.add(obs, action, rew, new_obs, done)

        # Then return a batch sampled from the buffer
        if self.config["prioritized_replay"]:
            experience = self.replay_buffer.sample(
                self.config["train_batch_size"],
                beta=self.beta_schedule.value(self.cur_timestep))
            (obses_t, actions, rewards, obses_tp1,
                dones, _, batch_idxes) = experience
            self._update_priorities_if_needed()
            self.samples_to_prioritize = (
                obses_t, actions, rewards, obses_tp1, dones, batch_idxes)
        else:
            obses_t, actions, rewards, obses_tp1, dones = \
                self.replay_buffer.sample(self.config["train_batch_size"])
            batch_idxes = None

        return self.samples_to_prioritize

    def compute_gradients(self, samples):
        obses_t, actions, rewards, obses_tp1, dones, batch_indxes = samples
        td_errors, grad = self.dqn_graph.compute_gradients(
            self.sess, obses_t, actions, rewards, obses_tp1, dones,
            np.ones_like(rewards))
        if self.config["prioritized_replay"]:
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            self.replay_buffer.update_priorities(batch_idxes, new_priorities)
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

        obses_t, actions, rewards, obses_tp1, dones, batch_idxes = \
            self.samples_to_prioritize
        td_errors = self.dqn_graph.compute_td_error(
            self.sess, obses_t, actions, rewards, obses_tp1, dones,
            np.ones_like(rewards))
        new_priorities = (
            np.abs(td_errors) + self.config["prioritized_replay_eps"])
        self.replay_buffer.update_priorities(batch_idxes, new_priorities)
        self.samples_to_prioritize = None

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
