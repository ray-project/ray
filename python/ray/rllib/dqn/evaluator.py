from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf

from ray.rllib.evaluator import Evaluator, TFMultiGpuSupport


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
            self.workers = None

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

    def sample(self):
        # First seed the replay buffer with a few new samples
        if self.workers:
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
        else:
            obses_t, actions, rewards, obses_tp1, dones = \
                self.replay_buffer.sample(self.config["train_batch_size"])
            batch_idxes = None

        return (obses_t, actions, rewards, obses_tp1, dones, batch_idxes)

    def gradients(self, samples):
        obses_t, actions, rewards, obses_tp1, dones, batch_indxes = samples
        td_errors, grad = self.dqn_graph.compute_gradients(
            self.sess, obses_t, actions, rewards, obses_tp1, dones,
            np.ones_like(rewards))
        # TODO(ekl) how can priorization updates happen if gradients are
        # computed on a different evaluator?
        if self.config["prioritized_replay"]:
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            self.replay_buffer.update_priorities(batch_idxes, new_priorities)
        return grad


class DQNEvaluator(Evaluator, TFMultiGpuSupport):
    def __init__(self, env_creator, config, logdir):
        env = env_creator()
        env = wrap_dqn(env, config["model"])
        self.env = env
        self.config = config

        tf_config = tf.ConfigProto(**config["tf_session_args"])
        self.sess = tf.Session(config=tf_config)
        self.dqn_graph = models.DQNGraph(env, config, logdir)

        # Create the schedule for exploration starting from 1.
        self.exploration = LinearSchedule(
            schedule_timesteps=int(
                config["exploration_fraction"] *
                config["schedule_max_timesteps"]),
            initial_p=1.0,
            final_p=config["exploration_final_eps"])

        # Initialize the parameters and copy them to the target network.
        self.sess.run(tf.global_variables_initializer())
        self.dqn_graph.update_target(self.sess)
        self.set_weights_time = RunningStat(())
        self.sample_time = RunningStat(())
        self.grad_time = RunningStat(())
        self.cur_timestep = 0

        # Note that this encompasses both the Q and target network
        self.variables = ray.experimental.TensorFlowVariables(
            tf.group(self.dqn_graph.q_t, self.dqn_graph.q_tp1), self.sess)

        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]
        self.saved_mean_reward = None
        self.obs = self.env.reset()
        self.file_writer = tf.summary.FileWriter(logdir, self.sess.graph)

    def set_cur_timestep(self, cur_timestep):
        self.cur_timestep = cur_timestep

    def sample(self):
        output = []
        for _ in range(self.config["sample_batch_size"]):
            result = self.step(self.cur_timestep)
            output.append(result)
        return output

    def gradients(self, samples):
        obses_t, actions, rewards, obses_tp1, dones = samples
        batch_idxes = None
        _, grad = self.dqn_graph.compute_gradients(
            self.sess, obses_t, actions, rewards, obses_tp1, dones,
            np.ones_like(rewards))
        return grad

    def apply(self, grads):
        self.dqn_graph.apply_gradients(self.sess, grads)

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def tf_loss_inputs(self):
        return self.dqn_graph.loss_inputs

    def build_tf_loss(self, input_placeholders):
        return self.dqn_graph.build_loss(*input_placeholders)

    def _step(self, cur_timestep):
        """Takes a single step, and returns the result of the step."""
        action = self.dqn_graph.act(
            self.sess, np.array(self.obs)[None],
            self.exploration.value(cur_timestep))[0]
        new_obs, rew, done, _ = self.env.step(action)
        ret = (self.obs, action, rew, new_obs, float(done))
        self.obs = new_obs
        self.episode_rewards[-1] += rew
        self.episode_lengths[-1] += 1
        if done:
            self.obs = self.env.reset()
            self.episode_rewards.append(0.0)
            self.episode_lengths.append(0.0)
        return ret

    # TODO(ekl) return a dictionary and use that everywhere to clean up the
    # bookkeeping of stats
    def stats(self):
        mean_100ep_reward = round(np.mean(self.episode_rewards[-101:-1]), 5)
        mean_100ep_length = round(np.mean(self.episode_lengths[-101:-1]), 5)
        exploration = self.exploration.value(self.cur_timestep)
        return (
            mean_100ep_reward,
            mean_100ep_length,
            len(self.episode_rewards),
            exploration,
            float(self.set_weights_time.mean),
            float(self.sample_time.mean),
            float(self.grad_time.mean))

    def save(self):
        return [
            self.exploration,
            self.episode_rewards,
            self.episode_lengths,
            self.saved_mean_reward,
            self.obs]

    def restore(self, data):
        self.exploration = data[0]
        self.episode_rewards = data[1]
        self.episode_lengths = data[2]
        self.saved_mean_reward = data[3]
        self.obs = data[4]
