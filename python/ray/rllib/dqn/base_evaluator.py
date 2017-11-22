from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf

from ray.rllib.evaluator import Evaluator, TFMultiGpuSupport


class DQNEvaluator(Evaluator, TFMultiGpuSupport):
    """The base DQN Evaluator that does not include the replay buffer."""

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

    def compute_gradients(self, samples):
        obses_t, actions, rewards, obses_tp1, dones = samples
        batch_idxes = None
        _, grad = self.dqn_graph.compute_gradients(
            self.sess, obses_t, actions, rewards, obses_tp1, dones,
            np.ones_like(rewards))
        return grad

    def apply_gradients(self, grads):
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
