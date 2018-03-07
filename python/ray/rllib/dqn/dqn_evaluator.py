from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete
import numpy as np
import tensorflow as tf

import ray
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.dqn import models
from ray.rllib.dqn.common.wrappers import wrap_dqn
from ray.rllib.dqn.common.schedules import ConstantSchedule, LinearSchedule
from ray.rllib.optimizers import SampleBatch, Evaluator
from ray.rllib.utils.compression import pack


def adjust_nstep(n_step, gamma, obs, actions, rewards, new_obs, dones):
    """Rewrites the given trajectory fragments to encode n-step rewards.

    reward[i] = (
        reward[i] * gamma**0 +
        reward[i+1] * gamma**1 +
        ... +
        reward[i+n_step-1] * gamma**(n_step-1))

    The ith new_obs is also adjusted to point to the (i+n_step-1)'th new obs.

    If the episode finishes, the reward will be truncated. After this rewrite,
    all the arrays will be shortened by (n_step - 1).
    """
    for i in range(len(rewards) - n_step + 1):
        if dones[i]:
            continue  # episode end
        for j in range(1, n_step):
            new_obs[i] = new_obs[i + j]
            rewards[i] += gamma ** j * rewards[i + j]
            if dones[i + j]:
                break  # episode end
    # truncate ends of the trajectory
    new_len = len(obs) - n_step + 1
    for arr in [obs, actions, rewards, new_obs, dones]:
        del arr[new_len:]


class DQNEvaluator(Evaluator):
    """The DQN Evaluator.

    TODO(rliaw): Support observation/reward filters?"""

    def __init__(self, registry, env_creator, config, logdir, worker_index):
        env = env_creator(config["env_config"])
        env = wrap_dqn(registry, env, config["model"])
        self.env = env
        self.config = config

        if not isinstance(env.action_space, Discrete):
            raise UnsupportedSpaceException(
                "Action space {} is not supported for DQN.".format(
                    env.action_space))

        tf_config = tf.ConfigProto(**config["tf_session_args"])
        self.sess = tf.Session(config=tf_config)
        self.dqn_graph = models.DQNGraph(registry, env, config, logdir)

        # Use either a different `eps` per worker, or a linear schedule.
        if config["per_worker_exploration"]:
            assert config["num_workers"] > 1, "This requires multiple workers"
            self.exploration = ConstantSchedule(
                0.4 ** (
                    1 + worker_index / float(config["num_workers"] - 1) * 7))
        else:
            self.exploration = LinearSchedule(
                schedule_timesteps=int(
                    config["exploration_fraction"] *
                    config["schedule_max_timesteps"]),
                initial_p=1.0,
                final_p=config["exploration_final_eps"])

        # Initialize the parameters and copy them to the target network.
        self.sess.run(tf.global_variables_initializer())
        self.dqn_graph.update_target(self.sess)
        self.global_timestep = 0
        self.local_timestep = 0

        # Note that this encompasses both the Q and target network
        self.variables = ray.experimental.TensorFlowVariables(
            tf.group(self.dqn_graph.q_t, self.dqn_graph.q_tp1), self.sess)

        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]
        self.saved_mean_reward = None

        self.obs = self.env.reset()

    def set_global_timestep(self, global_timestep):
        self.global_timestep = global_timestep

    def update_target(self):
        self.dqn_graph.update_target(self.sess)

    def sample(self):
        obs, actions, rewards, new_obs, dones = [], [], [], [], []
        for _ in range(
                self.config["sample_batch_size"] + self.config["n_step"] - 1):
            ob, act, rew, ob1, done = self._step(self.global_timestep)
            obs.append(ob)
            actions.append(act)
            rewards.append(rew)
            new_obs.append(ob1)
            dones.append(done)

        # N-step Q adjustments
        if self.config["n_step"] > 1:
            # Adjust for steps lost from truncation
            self.local_timestep -= (self.config["n_step"] - 1)
            adjust_nstep(
                self.config["n_step"], self.config["gamma"],
                obs, actions, rewards, new_obs, dones)

        batch = SampleBatch({
            "obs": [pack(np.array(o)) for o in obs], "actions": actions,
            "rewards": rewards,
            "new_obs": [pack(np.array(o)) for o in new_obs], "dones": dones,
            "weights": np.ones_like(rewards)})
        assert (batch.count == self.config["sample_batch_size"])

        # Prioritize on the worker side
        if self.config["worker_side_prioritization"]:
            td_errors = self.dqn_graph.compute_td_error(
                self.sess, obs, batch["actions"], batch["rewards"],
                new_obs, batch["dones"], batch["weights"])
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            batch.data["weights"] = new_priorities

        return batch

    def compute_apply(self, samples):
        if samples is None:
            return None
        td_error = self.dqn_graph.compute_apply(
            self.sess, samples["obs"], samples["actions"], samples["rewards"],
            samples["new_obs"], samples["dones"], samples["weights"])
        return td_error

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def _step(self, global_timestep):
        """Takes a single step, and returns the result of the step."""
        action = self.dqn_graph.act(
            self.sess, np.array(self.obs)[None],
            self.exploration.value(global_timestep))[0]
        new_obs, rew, done, _ = self.env.step(action)
        ret = (self.obs, action, rew, new_obs, float(done))
        self.obs = new_obs
        self.episode_rewards[-1] += rew
        self.episode_lengths[-1] += 1
        if done:
            self.obs = self.env.reset()
            self.episode_rewards.append(0.0)
            self.episode_lengths.append(0.0)
        self.local_timestep += 1
        return ret

    def stats(self):
        n = self.config["smoothing_num_episodes"] + 1
        mean_100ep_reward = round(np.mean(self.episode_rewards[-n:-1]), 5)
        mean_100ep_length = round(np.mean(self.episode_lengths[-n:-1]), 5)
        exploration = self.exploration.value(self.global_timestep)
        return {
            "mean_100ep_reward": mean_100ep_reward,
            "mean_100ep_length": mean_100ep_length,
            "num_episodes": len(self.episode_rewards),
            "exploration": exploration,
            "local_timestep": self.local_timestep,
        }

    def save(self):
        return [
            self.exploration,
            self.episode_rewards,
            self.episode_lengths,
            self.saved_mean_reward,
            self.obs,
            self.global_timestep,
            self.local_timestep]

    def restore(self, data):
        self.exploration = data[0]
        self.episode_rewards = data[1]
        self.episode_lengths = data[2]
        self.saved_mean_reward = data[3]
        self.obs = data[4]
        self.global_timestep = data[5]
        self.local_timestep = data[6]
