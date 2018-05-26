from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Box
import numpy as np
import tensorflow as tf

import ray
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.ddpg import models
from ray.rllib.dqn.common.schedules import ConstantSchedule, LinearSchedule
from ray.rllib.optimizers import SampleBatch, PolicyEvaluator
from ray.rllib.utils.compression import pack
from ray.rllib.dqn.dqn_evaluator import adjust_nstep
from ray.rllib.dqn.common.wrappers import wrap_dqn


class DDPGEvaluator(PolicyEvaluator):
    """The base DDPG Evaluator."""

    def __init__(self, registry, env_creator, config, logdir, worker_index):
        env = env_creator(config["env_config"])
        env = wrap_dqn(registry, env, config["model"], config["random_starts"])
        self.env = env
        self.config = config

        # when env.action_space is of Box type, e.g., Pendulum-v0
        # action_space.low is [-2.0], high is [2.0]
        # take action by calling, e.g., env.step([3.5])
        if not isinstance(env.action_space, Box):
            raise UnsupportedSpaceException(
                "Action space {} is not supported for DDPG.".format(
                    env.action_space))

        tf_config = tf.ConfigProto(**config["tf_session_args"])
        self.sess = tf.Session(config=tf_config)
        self.ddpg_graph = models.DDPGGraph(registry, env, config, logdir)

        # Use either a different `eps` per worker, or a linear schedule.
        if config["per_worker_exploration"]:
            assert config["num_workers"] > 1, "This requires multiple workers"
            self.exploration = ConstantSchedule(
                config["noise_scale"] * 0.4 **
                (1 + worker_index / float(config["num_workers"] - 1) * 7))
        else:
            self.exploration = LinearSchedule(
                schedule_timesteps=int(config["exploration_fraction"] *
                                       config["schedule_max_timesteps"]),
                initial_p=config["noise_scale"] * 1.0,
                final_p=config["noise_scale"] *
                config["exploration_final_eps"])

        # Initialize the parameters and copy them to the target network.
        self.sess.run(tf.global_variables_initializer())
        # hard instead of soft
        self.ddpg_graph.update_target(self.sess, 1.0)
        self.global_timestep = 0
        self.local_timestep = 0

        # Note that this encompasses both the policy and Q-value networks and
        # their corresponding target networks
        self.variables = ray.experimental.TensorFlowVariables(
            tf.group(self.ddpg_graph.q_tp0, self.ddpg_graph.q_tp1), self.sess)

        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]
        self.saved_mean_reward = None

        self.obs = self.env.reset()

    def set_global_timestep(self, global_timestep):
        self.global_timestep = global_timestep

    def update_target(self):
        self.ddpg_graph.update_target(self.sess)

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
            adjust_nstep(self.config["n_step"], self.config["gamma"], obs,
                         actions, rewards, new_obs, dones)

        batch = SampleBatch({
            "obs": [pack(np.array(o)) for o in obs],
            "actions": actions,
            "rewards": rewards,
            "new_obs": [pack(np.array(o)) for o in new_obs],
            "dones": dones,
            "weights": np.ones_like(rewards)
        })
        assert (batch.count == self.config["sample_batch_size"])

        # Prioritize on the worker side
        if self.config["worker_side_prioritization"]:
            td_errors = self.ddpg_graph.compute_td_error(
                self.sess, obs, batch["actions"], batch["rewards"], new_obs,
                batch["dones"], batch["weights"])
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            batch.data["weights"] = new_priorities

        return batch

    def compute_gradients(self, samples):
        td_err, grads = self.ddpg_graph.compute_gradients(
            self.sess, samples["obs"], samples["actions"], samples["rewards"],
            samples["new_obs"], samples["dones"], samples["weights"])
        return grads, {"td_error": td_err}

    def apply_gradients(self, grads):
        self.ddpg_graph.apply_gradients(self.sess, grads)

    def compute_apply(self, samples):
        td_error = self.ddpg_graph.compute_apply(
            self.sess, samples["obs"], samples["actions"], samples["rewards"],
            samples["new_obs"], samples["dones"], samples["weights"])
        return {"td_error": td_error}

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def _step(self, global_timestep):
        """Takes a single step, and returns the result of the step."""
        action = self.ddpg_graph.act(
            self.sess,
            np.array(self.obs)[None],
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
            # reset UO noise for each episode
            self.ddpg_graph.reset_noise(self.sess)

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
            self.exploration, self.episode_rewards, self.episode_lengths,
            self.saved_mean_reward, self.obs, self.global_timestep,
            self.local_timestep
        ]

    def restore(self, data):
        self.exploration = data[0]
        self.episode_rewards = data[1]
        self.episode_lengths = data[2]
        self.saved_mean_reward = data[3]
        self.obs = data[4]
        self.global_timestep = data[5]
        self.local_timestep = data[6]
