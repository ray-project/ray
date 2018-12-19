from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
from gym.spaces import Discrete, Box, Dict
import os
import random
import sys
import yaml

import ray
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.tune.registry import register_env



class ParametricActionsModel(Model):
    """Parametric action model that handles the dot product and masking.
    This assumes the outputs are logits for a single Categorical action dist.
    Getting this to work with a more complex output (e.g., if the action space
    is a tuple of several distributions) is also possible but left as an
    exercise to the reader.
    """

    def _build_layers_v2(self, input_dict, num_outputs, options):
        # Extract the available actions tensor from the observation.
        # avail_actions = input_dict["obs"]["avail_actions"]
        action_mask = input_dict["obs"]["action_mask"]
        # action_embed_size = avail_actions.shape[2].value
        if num_outputs != action_mask.shape[1].value:
            raise ValueError(
                "This model assumes num outputs is equal to max avail actions",
                num_outputs, action_mask)

        # Standard FC net component.
        last_layer = input_dict["obs"]["real_obs"]
        hiddens = [256, 256]
        for i, size in enumerate(hiddens):
            label = "fc{}".format(i)
            last_layer = slim.fully_connected(
                last_layer,
                size,
                weights_initializer=normc_initializer(1.0),
                activation_fn=tf.nn.tanh,
                scope=label)
        action_logits = slim.fully_connected(
            last_layer,
            num_outputs,
            weights_initializer=normc_initializer(0.01),
            activation_fn=None,
            scope="fc_out")

        # # Expand the model output to [BATCH, 1, EMBED_SIZE]. Note that the
        # # avail actions tensor is of shape [BATCH, MAX_ACTIONS, EMBED_SIZE].
        # intent_vector = tf.expand_dims(output, 1)

        # # Batch dot product => shape of logits is [BATCH, MAX_ACTIONS].
        # action_logits = tf.reduce_sum(avail_actions * intent_vector, axis=2)

        # Mask out invalid actions (use tf.float32.min for stability)
        inf_mask = tf.maximum(tf.log(action_mask), tf.float32.min)
        masked_logits = inf_mask + action_logits

        return masked_logits, last_layer


class SC2MultiAgentEnv(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 25 steps."""

    def __init__(self):
        PYMARL_PATH = os.environ.get("PYMARL_PATH")
        sys.path.append(os.path.join(PYMARL_PATH, "src"))
        from envs.starcraft2 import StarCraft2Env
        with open(os.path.join(PYMARL_PATH, "src/config/envs/sc2.yaml")) as f:
            pymarl_args = yaml.load(f)
            # HACK
            pymarl_args["env_args"]["seed"] = 0

        self._starcraft_env = StarCraft2Env(**pymarl_args)
        obs_size = self._starcraft_env.get_obs_size()
        num_actions = self._starcraft_env.get_total_actions()
        self.observation_space = Dict({
            "action_mask": Box(0, 1, shape=(num_actions,)),
            "real_obs": Box(-1, 1, shape=(obs_size,))
        })
        # self.observation_space = Box(-1, 1, shape=(obs_size,))
        self.action_space = Discrete(self._starcraft_env.get_total_actions())

    def reset(self):
        obs_list, state_list = self._starcraft_env.reset()
        return_obs = {}
        for i, obs in enumerate(obs_list):
            return_obs[i] = {
                "action_mask": self._starcraft_env.get_avail_agent_actions(i),
                "real_obs": obs
            }
        return return_obs

    def step(self, action_dict):
        # TODO(rliaw): Check to handle missing agents, if any
        actions = [action_dict[k] for k in sorted(action_dict)]
        rew, done, info = self._starcraft_env.step(actions)
        obs_list = self._starcraft_env.get_obs()
        return_obs = {}
        for i, obs in enumerate(obs_list):
            return_obs[i] = {
                "action_mask": self._starcraft_env.get_avail_agent_actions(i),
                "real_obs": obs
            }
        # obs = dict(enumerate(obs_list))

        # TODO: check what the reward actually is
        rews = {i: rew for i in range(len(obs_list))}
        dones = {i: done for i in range(len(obs_list))}
        dones["__all__"] = done
        infos = {i: info for i in range(len(obs_list))}
        return return_obs, rews, dones, infos


if __name__ == "__main__":
    path_to_pymarl = "/data/rliaw/pymarl/"
    os.environ.setdefault("PYMARL_PATH", path_to_pymarl)
    os.environ["SC2PATH"] = os.path.join(path_to_pymarl,
                                         "3rdparty/StarCraftII")
    from ray.rllib.agents.pg import PGAgent
    from ray.tune.logger import pretty_print
    ray.init()
    ModelCatalog.register_custom_model("pa_model", ParametricActionsModel)

    register_env("starcraft", lambda _: SC2MultiAgentEnv())
    agent = PGAgent(env="starcraft", config={
        "model": {
            "custom_model": "pa_model"
        }
    })
    for i in range(100):
        print(pretty_print(agent.train()))

    # env = SC2MultiAgentEnv()
    # x = env.reset()
    # returns = env.step({i: 0 for i in range(len(x))})
