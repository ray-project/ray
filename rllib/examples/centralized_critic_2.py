from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""An example of implementing a centralized critic by modifying the env.

The advantage of this approach is that it's very simple and you don't have
to change the algorithm at all. However, it is limited in that you can't
include the opponent actions as critic inputs.

See also: centralized_critic.py for an alternative approach that instead
modifies the policy to add a centralized value function that takes into
account both opponent observations and actions.
"""

from gym.spaces import Box, Dict, Discrete
import argparse
import numpy as np

from ray import tune
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.examples.twostep_game import TwoStepGame
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet_v2 import FullyConnectedNetwork
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--stop", type=int, default=100000)


class CentralizedCriticModel(TFModelV2):
    """Multi-agent model that implements a centralized VF.

    It assumes the observation is a dict with 'own_obs' and 'opponent_obs', the
    former of which can be used for computing actions (i.e., decentralized
    execution), and the latter for optimization (i.e., centralized learning).
    
    This model has two parts:
    - An action model that looks at just 'own_obs' to compute actions
    - A value model that also looks at the 'opponent_obs' to compute the value
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(CentralizedCriticModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)

        self.action_model = FullyConnectedNetwork(
            Box(low=0, high=1, shape=(6,)),  # one-hot encoded Discrete(6)
            action_space, num_outputs, model_config, name + "_action")
        self.register_variables(self.action_model.variables())

        self.value_model = FullyConnectedNetwork(
            obs_space, action_space, 1, model_config, name + "_vf")
        self.register_variables(self.value_model.variables())

    def forward(self, input_dict, state, seq_lens):
        self._value_out, _ = self.value_model(
            {"obs": input_dict["obs_flat"]}, state, seq_lens)
        return self.action_model(
            {"obs": input_dict["obs"]["own_obs"]}, state, seq_lens)

    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class GlobalObsTwoStepGame(MultiAgentEnv):
    action_space = Discrete(2)
    observation_space = Dict(
        {"own_obs": Discrete(6), "opponent_obs": Discrete(6)})

    def __init__(self, env_config):
        self.env = TwoStepGame(env_config)

    def reset(self):
        obs_dict = self.env.reset()
        return self.to_global_obs(obs_dict)

    def step(self, action_dict):
        obs_dict, rewards, dones, infos = self.env.step(action_dict)
        return self.to_global_obs(obs_dict), rewards, dones, infos

    def to_global_obs(self, obs_dict):
        return {
            self.env.agent_1: {
                "own_obs": obs_dict[self.env.agent_1],
                "opponent_obs": obs_dict[self.env.agent_2],
            },
            self.env.agent_2: {
                "own_obs": obs_dict[self.env.agent_2],
                "opponent_obs": obs_dict[self.env.agent_1],
            },
        }


if __name__ == "__main__":
    args = parser.parse_args()
    ModelCatalog.register_custom_model("cc_model", CentralizedCriticModel)
    tune.run(
        "PPO",
        stop={
            "timesteps_total": args.stop,
            "episode_reward_mean": 7.99,
        },
        config={
            "env": GlobalObsTwoStepGame,
            "batch_mode": "complete_episodes",
            "num_workers": 0,
            "multiagent": {
                "policies": {
                    "pol1": (None, GlobalObsTwoStepGame.observation_space,
                             GlobalObsTwoStepGame.action_space, {}),
                    "pol2": (None, GlobalObsTwoStepGame.observation_space,
                             GlobalObsTwoStepGame.action_space, {}),
                },
                "policy_mapping_fn": tune.function(
                    lambda x: "pol1" if x == 0 else "pol2"),
            },
            "model": {
                "custom_model": "cc_model",
            },
        })
