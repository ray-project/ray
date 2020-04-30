"""An example of implementing a centralized critic with ObservationFunction.

The advantage of this approach is that it's very simple and you don't have to
change the algorithm at all -- just use callbacks and a custom model.
However, it is a bit less principled in that you have to change the agent
observation spaces to include data that is only used at train time.

See also: centralized_critic.py for an alternative approach that instead
modifies the policy to add a centralized value function.
"""

import numpy as np
from gym.spaces import Box, Dict, Discrete
import argparse

from ray import tune
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.evaluation.observation_function import ObservationFunction
from ray.rllib.examples.twostep_game import TwoStepGame
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet_v2 import FullyConnectedNetwork
from ray.rllib.policy.sample_batch import SampleBatch
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
    - A value model that also looks at the 'opponent_obs' / 'opponent_action'
      to compute the value (it does this by using the 'obs_flat' tensor).
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(CentralizedCriticModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)

        self.action_model = FullyConnectedNetwork(
            Box(low=0, high=1, shape=(6, )),  # one-hot encoded Discrete(6)
            action_space,
            num_outputs,
            model_config,
            name + "_action")
        self.register_variables(self.action_model.variables())

        self.value_model = FullyConnectedNetwork(obs_space, action_space, 1,
                                                 model_config, name + "_vf")
        self.register_variables(self.value_model.variables())

    def forward(self, input_dict, state, seq_lens):
        self._value_out, _ = self.value_model({
            "obs": input_dict["obs_flat"]
        }, state, seq_lens)
        return self.action_model({
            "obs": input_dict["obs"]["own_obs"]
        }, state, seq_lens)

    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class FillInActions(DefaultCallbacks):
    """Fills in the opponent actions info in the training batches."""

    def on_postprocess_trajectory(self, worker, episode, agent_id, policy_id,
                                  policies, postprocessed_batch,
                                  original_batches, **kwargs):
        to_update = postprocessed_batch[SampleBatch.CUR_OBS]
        other_id = 1 if agent_id == 0 else 0
        action_encoder = ModelCatalog.get_preprocessor_for_space(Discrete(2))

        # set the opponent actions into the observation
        _, opponent_batch = original_batches[other_id]
        opponent_actions = np.array([
            action_encoder.transform(a)
            for a in opponent_batch[SampleBatch.ACTIONS]
        ])
        to_update[:, -2:] = opponent_actions


class CentralCriticObserver(ObservationFunction):
    """Rewrites the agent obs to include opponent data for training."""

    def observe(self, agent_obs, worker, base_env, episode, **kw):
        new_obs = {
            0: {
                "own_obs": agent_obs[0],
                "opponent_obs": agent_obs[1],
                "opponent_action": 0,  # filled in by FillInActions
            },
            1: {
                "own_obs": agent_obs[1],
                "opponent_obs": agent_obs[0],
                "opponent_action": 0,  # filled in by FillInActions
            },
        }
        return new_obs


if __name__ == "__main__":
    args = parser.parse_args()
    ModelCatalog.register_custom_model("cc_model", CentralizedCriticModel)
    action_space = Discrete(2)
    observer_space = Dict({
        "own_obs": Discrete(6),
        # These two fields are filled in by the CentralCriticObserver, and are
        # not used for inference, only for training.
        "opponent_obs": Discrete(6),
        "opponent_action": Discrete(2),
    })
    tune.run(
        "PPO",
        stop={
            "timesteps_total": args.stop,
            "episode_reward_mean": 7.99,
        },
        config={
            "env": TwoStepGame,
            "batch_mode": "complete_episodes",
            "callbacks": FillInActions,
            "num_workers": 0,
            "multiagent": {
                "policies": {
                    "pol1": (None, observer_space, action_space, {}),
                    "pol2": (None, observer_space, action_space, {}),
                },
                "policy_mapping_fn": lambda x: "pol1" if x == 0 else "pol2",
                "observation_fn": CentralCriticObserver,
            },
            "model": {
                "custom_model": "cc_model",
            },
        })
