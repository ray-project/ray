"""Example of using variable-length Repeated / struct observation spaces.

This example shows:
  - using a custom environment with Repeated / struct observations
  - using a custom model to view the batched list observations

For PyTorch / TF eager mode, use the --torch and --eager flags.
"""

import argparse
import gym
from gym.spaces import Discrete, Box, Dict

import ray
from ray import tune
from ray.rllib.utils import try_import_tf, try_import_torch
from ray.rllib.models import ModelCatalog
from ray.rllib.models.extra_spaces import Repeated
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet_v2 import FullyConnectedNetwork as TFFCNet
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFCNet

tf = try_import_tf()
torch, nn = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")
parser.add_argument("--eager", action="store_true")

# Constraints on the Repeated space.
MAX_PLAYERS = 4
MAX_ITEMS = 7
MAX_EFFECTS = 2


class SimpleRPG(gym.Env):
    """Example of a custom env with a complex, structured observation.

    The observation is a list of players, each of which is a Dict of
    attributes, and may further hold a list of items (categorical space).

    Note that the env doesn't train, it's just a dummy example to show how to
    use extra_spaces.Repeated in a custom model (see CustomRPGModel below).
    """

    def __init__(self, config):
        self.cur_pos = 0
        self.action_space = Discrete(4)

        # Represents an item.
        self.item_space = Discrete(5)

        # Represents an effect on the player.
        self.effect_space = Box(9000, 9999, shape=(4, ))

        # Represents a player.
        self.player_space = Dict({
            "location": Box(-100, 100, shape=(2, )),
            "status": Box(-1, 1, shape=(10, )),
            "items": Repeated(self.item_space, max_len=MAX_ITEMS),
            "effects": Repeated(self.effect_space, max_len=MAX_EFFECTS),
        })

        # Observation is a list of players.
        self.observation_space = Repeated(
            self.player_space, max_len=MAX_PLAYERS)

    def reset(self):
        return self.observation_space.sample()

    def step(self, action):
        return self.observation_space.sample(), 1, True, {}


class CustomTorchRPGModel(TorchModelV2, nn.Module):
    """Example of interpreting repeated observations."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        nn.Module.__init__(self)
        self.model = TorchFCNet(obs_space, action_space, num_outputs,
                                model_config, name)

    def forward(self, input_dict, state, seq_lens):
        # The unpacked input tensors, where M=MAX_PLAYERS, N=MAX_ITEMS:
        # {
        #   'items', <torch.Tensor shape=(?, M, N, 5)>,
        #   'location', <torch.Tensor shape=(?, M, 2)>,
        #   'status', <torch.Tensor shape=(?, M, 10)>,
        # }
        print("The unpacked input tensors:", input_dict["obs"])
        print()
        print("Unbatched repeat dim", input_dict["obs"].unbatch_repeat_dim())
        print()
        print("Fully unbatched", input_dict["obs"].unbatch_all())
        print()
        return self.model.forward(input_dict, state, seq_lens)

    def value_function(self):
        return self.model.value_function()


class CustomTFRPGModel(TFModelV2):
    """Example of interpreting repeated observations."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        self.model = TFFCNet(obs_space, action_space, num_outputs,
                             model_config, name)
        self.register_variables(self.model.variables())

    def forward(self, input_dict, state, seq_lens):
        # The unpacked input tensors, where M=MAX_PLAYERS, N=MAX_ITEMS:
        # {
        #   'items', <tf.Tensor shape=(?, M, N, 5)>,
        #   'location', <tf.Tensor shape=(?, M, 2)>,
        #   'status', <tf.Tensor shape=(?, M, 10)>,
        # }
        print("The unpacked input tensors:", input_dict["obs"])
        print()
        print("Unbatched repeat dim", input_dict["obs"].unbatch_repeat_dim())
        print()
        if args.eager:
            print("Fully unbatched", input_dict["obs"].unbatch_all())
            print()
        return self.model.forward(input_dict, state, seq_lens)

    def value_function(self):
        return self.model.value_function()


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    if args.torch:
        ModelCatalog.register_custom_model("my_model", CustomTorchRPGModel)
    else:
        ModelCatalog.register_custom_model("my_model", CustomTFRPGModel)
    tune.run(
        "PG",
        stop={
            "timesteps_total": 1,
        },
        config={
            "use_pytorch": args.torch,
            "eager": args.eager,
            "env": SimpleRPG,
            "rollout_fragment_length": 1,
            "train_batch_size": 2,
            "num_workers": 0,
            "model": {
                "custom_model": "my_model",
            },
        },
    )
