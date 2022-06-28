
from ray import tune
import ray
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.typing import TensorType

import torch
import torch.nn as nn

###############################################################################
# The pre-trained backbone that will be used to train the policy.
###############################################################################
class Encoder(nn.Module):
    def __init__(self, input_size, hidden_size):
        super().__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.fc2 = nn.Linear(hidden_size, hidden_size)
        self.fc3 = nn.Linear(hidden_size, hidden_size)

    def forward(self, x):
        x = self.fc1(x)
        x = self.fc2(x)
        x = self.fc3(x)
        return x
    
class MyModel(nn.Module):

    def __init__(self, input_dim, hidden_size):
        super().__init__()
        self.encoder = Encoder(input_dim, hidden_size)
        self.head = nn.Linear(hidden_size, 1)
    
    def forward(self, x):
        feat = self.encoder(x)
        out = self.head(feat)
        return out

###############################################################################
# The custom PPO Model that will re-used the pre-trained encoder.
###############################################################################
"""
We assume the PPO is comprised of an encoder, and two prediction heads:
1) the policy head which predicts the mean and std of the action distribution.
2) the value head which predicts the value of the state.
"""
class CustomPPOModel(TorchModelV2, nn.Module):

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)

        custom_config = model_config["custom_model_config"]
        self.encoder = Encoder(obs_space.shape[0], custom_config["hidden_size"])
        # assume obs and action space are a flat Box space
        self.policy_head = nn.Linear(custom_config["hidden_size"], 2 * action_space.shape[0])
        self.value_head = nn.Linear(custom_config["hidden_size"], 1)
        
        self._features = None
    
    def forward(self, input_dict, state, seq_lens):
        self._features = self.encoder(input_dict["obs"])
        policy_logits = self.policy_head(self._features)
        return policy_logits, state

    def value_function(self) -> TensorType:
        return self.value_head(self._features).squeeze(-1)


###############################################################################
# Custom callback to initialize the model with the pre-trained encoder.
###############################################################################

"""
In this example we assume you have a pre-trained encoder that you trained using arbitrary pytorch code-base. You can alternatively use AIR / Train API to load 
the corresponding checkpoint more elegantly.
"""
path_to_checkpoint = "checkpoint_dir/checkpoint_0.ckpt"
class InitPoliciesCallbacks(DefaultCallbacks):
    def __init__(self):
        super().__init__()

    def on_algorithm_init(self, *, algorithm, **kwargs):
        model = algorithm.workers.local_worker().get_policy().model
        
        # Load the pre-trained encoder.
        hidden_dims = algorithm.config["model"]["custom_model_config"]["hidden_size"]
        my_pretrained_model = MyModel(input_dim=model.obs_space.shape[0], hidden_size=hidden_dims)
        # Note: Assume there is a valid checkpoint inside path_to_checkpoint.
        # my_model.load_state_dict(torch.load(path_to_checkpoint))

        model.encoder.load_state_dict(my_pretrained_model.encoder.state_dict())

        # Note: You can optionally freeze the pre-trained encoder by setting the following line.
        for param in model.encoder:
            param.requires_grad = False




if __name__ == "__main__":

    ray.init(local_mode=True)
    config = (
        PPOConfig().framework('torch')
        .callbacks(callbacks_class=InitPoliciesCallbacks)
        .training(
            model=dict(
                custom_model=CustomPPOModel, 
                custom_model_config={"hidden_size": 256},
            )
        )
        .rollouts(num_rollout_workers=0)
    )
    ppo = config.build(env="Pendulum-v1")

    tune.run(
        PPO,
        config=config.to_dict(),
        stop={'timesteps_total': 100000},
    )

