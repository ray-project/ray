import torch.nn as nn
import torch
import torch.nn.functional as F
import gym
import numpy as np

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.typing import TensorType, ModelConfigDict
from ray.rllib.utils.annotations import override
from ray.rllib.models.preprocessors import get_preprocessor


def convert_to_tensor(arr):
    tensor = torch.from_numpy(np.asarray(arr))
    if tensor.dtype == torch.double:
        tensor = tensor.float()
    return tensor


class LeelaChessZeroModel(TorchModelV2, nn.Module):
    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
    ):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)
        try:
            self.preprocessor = get_preprocessor(obs_space.original_space)(
                obs_space.original_space
            )
        except Exception:
            self.preprocessor = get_preprocessor(obs_space)(obs_space)

        self.action_masking = False
        self.alpha_zero_obs = True
        if self.alpha_zero_obs:
            self.input_channel_size = 111
        else:
            self.input_channel_size = 19

        filters = 32
        res_blocks = 3
        se_channels = 0
        policy_conv_size = 73
        policy_output_size = 4672
        self.num_outputs = 4672
        self.name = name
        self.obs_space = obs_space
        self.action_space = action_space
        self.model_config = model_config

        self.filters = filters
        self.res_blocks = res_blocks
        self.se_channels = se_channels
        self.policy_conv_size = policy_conv_size
        self.policy_output_size = policy_output_size
        self.pre_conv = nn.Conv2d(
            self.input_channel_size, self.filters, 3, padding="same"
        )
        self.conv1 = nn.Conv2d(self.filters, self.filters, 3, padding="same")
        self.conv2 = nn.Conv2d(self.filters, self.filters, 3, padding="same")
        self.pool = nn.AvgPool2d(8)
        self.se1 = nn.Linear(self.filters, self.se_channels)
        self.se2 = nn.Linear(self.se_channels, self.filters * 2)
        self.fc_head = nn.Linear(self.filters * 64, 128)
        self.value_head = nn.Linear(128, 1)
        self.policy_conv1 = nn.Conv2d(
            self.filters, self.policy_conv_size, 3, padding="same"
        )
        self.policy_fc = nn.Linear(self.policy_conv_size * 64, self.policy_output_size)
        self._value = None

    @override(TorchModelV2)
    def forward(self, input_dict, state, seq_lens):
        try:
            obs = input_dict["obs"]["observation"]
            action_mask = input_dict["obs"]["action_mask"]
        except KeyError:
            try:
                obs = input_dict["obs"]
                action_mask = input_dict["action_mask"]
            except KeyError:
                try:
                    obs = input_dict["observation"]
                    action_mask = input_dict["action_mask"]
                except KeyError:
                    print(input_dict)
                    raise Exception("No observation in input_dict")
        if self.alpha_zero_obs:
            if not type(obs) == torch.Tensor:
                obs = torch.from_numpy(obs.astype(np.float32))
                action_mask = torch.from_numpy(action_mask.astype(np.float32))
            try:
                obs = torch.transpose(obs, 3, 1)
                obs = torch.transpose(obs, 3, 2)
            except IndexError:
                obs = torch.reshape(obs, (1, 8, 8, self.input_channel_size))
                obs = torch.transpose(obs, 3, 1)
                obs = torch.transpose(obs, 3, 2)

        x = self.pre_conv(obs)
        residual = x
        for i in range(self.res_blocks):
            x = self.conv1(x)
            x = self.conv2(x)
            if self.se_channels > 0:
                input = x
                se = self.pool(x)
                se = torch.flatten(se, 1)
                se = F.relu(self.se1(se))
                se = self.se2(se)
                w, b = torch.tensor_split(se, 2, dim=-1)
                z = torch.sigmoid(w)
                input = torch.reshape(input, (-1, self.filters, 64))
                z = torch.reshape(z, (-1, self.filters, 1))
                se = torch.mul(z, input)
                se = torch.reshape(se, (-1, self.filters, 8, 8))
                se += b
            x += residual
            residual = x
            x = torch.relu(x)
        value = torch.flatten(x, 1)
        value = torch.relu(self.fc_head(value))
        value = torch.tanh(self.value_head(value))
        policy = self.policy_conv1(x)
        policy = torch.flatten(policy, 1)
        policy = self.policy_fc(policy)
        self._value = value.squeeze(1)

        if self.action_masking:
            masked_policy = self.apply_action_mask(policy, action_mask)
            return masked_policy, state
        else:
            return policy, state

    @override(TorchModelV2)
    def value_function(self) -> TensorType:
        return self._value

    def apply_action_mask(self, policy, action_mask):
        masked_policy = torch.mul(policy, action_mask)
        action_mask = torch.clamp(torch.log(action_mask), -1e10, 3.4e38)
        return masked_policy + action_mask

    def get_board_evaluation(self, obs):
        return self.compute_priors_and_value(obs)

    def compute_priors_and_value(self, obs):
        new_obs = torch.from_numpy(
            obs["observation"]
            .astype(np.float32)
            .reshape([1, 8, 8, self.input_channel_size])
        )
        new_action_mask = torch.from_numpy(
            obs["action_mask"].astype(np.float32).reshape([1, self.num_outputs])
        )
        input_dict = {"obs": {"observation": new_obs, "action_mask": new_action_mask}}
        with torch.no_grad():
            model_out = self.forward(input_dict, None, [1])
            logits, _ = model_out
            value = self.value_function()
            logits, value = torch.squeeze(logits), torch.squeeze(value)
            priors = nn.Softmax(dim=-1)(logits)
            value = nn.Tanh()(value)

            priors = priors.cpu().numpy()
            value = value.cpu().numpy()
            return priors, value
