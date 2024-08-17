from typing import Any, Dict

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.torch.misc import (
    normc_initializer,
    same_padding,
    valid_padding,
)
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class TinyAtariCNN(TorchRLModule):
    """A tiny CNN stack for fast-learning of Atari envs.

    The architecture here is the exact same as the one used by the old API stack as
    CNN default ModelV2.

    We stack 3 CNN layers based on the config, then a 4th one with linear activation
    and n 1x1 filters, where n is the number of actions in the (discrete) action space.
    Simple reshaping (no flattening or extra linear layers necessary) lead to the
    action logits, which can directly be used inside a distribution or loss.

        import numpy as np
    import gymnasium as gym
    from ray.rllib.core.rl_module.rl_module import RLModuleConfig

    rl_module_config = RLModuleConfig(
        observation_space=gym.spaces.Box(-1.0, 1.0, (42, 42, 4), np.float32),
        action_space=gym.spaces.Discrete(4),
    )
    my_net = TinyAtariCNN(rl_module_config)

    B = 10
    w = 42
    h = 42
    c = 4
    data = torch.from_numpy(
        np.random.random_sample(size=(B, w, h, c)).astype(np.float32)
    )
    print(my_net.forward_inference({"obs": data}))
    print(my_net.forward_exploration({"obs": data}))
    print(my_net.forward_train({"obs": data}))

    num_all_params = sum(int(np.prod(p.size())) for p in my_net.parameters())
    print(f"num params = {num_all_params}")

    """

    @override(TorchRLModule)
    def setup(self):
        """Use this method to create all the model components that you require.

        Feel free to access the following useful properties in this class:
        - `self.config.model_config_dict`: The config dict for this RLModule class,
        which should contain flxeible settings, for example: {"hiddens": [256, 256]}.
        - `self.config.observation|action_space`: The observation and action space that
        this RLModule is subject to. Note that the observation space might not be the
        exact space from your env, but that it might have already gone through
        preprocessing through a connector pipeline (for example, flattening,
        frame-stacking, mean/std-filtering, etc..).
        """
        # Get the CNN stack config from our RLModuleConfig's (self.config)
        # `model_config_dict` property:
        conv_filters = self.config.model_config_dict.get("conv_filters")
        # Default CNN stack with 3 layers:
        if conv_filters is None:
            conv_filters = [
                [16, 4, 2, "same"],  # num filters, kernel wxh, stride wxh, padding type
                [32, 4, 2, "same"],
                [256, 11, 1, "valid"],
            ]

        # Build the CNN layers.
        layers = []

        # Add user-specified hidden convolutional layers first
        width, height, in_depth = self.config.observation_space.shape
        in_size = [width, height]
        for filter_specs in conv_filters:
            out_depth, kernel_size, strides, padding = filter_specs

            # Pad like in tensorflow's SAME/VALID mode.
            if padding == "same":
                padding_size, out_size = same_padding(in_size, kernel_size, strides)
                layers.append(nn.ZeroPad2d(padding_size))
            # No actual padding is performed for "valid" mode, but we will still
            # compute the output size (input for the next layer).
            else:
                out_size = valid_padding(in_size, kernel_size, strides)

            layer = nn.Conv2d(in_depth, out_depth, kernel_size, strides, bias=True)
            # Initialize CNN layer kernel and bias.
            nn.init.xavier_uniform_(layer.weight)
            nn.init.zeros_(layer.bias)
            layers.append(layer)
            # Activation.
            layers.append(nn.ReLU())

            in_size = out_size
            in_depth = out_depth

        self._base_cnn_stack = nn.Sequential(*layers)

        # Add the final CNN 1x1 layer with num_filters == num_actions to be reshaped to
        # yield the logits (no flattening, no additional linear layers required).
        _final_conv = nn.Conv2d(in_depth, self.config.action_space.n, 1, 1, bias=True)
        nn.init.xavier_uniform_(_final_conv.weight)
        nn.init.zeros_(_final_conv.bias)
        self._logits = nn.Sequential(
            nn.ZeroPad2d(same_padding(in_size, 1, 1)[0]), _final_conv
        )

        self._values = nn.Linear(in_depth, 1)
        # Mimick old API stack behavior of initializing the value function with `normc`
        # std=0.01.
        normc_initializer(0.01)(self._values.weight)

    @override(TorchRLModule)
    def _forward_inference(self, batch, **kwargs):
        # Compute the basic 1D feature tensor (inputs to policy- and value-heads).
        _, logits = self._compute_features_and_logits(batch)
        # Return logits as ACTION_DIST_INPUTS (categorical distribution).
        return {Columns.ACTION_DIST_INPUTS: logits}

    @override(TorchRLModule)
    def _forward_exploration(self, batch, **kwargs):
        return self._forward_inference(batch, **kwargs)

    @override(TorchRLModule)
    def _forward_train(self, batch, **kwargs):
        # Compute the basic 1D feature tensor (inputs to policy- and value-heads).
        features, logits = self._compute_features_and_logits(batch)
        # Besides the action logits, we also have to return value predictions here
        # (to be used inside the loss function).
        values = self._values(features).squeeze(-1)
        return {
            Columns.ACTION_DIST_INPUTS: logits,
            Columns.VF_PREDS: values,
        }

    # We implement this RLModule as a ValueFunctionAPI RLModule, so it can be used
    # by value-based methods like PPO or IMPALA.
    @override(ValueFunctionAPI)
    def compute_values(self, batch: Dict[str, Any]) -> TensorType:
        obs = batch[Columns.OBS]
        features = self._base_cnn_stack(obs.permute(0, 3, 1, 2))
        features = torch.squeeze(features, dim=[-1, -2])
        return self._values(features).squeeze(-1)

    def _compute_features_and_logits(self, batch):
        obs = batch[Columns.OBS].permute(0, 3, 1, 2)
        features = self._base_cnn_stack(obs)
        logits = self._logits(features)
        return (
            torch.squeeze(features, dim=[-1, -2]),
            torch.squeeze(logits, dim=[-1, -2]),
        )

    # TODO (sven): In order for this RLModule to work with PPO, we must define
    #  our own `get_..._action_dist_cls()` methods. This would become more obvious,
    #  if we simply subclassed the `PPOTorchRLModule` directly here (which we didn't do
    #  for simplicity and to keep some generality). We might even get rid of algo-
    #  specific RLModule subclasses altogether in the future and replace them
    #  by mere algo-specific APIs (w/o any actual implementations).
    @override(RLModule)
    def get_train_action_dist_cls(self):
        return TorchCategorical

    @override(RLModule)
    def get_exploration_action_dist_cls(self):
        return TorchCategorical

    @override(RLModule)
    def get_inference_action_dist_cls(self):
        return TorchCategorical
