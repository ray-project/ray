from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.torch.misc import normc_initializer
from ray.rllib.models.torch.misc import same_padding, valid_padding
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TinyAtariCNN(TorchRLModule):
    """A tiny CNN stack for fast-learning of Atari envs.

    The architecture here is the exact same as the one used by the old API stack as
    CNN default ModelV2.

    We stack 3 CNN layers based on the config, then a 4th one with linear activation
    and n 1x1 filters, where n is the number of actions in the (discrete) action space.
    Simple reshaping (no flattening or extra linear layers necessary) lead to the
    action logits, which can directly be used inside a distribution or loss.
    """
    @override(TorchRLModule)
    def setup(self):
        # Define the layers that this CNN stack needs.
        conv_filters = [
            [16, 4, 2],  # num filters, kernel wxh, stride wxh
            [32, 4, 2],
            [256, 11, 1, "valid"],  # , ... , [padding type]
        ]

        # Build the CNN layers.
        layers = []

        # Add user-specified hidden convolutional layers first
        width, height, in_depth = self.config.observation_space.shape
        in_size = [width, height]
        for filter_specs in conv_filters:
            # Padding information not provided -> Use "same" as default.
            if len(filter_specs) == 3:
                out_depth, kernel_size, strides = filter_specs
                padding = "same"
            # Padding information provided.
            else:
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
            # Initialize CNN layer kernel.
            nn.init.xavier_uniform_(layer.weight)
            # Initialize CNN layer bias.
            nn.init.zeros_(layer.bias)

            layers.append(layer)

            # Activation.
            layers.append(nn.ReLU())

            in_size = out_size
            in_depth = out_depth

        self._base_cnn_stack = nn.Sequential(*layers)

        # Add the final CNN 1x1 layer with num_filters == num_actions to be reshaped to
        # yield the logits (no flattening, no additional linear layers required).
        self._logits = nn.Sequential(
            nn.ZeroPad2d(same_padding(in_size, 1, 1)[0]),
            nn.Conv2d(in_depth, self.config.action_space.n, 1, 1, bias=True),
        )
        self._values = nn.Linear(in_depth, 1)
        # Mimick old API stack behavior of initializing the value function with `normc`
        # std=0.01.
        normc_initializer(0.01)(self._values.weight)

    @override(TorchRLModule)
    def _forward_inference(self, batch, **kwargs):
        _, logits = self._compute_features_and_logits(batch)
        return {
            Columns.ACTION_DIST_INPUTS: logits
        }

    @override(TorchRLModule)
    def _forward_exploration(self, batch, **kwargs):
        return self._forward_inference(batch, **kwargs)

    @override(TorchRLModule)
    def _forward_train(self, batch, **kwargs):
        features, logits = self._compute_features_and_logits(batch)
        # Besides the action logits, we also have to return value predictions here
        # (to be used inside the loss function).
        vf = self._values(features)
        return {
            Columns.ACTION_DIST_INPUTS: logits,
            Columns.VF_PREDS: vf.squeeze(-1),
        }

    def _compute_features_and_logits(self, batch):
        obs = batch[Columns.OBS].permute(0, 3, 1, 2)
        features = self._base_cnn_stack(obs)
        logits = self._logits(features)
        return torch.squeeze(features, dim=[-1, -2]), torch.squeeze(logits, dim=[-1, -2])


if __name__ == "__main__":
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