import numpy as np
from typing import Dict, List
import gym

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.misc import normc_initializer, same_padding, \
    SlimConv2d, SlimFC
from ray.rllib.models.utils import get_filter_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModelConfigDict, TensorType

_, nn = try_import_torch()


class VisionNetwork(TorchModelV2, nn.Module):
    """Generic vision network."""

    def __init__(self, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, num_outputs: int,
                 model_config: ModelConfigDict, name: str):

        if not model_config.get("conv_filters"):
            model_config["conv_filters"] = get_filter_config(obs_space.shape)

        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        activation = self.model_config.get("conv_activation")
        filters = self.model_config["conv_filters"]
        assert len(filters) > 0,\
            "Must provide at least 1 entry in `conv_filters`!"
        no_final_linear = self.model_config.get("no_final_linear")
        vf_share_layers = self.model_config.get("vf_share_layers")

        # Whether the last layer is the output of a Flattened (rather than
        # a n x (1,1) Conv2D).
        self.last_layer_is_flattened = False
        self._logits = None
        self.traj_view_framestacking = False

        layers = []
        # Perform Atari framestacking via traj. view API.
        if model_config.get("num_framestacks") != "auto" and \
                model_config.get("num_framestacks", 0) > 1:
            (w, h) = obs_space.shape
            in_channels = model_config["num_framestacks"]
            self.traj_view_framestacking = True
        else:
            (w, h, in_channels) = obs_space.shape

        in_size = [w, h]
        for out_channels, kernel, stride in filters[:-1]:
            padding, out_size = same_padding(in_size, kernel, [stride, stride])
            layers.append(
                SlimConv2d(
                    in_channels,
                    out_channels,
                    kernel,
                    stride,
                    padding,
                    activation_fn=activation))
            in_channels = out_channels
            in_size = out_size

        out_channels, kernel, stride = filters[-1]

        # No final linear: Last layer is a Conv2D and uses num_outputs.
        if no_final_linear and num_outputs:
            layers.append(
                SlimConv2d(
                    in_channels,
                    num_outputs,
                    kernel,
                    stride,
                    None,  # padding=valid
                    activation_fn=activation))
            out_channels = num_outputs
        # Finish network normally (w/o overriding last layer size with
        # `num_outputs`), then add another linear one of size `num_outputs`.
        else:
            layers.append(
                SlimConv2d(
                    in_channels,
                    out_channels,
                    kernel,
                    stride,
                    None,  # padding=valid
                    activation_fn=activation))

            # num_outputs defined. Use that to create an exact
            # `num_output`-sized (1,1)-Conv2D.
            if num_outputs:
                in_size = [
                    np.ceil((in_size[0] - kernel[0]) / stride),
                    np.ceil((in_size[1] - kernel[1]) / stride)
                ]
                padding, _ = same_padding(in_size, [1, 1], [1, 1])
                self._logits = SlimConv2d(
                    out_channels,
                    num_outputs, [1, 1],
                    1,
                    padding,
                    activation_fn=None)
            # num_outputs not known -> Flatten, then set self.num_outputs
            # to the resulting number of nodes.
            else:
                self.last_layer_is_flattened = True
                layers.append(nn.Flatten())
                self.num_outputs = out_channels

        self._convs = nn.Sequential(*layers)

        # Build the value layers
        self._value_branch_separate = self._value_branch = None
        if vf_share_layers:
            self._value_branch = SlimFC(
                out_channels,
                1,
                initializer=normc_initializer(0.01),
                activation_fn=None)
        else:
            vf_layers = []
            if self.traj_view_framestacking:
                (w, h) = obs_space.shape
                in_channels = model_config["num_framestacks"]
            else:
                (w, h, in_channels) = obs_space.shape
            in_size = [w, h]
            for out_channels, kernel, stride in filters[:-1]:
                padding, out_size = same_padding(in_size, kernel,
                                                 [stride, stride])
                vf_layers.append(
                    SlimConv2d(
                        in_channels,
                        out_channels,
                        kernel,
                        stride,
                        padding,
                        activation_fn=activation))
                in_channels = out_channels
                in_size = out_size

            out_channels, kernel, stride = filters[-1]
            vf_layers.append(
                SlimConv2d(
                    in_channels,
                    out_channels,
                    kernel,
                    stride,
                    None,
                    activation_fn=activation))

            vf_layers.append(
                SlimConv2d(
                    in_channels=out_channels,
                    out_channels=1,
                    kernel=1,
                    stride=1,
                    padding=None,
                    activation_fn=None))
            self._value_branch_separate = nn.Sequential(*vf_layers)

        # Holds the current "base" output (before logits layer).
        self._features = None

        # Optional: framestacking obs/new_obs for Atari.
        if self.traj_view_framestacking:
            from_ = model_config["num_framestacks"] - 1
            self.view_requirements[SampleBatch.OBS].shift = \
                "-{}:0".format(from_)
            self.view_requirements[SampleBatch.OBS].shift_from = -from_
            self.view_requirements[SampleBatch.OBS].shift_to = 0
            self.view_requirements[SampleBatch.NEXT_OBS] = ViewRequirement(
                data_col=SampleBatch.OBS,
                shift="-{}:1".format(from_ - 1),
                space=self.view_requirements[SampleBatch.OBS].space,
            )

    @override(TorchModelV2)
    def forward(self, input_dict: Dict[str, TensorType],
                state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
        self._features = input_dict["obs"].float()
        # No framestacking:
        if not self.traj_view_framestacking:
            self._features = self._features.permute(0, 3, 1, 2)
        conv_out = self._convs(self._features)
        # Store features to save forward pass when getting value_function out.
        if not self._value_branch_separate:
            self._features = conv_out

        if not self.last_layer_is_flattened:
            if self._logits:
                conv_out = self._logits(conv_out)
            if conv_out.shape[2] != 1 or conv_out.shape[3] != 1:
                raise ValueError(
                    "Given `conv_filters` ({}) do not result in a [B, {} "
                    "(`num_outputs`), 1, 1] shape (but in {})! Please adjust "
                    "your Conv2D stack such that the last 2 dims are both "
                    "1.".format(self.model_config["conv_filters"],
                                self.num_outputs, list(conv_out.shape)))
            logits = conv_out.squeeze(3)
            logits = logits.squeeze(2)

            return logits, state
        else:
            return conv_out, state

    @override(TorchModelV2)
    def value_function(self) -> TensorType:
        assert self._features is not None, "must call forward() first"
        if self._value_branch_separate:
            value = self._value_branch_separate(self._features)
            value = value.squeeze(3)
            value = value.squeeze(2)
            return value.squeeze(1)
        else:
            if not self.last_layer_is_flattened:
                features = self._features.squeeze(3)
                features = features.squeeze(2)
            else:
                features = self._features
            return self._value_branch(features).squeeze(1)

    def _hidden_layers(self, obs: TensorType) -> TensorType:
        res = self._convs(obs.permute(0, 3, 1, 2))  # switch to channel-major
        res = res.squeeze(3)
        res = res.squeeze(2)
        return res
