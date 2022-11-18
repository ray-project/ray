from typing import TYPE_CHECKING, List

import torch
from torch import nn

from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.torch.model import TorchModel
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.nested_dict import NestedDict

if TYPE_CHECKING:
    from ray.rllib.models.configs.encoder import VectorEncoderConfig


def input_to_output_spec(
    input_spec: ModelSpec, output_key: str, feature_dims: List[int]
) -> ModelSpec:
    """Convert an input spec to an output spec by replacing the input dims and
    key with the output dims and output key.

    E.g.
    input_to_output_spec(
        input_spec=ModelSpec({
            "bork": "a, b, c, d",
            "dork": "e, f, g, h"
            }, h=2, d=3
        ),
        output_key="foo",
        feature_dims=(2,)
    )

    should return:
    ModelSpec({"foo": "a, b, c, d"}, d=2)

    Args:
        input_spec: The input spec for the model
        output_key: The key that the model will place the outputs under in
            the nested dict
        feature_dims: A list denoting the features dimensions, e.g. [-1] or [3, 4]
    """
    num_feat_dims = len(feature_dims)
    assert num_feat_dims >= 1, "Must specify at least one feature dim"
    num_dims = [len(v.shape) != len for v in input_spec.values()]
    assert all(
        [nd == num_dims[0] for nd in num_dims]
    ), "Inputs must all have the same number of dimensions"

    # All keys in input should have the same numbers of dims
    # so it doesn't matter which key we use
    key = list(input_spec.keys())[0]
    tspec = input_spec[key].rdrop(len(feature_dims)).append(feature_dims)
    return ModelSpec({output_key: tspec})


class TorchVectorEncoder(TorchModel):
    """An MLP encoder. This encoder concatenates inputs along the last dimension,
    then pushes them through an MLP.

    """

    @property
    def input_spec(self) -> ModelSpec:
        return self._input_spec

    @property
    def output_spec(self) -> ModelSpec:
        return self._output_spec

    def __init__(
        self,
        input_spec: ModelSpec,
        config: "VectorEncoderConfig",
    ):
        super().__init__(config=config)
        # Setup input and output specs
        self._input_spec = input_spec
        self._output_spec = input_to_output_spec(
            input_spec, config.output_key, config.hidden_layer_sizes[-1:]
        )
        # Returns the size of the feature dimension for the input tensors
        prev_size = sum([v.shape[-1] for v in input_spec.values()])

        # Construct layers
        layers = []
        act = (
            None
            if config.activation == "linear"
            else get_activation_fn(config.activation, framework=config.framework_str)()
        )
        for size in config.hidden_layer_sizes[:-1]:
            layers += [nn.Linear(prev_size, size)]
            layers += [act] if act is not None else []
            prev_size = size

        # Final layer
        layers += [
            nn.Linear(config.hidden_layer_sizes[-2], config.hidden_layer_sizes[-1])
        ]
        if config.final_activation != "linear":
            layers += [
                get_activation_fn(
                    config.final_activation, framework=config.framework_str
                )()
            ]

        self.net = nn.Sequential(*layers)

    def _forward(self, inputs: NestedDict) -> NestedDict:
        """Runs the forward pass of the MLP. Call this via unroll().

        Args:
            inputs: The nested dictionary of inputs
        Returns:
            The nested dictionary of outputs
        """
        # Ensure all inputs have matching dims before concat
        # so we can emit an informative error message
        first_key, first_tensor = list(inputs.items())[0]
        for k, tensor in inputs.items():
            assert tensor.shape[:-1] == first_tensor.shape[:-1], (
                "Inputs have mismatching dimensions, all dims but the last should "
                f"be equal: {first_key}: {first_tensor.shape} != {k}: {tensor.shape}"
            )

        # Concatenate all input along the feature dim
        x = torch.cat(list(inputs.values()), dim=-1)
        [out_key] = self.output_spec.keys()
        inputs[out_key] = self.net(x)
        return inputs
