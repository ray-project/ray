import numpy as np

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.base import ModelConfig
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.torch.primitives import TorchCNN
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.misc import SlimConv2d
from ray.rllib.models.torch.misc import same_padding
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchCNNModel(TorchModel, nn.Module):
    """A model containing a CNN."""

    def __init__(self, config: ModelConfig) -> None:
        nn.Module.__init__(self)
        TorchModel.__init__(self, config)

        assert len(self.config.input_dims) == 3
        assert isinstance(self.config.output_dim, int)

        layers = []

        filter_layer_activation = self.config.filter_layer_activation
        output_dim = self.config.output_dim
        filter_specifiers = self.config.filter_specifiers

        if output_dim is not None:
            # In this case we want to append a final layer after the core CNN that
            # will have output_activation
            core_output_activation = filter_layer_activation
        else:
            core_output_activation = self.config.output_activation

        # Create the core CNN.
        core_cnn = TorchCNN(
            input_dims=self.config.input_dims,
            filter_specifiers=filter_specifiers,
            filter_layer_activation=self.config.filter_layer_activation,
            output_activation=core_output_activation,
        )
        layers.append(core_cnn)

        if output_dim is not None:
            # Append a last layer to the CNN and flatten it.

            # Get some number of the last layer of core_cnn.
            width, height, out_depth = core_cnn.out_dims
            kernel = core_cnn.last_kernel
            stride = core_cnn.last_stride

            in_size = (
                int(np.ceil((width - kernel[0]) / stride)),
                int(np.ceil((height - kernel[1]) / stride)),
            )
            padding, _ = same_padding(in_size, (1, 1), (1, 1))
            # TODO(Artur): Inline SlimConv2d or use TorchCNN here
            layers.append(
                SlimConv2d(
                    out_depth,
                    1,
                    (1, 1),
                    1,
                    padding,
                    activation_fn=core_output_activation,
                )
            )

            layers.append(nn.Flatten())
            # We always append a final linear layer to make sure that the outputs
            # have the correct dimensionality.
            layers.append(nn.Linear(int(width) * int(height), output_dim))
            self.output_dim = output_dim
        else:
            # If our output_dim still unknown, we need to do a test pass to
            # figure out the output dimensions.

            # Create a B=1 dummy sample and push it through our conv-net.
            dummy_in = (
                torch.from_numpy(np.array(self.config.input_dims))
                .permute(2, 0, 1)
                .unsqueeze(0)
                .float()
            )
            dummy_out = core_cnn(dummy_in)
            self.output_dim = dummy_out.shape[1]

        # Create the cnn that potentially includes a flattened layer
        self.cnn = nn.Sequential(*layers)

        self.input_spec = TorchTensorSpec(
            "b, w, h, d",
            w=self.config.input_dims[0],
            h=self.config.input_dims[1],
            d=self.config.input_dims[2],
        )
        self.output_spec = TorchTensorSpec("b, h", h=self.output_dim)

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        # Permute b/c data comes in as [B, dim, dim, channels]:
        inputs = inputs.permute(0, 3, 1, 2)
        return self.cnn(inputs)
