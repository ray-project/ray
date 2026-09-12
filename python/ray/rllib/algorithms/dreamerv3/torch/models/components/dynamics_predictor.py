"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

from ray.rllib.algorithms.dreamerv3.torch.models.components import (
    representation_layer,
)
from ray.rllib.algorithms.dreamerv3.torch.models.components.mlp import MLP
from ray.rllib.algorithms.dreamerv3.utils import get_dense_hidden_units
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class DynamicsPredictor(nn.Module):
    """The dynamics (or "prior") network described in [1], producing prior z-states.

    The dynamics net is used to:
    - compute the initial z-state (from the tanh'd initial h-state variable) at the
    beginning of a sequence.
    - compute prior-z-states during dream data generation. Note that during dreaming,
    no actual observations are available and thus no posterior z-states can be computed.
    """

    def __init__(
        self,
        *,
        input_size: int,
        model_size: str = "XS",
        num_categoricals: Optional[int] = None,
        num_classes_per_categorical: Optional[int] = None,
    ):
        """Initializes a DynamicsPredictor instance.

        Args:
            input_size: The input size of the dynamics predictor.
            model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different parameters.
            num_categoricals: Overrides the number of categoricals used in the z-states.
                In [1], 32 is used for any model size.
            num_classes_per_categorical: Overrides the number of classes within each
                categorical used for the z-states. In [1], 32 is used for any model
                dimension.
        """
        super().__init__()

        self.mlp = MLP(
            input_size=input_size,
            num_dense_layers=1,
            model_size=model_size,
            output_layer_size=None,
        )
        representation_layer_input_size = get_dense_hidden_units(model_size)
        self.representation_layer = representation_layer.RepresentationLayer(
            input_size=representation_layer_input_size,
            model_size=model_size,
            num_categoricals=num_categoricals,
            num_classes_per_categorical=num_classes_per_categorical,
        )

    def forward(self, h, return_z_probs=False):
        """Performs a forward pass through the dynamics (or "prior") network.

        Args:
            h: The deterministic hidden state of the sequence model.
            return_z_probs: Whether to return the probabilities for the categorical
                distribution (in the shape of [B, num_categoricals, num_classes])
                as a second return value.
        """
        out = self.mlp(h)
        return self.representation_layer(out, return_z_probs=return_z_probs)
