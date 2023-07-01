"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import gymnasium as gym

from ray.rllib.algorithms.dreamerv3.torch.models.components.mlp import MLP
from ray.rllib.algorithms.dreamerv3.utils import get_gru_units, get_dense_hidden_units
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class SequenceModel(nn.Module):
    """The "sequence model" of the RSSM, computing ht+1 given (ht, zt, at).

    Note: The "internal state" always consists of:
    The actions `a` (initially, this is a zeroed-out action), `h`-states (deterministic,
    continuous), and `z`-states (stochastic, discrete).
    There are two versions of z-states: "posterior" for world model training and "prior"
    for creating the dream data.

    Initial internal state values (`a`, `h`, and `z`) are used where ever a new episode
    starts within a batch row OR at the beginning of each train batch's B rows,
    regardless of whether there was an actual episode boundary or not. Thus, internal
    states are not required to be stored in or retrieved from the replay buffer AND
    retrieved batches from the buffer must not be zero padded.

    Initial `a` is the zero "one hot" action, e.g. [0.0, 0.0] for Discrete(2), initial
    `h` is a separate learned variable, and initial `z` are computed by the "dynamics"
    (or "prior") net, using only the initial-h state as input.

    The GRU in this SequenceModel always produces the next h-state, then.
    """

    def __init__(
        self,
        *,
        input_size: int,
        model_size: str = "XS",
        action_space: gym.Space,
        num_gru_units: Optional[int] = None,
    ):
        """Initializes a SequenceModel instance.

        Args:
            input_size: The input size of the pre-layer (Dense) of the sequence model.
            model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the number of GRU units used.
            action_space: The action space the our environment used.
            num_gru_units: Overrides the number of GRU units (dimension of the h-state).
                If None, use the value given through `model_size`
                (see [1] Appendix B).
        """
        super().__init__()

        num_gru_units = get_gru_units(model_size, override=num_gru_units)
        self.action_space = action_space

        # In Danijar's code, there is an additional layer (units=[model_size])
        # prior to the GRU (but always only with 1 layer), which is not mentioned in
        # the paper.
        self.pre_gru_layer = MLP(
            input_size=input_size,
            num_dense_layers=1,
            model_size=model_size,
            output_layer_size=None,
        )
        gru_input_size = get_dense_hidden_units(model_size)
        self.gru_unit = nn.GRU(
            input_size=gru_input_size,
            hidden_size=num_gru_units,
            batch_first=False,  # time major
        )

    def forward(self, a, h, z):
        """

        Args:
            a: The previous action (already one-hot'd if applicable). (B, ...).
            h: The previous deterministic hidden state of the sequence model.
                (B, num_gru_units)
            z: The previous stochastic discrete representations of the original
                observation input. (B, num_categoricals, num_classes_per_categorical).
        """
        # Flatten last two dims of z.
        z_shape = z.shape
        z = z.view(z_shape[0], -1)
        out = torch.cat([z, a], dim=-1)
        # Pass through pre-GRU layer.
        out = self.pre_gru_layer(out)
        # Pass through GRU.
        h_next, _ = self.gru_unit(out.unsqueeze(0), h.unsqueeze(0))
        h_next = h_next.squeeze(0)  # Remove time dimension
        # Return the GRU's output (the next h-state).
        return h_next
