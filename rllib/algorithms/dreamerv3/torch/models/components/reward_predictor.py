"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from ray.rllib.algorithms.dreamerv3.torch.models.components.mlp import MLP
from ray.rllib.algorithms.dreamerv3.torch.models.components import (
    reward_predictor_layer,
)
from ray.rllib.algorithms.dreamerv3.utils import get_dense_hidden_units

from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class RewardPredictor(nn.Module):
    """Wrapper of MLP and RewardPredictorLayer to predict rewards for the world model.

    Predicted rewards are used to produce "dream data" to learn the policy in.
    """

    def __init__(
        self,
        *,
        input_size: int,
        model_size: str = "XS",
        num_buckets: int = 255,
        lower_bound: float = -20.0,
        upper_bound: float = 20.0,
    ):
        """Initializes a RewardPredictor instance.

        Args:
            input_size: The input size of the reward predictor.
            model_size: The "Model Size" used according to [1] Appendinx B.
                Determines the exact size of the underlying MLP.
            num_buckets: The number of buckets to create. Note that the number of
                possible symlog'd outcomes from the used distribution is
                `num_buckets` + 1:
                lower_bound --bucket-- o[1] --bucket-- o[2] ... --bucket-- upper_bound
                o=outcomes
                lower_bound=o[0]
                upper_bound=o[num_buckets]
            lower_bound: The symlog'd lower bound for a possible reward value.
                Note that a value of -20.0 here already allows individual (actual env)
                rewards to be as low as -400M. Buckets will be created between
                `lower_bound` and `upper_bound`.
            upper_bound: The symlog'd upper bound for a possible reward value.
                Note that a value of +20.0 here already allows individual (actual env)
                rewards to be as high as 400M. Buckets will be created between
                `lower_bound` and `upper_bound`.
        """
        super().__init__()

        self.mlp = MLP(
            input_size=input_size,
            model_size=model_size,
            output_layer_size=None,
        )
        reward_predictor_input_size = get_dense_hidden_units(model_size)
        self.reward_layer = reward_predictor_layer.RewardPredictorLayer(
            input_size=reward_predictor_input_size,
            num_buckets=num_buckets,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
        )

    def forward(self, h, z, return_logits=False):
        """Computes the expected reward using N equal sized buckets of possible values.

        Args:
            h: The deterministic hidden state of the sequence model. [B, dim(h)].
            z: The stochastic discrete representations of the original
                observation input. [B, num_categoricals, num_classes].
            return_logits: Whether to return the logits over the reward buckets
                as a second return value (besides the expected reward).
        """
        # Flatten last two dims of z.
        z_shape = z.shape
        z = z.view(z_shape[0], -1)
        out = torch.cat([h, z], dim=-1)
        # Send h-cat-z through MLP.
        out = self.mlp(out)
        # Return a) mean reward OR b) a tuple: (mean reward, logits over the reward
        # buckets).
        return self.reward_layer(out, return_logits=return_logits)
