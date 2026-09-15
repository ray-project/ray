"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from ray.rllib.algorithms.dreamerv3.torch.models.components.mlp import MLP
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class ContinuePredictor(nn.Module):
    """The world-model network sub-component used to predict the `continue` flags .

    Predicted continue flags are used to produce "dream data" to learn the policy in.

    The continue flags are predicted via a linear output used to parameterize a
    Bernoulli distribution, from which simply the mode is used (no stochastic
    sampling!). In other words, if the sigmoid of the output of the linear layer is
    >0.5, we predict a continuation of the episode, otherwise we predict an episode
    terminal.
    """

    def __init__(self, *, input_size: int, model_size: str = "XS"):
        """Initializes a ContinuePredictor instance.

        Args:
            input_size: The input size of the continue predictor.
            model_size: The "Model Size" used according to [1] Appendinx B.
                Determines the exact size of the underlying MLP.
        """
        super().__init__()

        self.mlp = MLP(
            input_size=input_size,
            model_size=model_size,
            output_layer_size=1,
        )

    def forward(self, h, z, return_distribution=False):
        """Performs a forward pass through the continue predictor.

        Args:
            h: The deterministic hidden state of the sequence model. [B, dim(h)].
            z: The stochastic discrete representations of the original
                observation input. [B, num_categoricals, num_classes].
            return_distribution: Whether to return (as a second tuple item) the
                Bernoulli distribution object created by the underlying MLP.
        """
        z_shape = z.size()
        z = z.view(z_shape[0], -1)

        out = torch.cat([h, z], dim=-1)
        out = self.mlp(out)
        logits = out.squeeze(dim=-1)
        bernoulli = torch.distributions.Bernoulli(logits=logits)
        # Use the mode of the Bernoulli distribution (greedy, deterministic "sample").
        continue_ = bernoulli.probs > 0.5

        if return_distribution:
            return continue_, bernoulli
        return continue_
