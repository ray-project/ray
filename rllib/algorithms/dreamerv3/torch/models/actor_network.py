"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.dreamerv3.torch.models.components.mlp import MLP
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class ActorNetwork(nn.Module):
    """The `actor` (policy net) of DreamerV3.

    Consists of a simple MLP for Discrete actions and two MLPs for cont. actions (mean
    and stddev).
    Also contains two scalar variables to keep track of the percentile-5 and
    percentile-95 values of the computed value targets within a batch. This is used to
    compute the "scaled value targets" for actor learning. These two variables decay
    over time exponentially (see [1] for more details).
    """

    def __init__(
        self,
        *,
        input_size: int,
        model_size: str = "XS",
        action_space: gym.Space,
    ):
        """Initializes an ActorNetwork instance.

        Args:
            input_size: The input size of the actor network.
            model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different network sizes.
            action_space: The action space the our environment used.
        """
        super().__init__()

        self.input_size = input_size
        self.model_size = model_size
        self.action_space = action_space

        # The EMA decay variables used for the [Percentile(R, 95%) - Percentile(R, 5%)]
        # diff to scale value targets for the actor loss.
        self.ema_value_target_pct5 = nn.Parameter(
            torch.tensor(float("nan")), requires_grad=False
        )
        self.ema_value_target_pct95 = nn.Parameter(
            torch.tensor(float("nan")), requires_grad=False
        )

        # For discrete actions, use a single MLP that computes logits.
        if isinstance(self.action_space, gym.spaces.Discrete):
            self.mlp = MLP(
                input_size=self.input_size,
                model_size=self.model_size,
                output_layer_size=self.action_space.n,
            )
        # For cont. actions, use separate MLPs for Gaussian mean and stddev.
        # TODO (sven): In the author's original code repo, this is NOT the case,
        #  inputs are pushed through a shared MLP, then only the two output linear
        #  layers are separate for std- and mean logits.
        elif isinstance(action_space, gym.spaces.Box):
            output_layer_size = np.prod(action_space.shape)
            self.mlp = MLP(
                input_size=self.input_size,
                model_size=self.model_size,
                output_layer_size=output_layer_size,
            )
            self.std_mlp = MLP(
                input_size=self.input_size,
                model_size=self.model_size,
                output_layer_size=output_layer_size,
            )
        else:
            raise ValueError(f"Invalid action space: {action_space}")

    def forward(self, h, z, return_distr_params=False):
        """Performs a forward pass through this policy network.

        Args:
            h: The deterministic hidden state of the sequence model. [B, dim(h)].
            z: The stochastic discrete representations of the original
                observation input. [B, num_categoricals, num_classes].
            return_distr_params: Whether to return (as a second tuple item) the action
                distribution parameter tensor created by the policy.
        """
        # Flatten last two dims of z.
        assert len(z.shape) == 3
        z_shape = z.shape
        z = z.view(z_shape[0], -1)
        assert len(z.shape) == 2
        out = torch.cat([h, z], dim=-1)
        # Send h-cat-z through MLP.
        action_logits = self.mlp(out)

        if isinstance(self.action_space, gym.spaces.Discrete):
            action_probs = nn.functional.softmax(action_logits, dim=-1)

            # Add the unimix weighting (1% uniform) to the probs.
            # See [1]: "Unimix categoricals: We parameterize the categorical
            # distributions for the world model representations and dynamics, as well as
            # for the actor network, as mixtures of 1% uniform and 99% neural network
            # output to ensure a minimal amount of probability mass on every class and
            # thus keep log probabilities and KL divergences well behaved."
            action_probs = 0.99 * action_probs + 0.01 * (1.0 / self.action_space.n)

            # Danijar's code does: distr = [Distr class](logits=torch.log(probs)).
            # Not sure why we don't directly use the already available probs instead.
            action_logits = torch.log(action_probs)

            # Distribution parameters are the log(probs) directly.
            distr_params = action_logits
            distr = self.get_action_dist_object(distr_params)

            action = distr.sample().float().detach() + (
                action_probs - action_probs.detach()
            )

        elif isinstance(self.action_space, gym.spaces.Box):
            # Send h-cat-z through MLP to compute stddev logits for Normal dist
            std_logits = self.std_mlp(out)
            # minstd, maxstd taken from [1] from configs.yaml
            minstd = 0.1
            maxstd = 1.0

            # Distribution parameters are the squashed std_logits and the tanh'd
            # mean logits.
            # squash std_logits from (-inf, inf) to (minstd, maxstd)
            std_logits = (maxstd - minstd) * torch.sigmoid(std_logits + 2.0) + minstd
            mean_logits = torch.tanh(action_logits)

            distr_params = torch.cat([mean_logits, std_logits], dim=-1)
            distr = self.get_action_dist_object(distr_params)

            action = distr.rsample()

        if return_distr_params:
            return action, distr_params
        return action

    def get_action_dist_object(self, action_dist_params_T_B):
        """Helper method to create an action distribution object from (T, B, ..) params.

        Args:
            action_dist_params_T_B: The time-major action distribution parameters.
                This could be simply the logits (discrete) or a to-be-split-in-2
                tensor for mean and stddev (continuous).

        Returns:
            The torch action distribution object, from which one can sample, compute
            log probs, entropy, etc..
        """
        if isinstance(self.action_space, gym.spaces.Discrete):
            # Create the distribution object using the unimix'd logits.
            distr = torch.distributions.OneHotCategorical(logits=action_dist_params_T_B)

        elif isinstance(self.action_space, gym.spaces.Box):
            # Compute Normal distribution from action_logits and std_logits
            loc, scale = torch.split(
                action_dist_params_T_B,
                action_dist_params_T_B.shape[-1] // 2,
                dim=-1,
            )
            distr = torch.distributions.Normal(loc=loc, scale=scale)

            # If action_space is a box with multiple dims, make individual dims
            # independent.
            distr = torch.distributions.Independent(distr, len(self.action_space.shape))

        else:
            raise ValueError(f"Action space {self.action_space} not supported!")

        return distr
