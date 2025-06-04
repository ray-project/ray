from typing import Dict

import gymnasium as gym

from ray.rllib.core import Columns
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDiagGaussian,
    TorchMultiDistribution,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import one_hot
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class AutoregressiveActionsRLM(TorchRLModule, ValueFunctionAPI):
    """An RLModule that uses an autoregressive action distribution.

    Actions are sampled in two steps. The first (prior) action component is sampled from
    a categorical distribution. Then, the second (posterior) action component is sampled
    from a posterior distribution that depends on the first action component and the
    other input data (observations).

    Note, this RLModule works in combination with any algorithm, whose Learners require
    the `ValueFunctionAPI`.
    """

    @override(RLModule)
    def setup(self):
        super().setup()

        # Assert the action space is correct.
        assert isinstance(self.action_space, gym.spaces.Tuple)
        assert isinstance(self.action_space[0], gym.spaces.Discrete)
        assert self.action_space[0].n == 3
        assert isinstance(self.action_space[1], gym.spaces.Box)

        self._prior_net = nn.Sequential(
            nn.Linear(
                in_features=self.observation_space.shape[0],
                out_features=256,
            ),
            nn.Tanh(),
            nn.Linear(in_features=256, out_features=self.action_space[0].n),
        )

        self._posterior_net = nn.Sequential(
            nn.Linear(
                in_features=self.observation_space.shape[0] + self.action_space[0].n,
                out_features=256,
            ),
            nn.Tanh(),
            nn.Linear(in_features=256, out_features=self.action_space[1].shape[0] * 2),
        )

        # Build the value function head.
        self._value_net = nn.Sequential(
            nn.Linear(
                in_features=self.observation_space.shape[0],
                out_features=256,
            ),
            nn.Tanh(),
            nn.Linear(in_features=256, out_features=1),
        )

    @override(TorchRLModule)
    def _forward_inference(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        return self._pi(batch[Columns.OBS], inference=True)

    @override(TorchRLModule)
    def _forward_exploration(
        self, batch: Dict[str, TensorType], **kwargs
    ) -> Dict[str, TensorType]:
        return self._pi(batch[Columns.OBS], inference=False)

    @override(TorchRLModule)
    def _forward_train(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        return self._forward_exploration(batch)

    @override(ValueFunctionAPI)
    def compute_values(self, batch: Dict[str, TensorType], embeddings=None):
        # Value function forward pass.
        vf_out = self._value_net(batch[Columns.OBS])
        # Squeeze out last dimension (single node value head).
        return vf_out.squeeze(-1)

    # __sphinx_begin__
    def _pi(self, obs, inference: bool):
        # Prior forward pass and sample a1.
        prior_out = self._prior_net(obs)
        dist_a1 = TorchCategorical.from_logits(prior_out)
        if inference:
            dist_a1 = dist_a1.to_deterministic()
        a1 = dist_a1.sample()

        # Posterior forward pass and sample a2.
        posterior_batch = torch.cat(
            [obs, one_hot(a1, self.action_space[0])],
            dim=-1,
        )
        posterior_out = self._posterior_net(posterior_batch)
        dist_a2 = TorchDiagGaussian.from_logits(posterior_out)
        if inference:
            dist_a2 = dist_a2.to_deterministic()
        a2 = dist_a2.sample()
        actions = (a1, a2)

        # We need logp and distribution parameters for the loss.
        return {
            Columns.ACTION_LOGP: (
                TorchMultiDistribution((dist_a1, dist_a2)).logp(actions)
            ),
            Columns.ACTION_DIST_INPUTS: torch.cat([prior_out, posterior_out], dim=-1),
            Columns.ACTIONS: actions,
        }
        # __sphinx_end__

    @override(TorchRLModule)
    def get_inference_action_dist_cls(self):
        return TorchMultiDistribution.get_partial_dist_cls(
            child_distribution_cls_struct=(TorchCategorical, TorchDiagGaussian),
            input_lens=(3, 2),
        )
