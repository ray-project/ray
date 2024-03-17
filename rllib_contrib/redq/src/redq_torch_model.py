import random
from typing import List, Optional, Tuple

import gymnasium as gym
import numpy as np
from gymnasium.spaces import Box, Discrete

from ray.rllib.algorithms.sac.sac_torch_model import SACTorchModel
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.typing import ModelConfigDict, TensorType

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


class REDQTorchModel(SACTorchModel):
    """Extension of the SACTorchModel for REDQ.

    To customize, do one of the following:
    - sub-class REDQTorchModel and override one or more of its methods.
    - Use REDQ's `q_model_config` and `policy_model` keys to tweak the default model
      behaviors (e.g. fcnet_hiddens, conv_filters, etc..).
    - Use REDQ's `q_model_config->custom_model` and `policy_model->custom_model` keys
      to specify your own custom Q-model(s) and policy-models, which will be
      created within this REDQTorchModel (see `build_policy_model` and
      `build_q_model`.

    Note: It is not recommended to override the `forward` method for REDQ. This
    would lead to shared weights (between policy and Q-nets), which will then
    not be optimized by either of the critic- or actor-optimizers!

    Data flow:
        `obs` -> forward() (should stay a noop method!) -> `model_out`
        `model_out` -> get_policy_output() -> pi(actions|obs)
        `model_out`, `actions` -> get_q_values() -> Q(s, a)
        `model_out`, `actions` -> get_twin_q_values() -> Q_twin(s, a)
    """

    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: Optional[int],
        model_config: ModelConfigDict,
        name: str,
        policy_model_config: ModelConfigDict = None,
        q_model_config: ModelConfigDict = None,
        initial_alpha: Optional[float] = 1.0,
        target_entropy: Optional[float] = None,
        ensemble_size: Optional[int] = 2,
        target_q_fcn_aggregator: Optional[str] = "min",
        q_fcn_aggregator: Optional[str] = "mean",
    ):
        """Initializes a REDQTorchModel instance.

        Args:
            policy_model_config: The config dict for the
                policy network.
            q_model_config: The config dict for the
                Q-network(s).
            initial_alpha (Optional[float]): The initial value for the to-be-optimized
                alpha parameter (default: 1.0).
            target_entropy (Optional[float]): A target entropy value for
                the to-be-optimized alpha parameter. If None, will use the
                defaults described in the papers for SAC (and discrete SAC).
            ensemble_size (Optional[int]): The number of models in the ensemble of
                models (default: 2).
            target_q_fcn_aggregator (Optional[str]): a string ('min' or 'mean')
                determining an aggregator function for target Q functions
                in target labels computations
            q_fcn_aggregator (Optional[str]): a string ('min' or 'mean')
                determining an aggregator function for Q functions
                in policy loss computation

        Note that the core layers for forward() are not defined here, this
        only defines the layers for the output heads. Those layers for
        forward() should be defined in subclasses of REDQModel.
        """
        nn.Module.__init__(self)
        super(SACTorchModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name
        )

        if isinstance(action_space, Discrete):
            self.action_dim = action_space.n
            self.discrete = True
            action_outs = q_outs = self.action_dim
        elif isinstance(action_space, Box):
            self.action_dim = np.product(action_space.shape)
            self.discrete = False
            action_outs = 2 * self.action_dim
            q_outs = 1
        else:
            assert isinstance(action_space, Simplex)
            self.action_dim = np.product(action_space.shape)
            self.discrete = False
            action_outs = self.action_dim
            q_outs = 1

        # Build the policy network.
        self.action_model = self.build_policy_model(
            self.obs_space, action_outs, policy_model_config, "policy_model"
        )

        # Build the list of Q-network(s).
        self.q_net_list = nn.ModuleList([])
        for idx in range(ensemble_size):
            self.q_net_list.add_module(
                f"Q_{idx}",
                self.build_q_model(
                    self.obs_space,
                    self.action_space,
                    q_outs,
                    q_model_config,
                    f"q_{idx}",
                ),
            )

        log_alpha = nn.Parameter(
            torch.from_numpy(np.array([np.log(initial_alpha)])).float()
        )
        self.register_parameter("log_alpha", log_alpha)

        # Auto-calculate the target entropy.
        if target_entropy is None or target_entropy == "auto":
            # We want the entorpy of the policy to be larger
            # than the entropy of \epsilon-greedy policy with \epsilon=0.975.
            # Then entropy is computed as below:
            if self.discrete:
                epsilon = 0.975
                target_entropy = -np.array(
                    # log probability of the greedy action
                    epsilon * np.log(epsilon) +
                    # log probability of random actions
                    (1 - epsilon) * np.log((1 - epsilon) / (action_space.n - 1)),
                    dtype=np.float32,
                )
            # See [1] (README.md).
            else:
                target_entropy = -np.prod(action_space.shape)

        target_entropy = nn.Parameter(
            torch.from_numpy(np.array([target_entropy])).float(), requires_grad=False
        )

        if target_q_fcn_aggregator == "min":
            self.target_q_fcn_aggregator = lambda x: torch.min(torch.stack(x), axis=0)[
                0
            ]
        else:
            self.target_q_fcn_aggregator = lambda x: getattr(
                torch, target_q_fcn_aggregator
            )(torch.stack(x), axis=0)

        if q_fcn_aggregator == "min":
            self.q_fcn_aggregator = lambda x: torch.min(torch.stack(x), axis=0)[0]
        else:
            self.q_fcn_aggregator = lambda x: getattr(torch, q_fcn_aggregator)(
                torch.stack(x), axis=0
            )

        self.register_parameter("target_entropy", target_entropy)

    @override(SACTorchModel)
    def get_q_values(
        self,
        model_out: TensorType,
        actions: Optional[TensorType] = None,
        num_critics: Optional[int] = -1,
    ) -> Tuple[List[TensorType], List[TensorType]]:
        """Returns Q-values, given the output of self.__call__().

        This implements [Q_0(s, a), ..., Q_N(s, a)] ->
        [Q-values for all Q_i-functions] for the continuous case and
        [Q_0(s), ..., Q_N(s, a)] -> [Q-values for all actions and for all Q_i-functions]
        for the discrete case.

        Args:
            model_out: Feature outputs from the model layers
                (result of doing `self.__call__(obs)`).
            actions (Optional[TensorType]): Continuous action batch to return
                Q-values for. Shape: [BATCH_SIZE, action_dim]. If None
                (discrete action case), return Q-values for all actions.
            num_critics ( Optional[int]): number of critics to sample
                from the ensemble (default: -1 - sample all the critics)

        Returns:
            TensorType: Q-values tensor of shape [BATCH_SIZE, 1].
        """

        critic_idx = list(range(len(self.q_net_list)))
        if num_critics > 0:
            # sample critics without replacement
            critic_idx = random.sample(critic_idx, num_critics)
        q_value_list = [
            self._get_q_value(
                model_out, actions, self.q_net_list.get_submodule(f"Q_{idx}")
            )
            for idx in critic_idx
        ]
        return zip(*q_value_list)

    @override(SACTorchModel)
    def get_twin_q_values(
        self, model_out: TensorType, actions: Optional[TensorType] = None
    ) -> TensorType:
        raise ValueError("Deprecated method")

    @override(SACTorchModel)
    def q_variables(self):
        """Return the list of variables for Q nets."""
        res = []
        for q_net in self.q_net_list:
            res += q_net.variables()
        return res
