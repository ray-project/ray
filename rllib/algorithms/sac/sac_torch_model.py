import gym
from gym.spaces import Box, Discrete
import numpy as np
import tree  # pip install dm_tree
from typing import Dict, List, Optional

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.typing import ModelConfigDict, TensorType, TensorStructType

torch, nn = try_import_torch()


class SACTorchModel(TorchModelV2, nn.Module):
    """Extension of the standard TorchModelV2 for SAC.

    To customize, do one of the following:
    - sub-class SACTorchModel and override one or more of its methods.
    - Use SAC's `q_model_config` and `policy_model` keys to tweak the default model
      behaviors (e.g. fcnet_hiddens, conv_filters, etc..).
    - Use SAC's `q_model_config->custom_model` and `policy_model->custom_model` keys
      to specify your own custom Q-model(s) and policy-models, which will be
      created within this SACTFModel (see `build_policy_model` and
      `build_q_model`.

    Note: It is not recommended to override the `forward` method for SAC. This
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
        twin_q: bool = False,
        initial_alpha: float = 1.0,
        target_entropy: Optional[float] = None,
    ):
        """Initializes a SACTorchModel instance.
        7
                Args:
                    policy_model_config: The config dict for the
                        policy network.
                    q_model_config: The config dict for the
                        Q-network(s) (2 if twin_q=True).
                    twin_q: Build twin Q networks (Q-net and target) for more
                        stable Q-learning.
                    initial_alpha: The initial value for the to-be-optimized
                        alpha parameter (default: 1.0).
                    target_entropy (Optional[float]): A target entropy value for
                        the to-be-optimized alpha parameter. If None, will use the
                        defaults described in the papers for SAC (and discrete SAC).

                Note that the core layers for forward() are not defined here, this
                only defines the layers for the output heads. Those layers for
                forward() should be defined in subclasses of SACModel.
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

        # Build the Q-network(s).
        self.q_net = self.build_q_model(
            self.obs_space, self.action_space, q_outs, q_model_config, "q"
        )
        if twin_q:
            self.twin_q_net = self.build_q_model(
                self.obs_space, self.action_space, q_outs, q_model_config, "twin_q"
            )
        else:
            self.twin_q_net = None

        log_alpha = nn.Parameter(
            torch.from_numpy(np.array([np.log(initial_alpha)])).float()
        )
        self.register_parameter("log_alpha", log_alpha)

        # Auto-calculate the target entropy.
        if target_entropy is None or target_entropy == "auto":
            # See hyperparams in [2] (README.md).
            if self.discrete:
                target_entropy = 0.98 * np.array(
                    -np.log(1.0 / action_space.n), dtype=np.float32
                )
            # See [1] (README.md).
            else:
                target_entropy = -np.prod(action_space.shape)

        target_entropy = nn.Parameter(
            torch.from_numpy(np.array([target_entropy])).float(), requires_grad=False
        )
        self.register_parameter("target_entropy", target_entropy)

    @override(TorchModelV2)
    def forward(
        self,
        input_dict: Dict[str, TensorType],
        state: List[TensorType],
        seq_lens: TensorType,
    ) -> (TensorType, List[TensorType]):
        """The common (Q-net and policy-net) forward pass.

        NOTE: It is not(!) recommended to override this method as it would
        introduce a shared pre-network, which would be updated by both
        actor- and critic optimizers.
        """
        return input_dict["obs"], state

    def build_policy_model(self, obs_space, num_outputs, policy_model_config, name):
        """Builds the policy model used by this SAC.

        Override this method in a sub-class of SACTFModel to implement your
        own policy net. Alternatively, simply set `custom_model` within the
        top level SAC `policy_model` config key to make this default
        implementation of `build_policy_model` use your custom policy network.

        Returns:
            TorchModelV2: The TorchModelV2 policy sub-model.
        """
        model = ModelCatalog.get_model_v2(
            obs_space,
            self.action_space,
            num_outputs,
            policy_model_config,
            framework="torch",
            name=name,
        )
        return model

    def build_q_model(self, obs_space, action_space, num_outputs, q_model_config, name):
        """Builds one of the (twin) Q-nets used by this SAC.

        Override this method in a sub-class of SACTFModel to implement your
        own Q-nets. Alternatively, simply set `custom_model` within the
        top level SAC `q_model_config` config key to make this default implementation
        of `build_q_model` use your custom Q-nets.

        Returns:
            TorchModelV2: The TorchModelV2 Q-net sub-model.
        """
        self.concat_obs_and_actions = False
        if self.discrete:
            input_space = obs_space
        else:
            orig_space = getattr(obs_space, "original_space", obs_space)
            if isinstance(orig_space, Box) and len(orig_space.shape) == 1:
                input_space = Box(
                    float("-inf"),
                    float("inf"),
                    shape=(orig_space.shape[0] + action_space.shape[0],),
                )
                self.concat_obs_and_actions = True
            else:
                input_space = gym.spaces.Tuple([orig_space, action_space])

        model = ModelCatalog.get_model_v2(
            input_space,
            action_space,
            num_outputs,
            q_model_config,
            framework="torch",
            name=name,
        )
        return model

    def get_q_values(
        self, model_out: TensorType, actions: Optional[TensorType] = None
    ) -> TensorType:
        """Returns Q-values, given the output of self.__call__().

        This implements Q(s, a) -> [single Q-value] for the continuous case and
        Q(s) -> [Q-values for all actions] for the discrete case.

        Args:
            model_out: Feature outputs from the model layers
                (result of doing `self.__call__(obs)`).
            actions (Optional[TensorType]): Continuous action batch to return
                Q-values for. Shape: [BATCH_SIZE, action_dim]. If None
                (discrete action case), return Q-values for all actions.

        Returns:
            TensorType: Q-values tensor of shape [BATCH_SIZE, 1].
        """
        return self._get_q_value(model_out, actions, self.q_net)

    def get_twin_q_values(
        self, model_out: TensorType, actions: Optional[TensorType] = None
    ) -> TensorType:
        """Same as get_q_values but using the twin Q net.

        This implements the twin Q(s, a).

        Args:
            model_out: Feature outputs from the model layers
                (result of doing `self.__call__(obs)`).
            actions (Optional[Tensor]): Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim]. If None (discrete action
                case), return Q-values for all actions.

        Returns:
            TensorType: Q-values tensor of shape [BATCH_SIZE, 1].
        """
        return self._get_q_value(model_out, actions, self.twin_q_net)

    def _get_q_value(self, model_out, actions, net):
        # Model outs may come as original Tuple observations, concat them
        # here if this is the case.
        if isinstance(net.obs_space, Box):
            if isinstance(model_out, (list, tuple)):
                model_out = torch.cat(model_out, dim=-1)
            elif isinstance(model_out, dict):
                model_out = torch.cat(list(model_out.values()), dim=-1)

        # Continuous case -> concat actions to model_out.
        if actions is not None:
            if self.concat_obs_and_actions:
                input_dict = {"obs": torch.cat([model_out, actions], dim=-1)}
            else:
                # TODO(junogng) : SampleBatch doesn't support list columns yet.
                #     Use ModelInputDict.
                input_dict = {"obs": (model_out, actions)}
        # Discrete case -> return q-vals for all actions.
        else:
            input_dict = {"obs": model_out}
        # Switch on training mode (when getting Q-values, we are usually in
        # training).
        input_dict["is_training"] = True

        return net(input_dict, [], None)

    def get_action_model_outputs(
        self,
        model_out: TensorType,
        state_in: List[TensorType] = None,
        seq_lens: TensorType = None,
    ) -> (TensorType, List[TensorType]):
        """Returns distribution inputs and states given the output of
        policy.model().

        For continuous action spaces, these will be the mean/stddev
        distribution inputs for the (SquashedGaussian) action distribution.
        For discrete action spaces, these will be the logits for a categorical
        distribution.

        Args:
            model_out: Feature outputs from the model layers
                (result of doing `model(obs)`).
            state_in List(TensorType): State input for recurrent cells
            seq_lens: Sequence lengths of input- and state
                sequences

        Returns:
            TensorType: Distribution inputs for sampling actions.
        """

        def concat_obs_if_necessary(obs: TensorStructType):
            """Concat model outs if they come as original tuple observations."""
            if isinstance(obs, (list, tuple)):
                obs = torch.cat(obs, dim=-1)
            elif isinstance(obs, dict):
                obs = torch.cat(
                    [
                        torch.unsqueeze(val, 1) if len(val.shape) == 1 else val
                        for val in tree.flatten(obs.values())
                    ],
                    dim=-1,
                )
            return obs

        if state_in is None:
            state_in = []

        if isinstance(model_out, dict) and "obs" in model_out:
            # Model outs may come as original Tuple observations
            if isinstance(self.action_model.obs_space, Box):
                model_out["obs"] = concat_obs_if_necessary(model_out["obs"])
            return self.action_model(model_out, state_in, seq_lens)
        else:
            if isinstance(self.action_model.obs_space, Box):
                model_out = concat_obs_if_necessary(model_out)
            return self.action_model({"obs": model_out}, state_in, seq_lens)

    def policy_variables(self):
        """Return the list of variables for the policy net."""

        return self.action_model.variables()

    def q_variables(self):
        """Return the list of variables for Q / twin Q nets."""

        return self.q_net.variables() + (
            self.twin_q_net.variables() if self.twin_q_net else []
        )
