import gym
from gym.spaces import Box, Discrete
import numpy as np
from typing import Optional, Tuple

from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.typing import ModelConfigDict, TensorType

torch, nn = try_import_torch()


class SACTorchModel(TorchModelV2, nn.Module):
    """Extension of the standard TorchModelV2 for SAC.

    Instances of this Model get created via wrapping this class around another
    default- or custom model (inside
    rllib/agents/sac/sac_torch_policy.py::build_sac_model). Doing so simply
    adds this class' methods (`get_q_values`, etc..) to the wrapped model, such
    that the wrapped model can be used by the SAC algorithm.

    Data flow:
        `obs` -> forward() -> `model_out`
        `model_out` -> get_policy_output() -> pi(actions|obs)
        `model_out`, `actions` -> get_q_values() -> Q(s, a)
        `model_out`, `actions` -> get_twin_q_values() -> Q_twin(s, a)
    """

    def __init__(self,
                 obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space,
                 num_outputs: Optional[int],
                 model_config: ModelConfigDict,
                 name: str,
                 actor_hidden_activation: str = "relu",
                 actor_hiddens: Tuple[int] = (256, 256),
                 critic_hidden_activation: str = "relu",
                 critic_hiddens: Tuple[int] = (256, 256),
                 twin_q: bool = False,
                 initial_alpha: float = 1.0,
                 target_entropy: Optional[float] = None):
        """Initializes a SACTorchModel instance.
7
        Args:
            actor_hidden_activation (str): Activation for the actor network.
            actor_hiddens (list): Hidden layers sizes for the actor network.
            critic_hidden_activation (str): Activation for the critic network.
            critic_hiddens (list): Hidden layers sizes for the critic network.
            twin_q (bool): Build twin Q networks (Q-net and target) for more
                stable Q-learning.
            initial_alpha (float): The initial value for the to-be-optimized
                alpha parameter (default: 1.0).
            target_entropy (Optional[float]): A target entropy value for
                the to-be-optimized alpha parameter. If None, will use the
                defaults described in the papers for SAC (and discrete SAC).

        Note that the core layers for forward() are not defined here, this
        only defines the layers for the output heads. Those layers for
        forward() should be defined in subclasses of SACModel.
        """
        nn.Module.__init__(self)
        super(SACTorchModel, self).__init__(obs_space, action_space,
                                            num_outputs, model_config, name)

        if isinstance(action_space, Discrete):
            self.action_dim = action_space.n
            self.discrete = True
            action_outs = q_outs = self.action_dim
            action_ins = None  # No action inputs for the discrete case.
        elif isinstance(action_space, Box):
            self.action_dim = np.product(action_space.shape)
            self.discrete = False
            action_outs = 2 * self.action_dim
            action_ins = self.action_dim
            q_outs = 1
        else:
            assert isinstance(action_space, Simplex)
            self.action_dim = np.product(action_space.shape)
            self.discrete = False
            action_outs = self.action_dim
            action_ins = self.action_dim
            q_outs = 1

        # Build the policy network.
        self.action_model = nn.Sequential()
        ins = self.num_outputs
        self.obs_ins = ins
        activation = get_activation_fn(
            actor_hidden_activation, framework="torch")
        for i, n in enumerate(actor_hiddens):
            self.action_model.add_module(
                "action_{}".format(i),
                SlimFC(
                    ins,
                    n,
                    initializer=torch.nn.init.xavier_uniform_,
                    activation_fn=activation))
            ins = n
        self.action_model.add_module(
            "action_out",
            SlimFC(
                ins,
                action_outs,
                initializer=torch.nn.init.xavier_uniform_,
                activation_fn=None))

        # Build the Q-net(s), including target Q-net(s).
        def build_q_net(name_):
            activation = get_activation_fn(
                critic_hidden_activation, framework="torch")
            # For continuous actions: Feed obs and actions (concatenated)
            # through the NN. For discrete actions, only obs.
            q_net = nn.Sequential()
            ins = self.obs_ins + (0 if self.discrete else action_ins)
            for i, n in enumerate(critic_hiddens):
                q_net.add_module(
                    "{}_hidden_{}".format(name_, i),
                    SlimFC(
                        ins,
                        n,
                        initializer=torch.nn.init.xavier_uniform_,
                        activation_fn=activation))
                ins = n

            q_net.add_module(
                "{}_out".format(name_),
                SlimFC(
                    ins,
                    q_outs,
                    initializer=torch.nn.init.xavier_uniform_,
                    activation_fn=None))
            return q_net

        self.q_net = build_q_net("q")
        if twin_q:
            self.twin_q_net = build_q_net("twin_q")
        else:
            self.twin_q_net = None

        log_alpha = nn.Parameter(
            torch.from_numpy(np.array([np.log(initial_alpha)])).float())
        self.register_parameter("log_alpha", log_alpha)

        # Auto-calculate the target entropy.
        if target_entropy is None or target_entropy == "auto":
            # See hyperparams in [2] (README.md).
            if self.discrete:
                target_entropy = 0.98 * np.array(
                    -np.log(1.0 / action_space.n), dtype=np.float32)
            # See [1] (README.md).
            else:
                target_entropy = -np.prod(action_space.shape)

        self.target_entropy = torch.tensor(
            data=[target_entropy], dtype=torch.float32, requires_grad=False)

    def get_q_values(self,
                     model_out: TensorType,
                     actions: Optional[TensorType] = None) -> TensorType:
        """Returns Q-values, given the output of self.__call__().

        This implements Q(s, a) -> [single Q-value] for the continuous case and
        Q(s) -> [Q-values for all actions] for the discrete case.

        Args:
            model_out (TensorType): Feature outputs from the model layers
                (result of doing `self.__call__(obs)`).
            actions (Optional[TensorType]): Continuous action batch to return
                Q-values for. Shape: [BATCH_SIZE, action_dim]. If None
                (discrete action case), return Q-values for all actions.

        Returns:
            TensorType: Q-values tensor of shape [BATCH_SIZE, 1].
        """
        # Continuous case -> concat actions to model_out.
        if actions is not None:
            return self.q_net(torch.cat([model_out, actions], -1))
        # Discrete case -> return q-vals for all actions.
        else:
            return self.q_net(model_out)

    def get_twin_q_values(self,
                          model_out: TensorType,
                          actions: Optional[TensorType] = None) -> TensorType:
        """Same as get_q_values but using the twin Q net.

        This implements the twin Q(s, a).

        Args:
            model_out (TensorType): Feature outputs from the model layers
                (result of doing `self.__call__(obs)`).
            actions (Optional[Tensor]): Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim]. If None (discrete action
                case), return Q-values for all actions.

        Returns:
            TensorType: Q-values tensor of shape [BATCH_SIZE, 1].
        """
        # Continuous case -> concat actions to model_out.
        if actions is not None:
            return self.twin_q_net(torch.cat([model_out, actions], -1))
        # Discrete case -> return q-vals for all actions.
        else:
            return self.twin_q_net(model_out)

    def get_policy_output(self, model_out: TensorType) -> TensorType:
        """Returns policy outputs, given the output of self.__call__().

        For continuous action spaces, these will be the mean/stddev
        distribution inputs for the (SquashedGaussian) action distribution.
        For discrete action spaces, these will be the logits for a categorical
        distribution.

        Args:
            model_out (TensorType): Feature outputs from the model layers
                (result of doing `self.__call__(obs)`).

        Returns:
            TensorType: Distribution inputs for sampling actions.
        """
        return self.action_model(model_out)

    def policy_variables(self):
        """Return the list of variables for the policy net."""

        return list(self.action_model.parameters())

    def q_variables(self):
        """Return the list of variables for Q / twin Q nets."""

        return list(self.q_net.parameters()) + \
            (list(self.twin_q_net.parameters()) if self.twin_q_net else [])
