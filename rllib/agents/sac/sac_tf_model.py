import gym
from gym.spaces import Box, Discrete
import numpy as np
from typing import Dict, List, Optional, Tuple

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.typing import ModelConfigDict, TensorType

tf1, tf, tfv = try_import_tf()


class SACTFModel(TFModelV2):
    """Extension of the standard TFModelV2 for SAC.

    #TODO: fix comment, no more wrapping.
    Instances of this Model get created via wrapping this class around another
    default- or custom model (inside
    rllib/agents/sac/sac_tf_policy.py::build_sac_model). Doing so simply adds
    this class' methods (`get_q_values`, etc..) to the wrapped model, such that
    the wrapped model can be used by the SAC algorithm.

    Data flow:
        `obs` -> forward() (should stay a noop method!) -> `model_out`
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
                 policy_model_config: ModelConfigDict = None,
                 q_model_config: ModelConfigDict = None,
                 #actor_hidden_activation: str = "relu",
                 #actor_hiddens: Tuple[int] = (256, 256),
                 #critic_hidden_activation: str = "relu",
                 #critic_hiddens: Tuple[int] = (256, 256),
                 twin_q: bool = False,
                 initial_alpha: float = 1.0,
                 target_entropy: Optional[float] = None):
        """Initialize a SACTFModel instance.

        Args:
            #actor_hidden_activation (str): Activation for the actor network.
            #actor_hiddens (list): Hidden layers sizes for the actor network.
            #critic_hidden_activation (str): Activation for the critic network.
            #critic_hiddens (list): Hidden layers sizes for the critic network.
            TODO
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
        super(SACTFModel, self).__init__(obs_space, action_space, num_outputs,
                                         model_config, name)
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

        self.action_model = self.build_policy_model(
            self.obs_space, action_outs, policy_model_config, "policy_model")

        self.q_net = self.build_q_net(
            self.obs_space, self.action_space, q_outs, q_model_config, "q")
        if twin_q:
            self.twin_q_net = self.build_q_net(
                self.obs_space, self.action_space, q_outs, q_model_config,
                "twin_q")
        else:
            self.twin_q_net = None

        self.log_alpha = tf.Variable(
            np.log(initial_alpha), dtype=tf.float32, name="log_alpha")
        self.alpha = tf.exp(self.log_alpha)

        # Auto-calculate the target entropy.
        if target_entropy is None or target_entropy == "auto":
            # See hyperparams in [2] (README.md).
            if self.discrete:
                target_entropy = 0.98 * np.array(
                    -np.log(1.0 / action_space.n), dtype=np.float32)
            # See [1] (README.md).
            else:
                target_entropy = -np.prod(action_space.shape)
        self.target_entropy = target_entropy

    @override(TFModelV2)
    def forward(self, input_dict: Dict[str, TensorType],
                state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
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
        implementation use your custom policy network.

        Args:
            TODO

        Returns:
            TFModelV2: The TFModelV2 policy sub-model.
        """
        model = ModelCatalog.get_model_v2(obs_space, self.action_space, num_outputs, policy_model_config, framework="tf", name=name)
        return model

    def build_q_net(self, obs_space, action_space, num_outputs, q_model_config, name):
        """Builds one of the (twin) Q-nets used by this SAC.

        Override this method in a sub-class of SACTFModel to implement your
        own Q-nets. Alternatively, simply set `custom_model` within the
        top level SAC `Q_model` config key to make this default implementation
        use your custom Q-nets.

        Args:
            observations (tf.keras.layer.Layer): The observation inputs layer.
            actions  (tf.keras.layer.Layer): The actions inputs layer.

        Returns:
            tf.keras.model.Model: The keras Q-net model.
        """
        if self.discrete:
            input_space = obs_space
        else:
            orig_space = getattr(obs_space, "original_space", obs_space)
            input_space = gym.spaces.Tuple(
                (orig_space.spaces if isinstance(orig_space, gym.spaces.Tuple)
                 else [obs_space]) + [action_space])
        model = ModelCatalog.get_model_v2(input_space, action_space, num_outputs, q_model_config, framework="tf", name=name)
        return model

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
            input_dict = {"obs": force_list(model_out) + [actions]}
        # Discrete case -> return q-vals for all actions.
        else:
            input_dict = {"obs": model_out}

        out, _ = self.q_net(input_dict, [], None)
        return out

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
            input_dict = {"obs": force_list(model_out) + [actions]}
        # Discrete case -> return q-vals for all actions.
        else:
            input_dict = {"obs": model_out}

        out, _ = self.twin_q_net(input_dict, [], None)
        return out

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
        out, _ = self.action_model({"obs": model_out}, [], None)
        return out

    def policy_variables(self):
        """Return the list of variables for the policy net."""

        return list(self.action_model.variables())

    def q_variables(self):
        """Return the list of variables for Q / twin Q nets."""

        return self.q_net.variables() + (self.twin_q_net.variables()
                                         if self.twin_q_net else [])
