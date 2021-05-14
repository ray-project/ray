import gym
import numpy as np
from typing import Optional

from ray.rllib.agents.sac.sac_tf_model import SACTFModel
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import ModelConfigDict

tf1, tf, tfv = try_import_tf()


class CQLSACTFModel(SACTFModel):
    """Extension of SACTFModel for CQL.

    To customize, do one of the following:
    - sub-class CQLTFModel and override one or more of its methods.
    - Use CQL's `Q_model` and `policy_model` keys to tweak the default model
      behaviors (e.g. fcnet_hiddens, conv_filters, etc..).
    - Use CQL's `Q_model->custom_model` and `policy_model->custom_model` keys
      to specify your own custom Q-model(s) and policy-models, which will be
      created within this CQLTFModel (see `build_policy_model` and
      `build_q_model`.

    Note: It is not recommended to override the `forward` method for CQL. This
    would lead to shared weights (between policy and Q-nets), which will then
    not be optimized by either of the critic- or actor-optimizers!

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
                 twin_q: bool = False,
                 initial_alpha: float = 1.0,
                 target_entropy: Optional[float] = None,
                 lagrangian: bool = False,
                 initial_alpha_prime: float = 1.0):
        """Initialize a CQLSACTFModel instance.

        Args:
            policy_model_config (ModelConfigDict): The config dict for the
                policy network.
            q_model_config (ModelConfigDict): The config dict for the
                Q-network(s) (2 if twin_q=True).
            twin_q (bool): Build twin Q networks (Q-net and target) for more
                stable Q-learning.
            initial_alpha (float): The initial value for the to-be-optimized
                alpha parameter (default: 1.0).
            target_entropy (Optional[float]): A target entropy value for
                the to-be-optimized alpha parameter. If None, will use the
                defaults described in the papers for SAC (and discrete SAC).
            lagrangian (bool): Whether to automatically adjust value via
                Lagrangian dual gradient descent.
            initial_alpha_prime (float): The initial value for the to-be-optimized
                alpha_prime parameter (default: 1.0).

        Note that the core layers for forward() are not defined here, this
        only defines the layers for the output heads. Those layers for
        forward() should be defined in subclasses of CQLModel.
        """
        super(CQLSACTFModel, self).__init__(obs_space, action_space, num_outputs,
                                         model_config, name, policy_model_config,
                                         q_model_config, twin_q, initial_alpha,
                                         target_entropy)
        if lagrangian:
            self.log_alpha_prime = tf.Variable(
                np.log(initial_alpha_prime), dtype=tf.float32, name="log_alpha_prime")
            self.alpha_prime = tf.exp(self.log_alpha_prime)
