from typing import Union, Type, TYPE_CHECKING, List, Dict, cast
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.typing import (
    TensorType,
    ModelConfigDict,
)

from ray.rllib.utils.framework import try_import_torch
import gym
import numpy as np

from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.utils import get_activation_fn

torch, nn = try_import_torch()

class CRRModel(TorchModelV2, nn.Module):

    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
    ):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)

        self._is_action_discrete = isinstance(action_space, gym.spaces.Discrete)

        # TODO: I don't know why this is true yet? (in = num_outputs)
        self.obs_ins = num_outputs
        self.action_dim = np.product(self.action_space.shape)
        self.actor_model = self._build_actor_net('actor')
        twin_q = self.model_config['twin_q']
        self.q_model = self._build_q_net("q")
        if twin_q:
            self.twin_q_model = self._build_q_net("twin_q")
        else:
            self.twin_q_model = None

    def _build_actor_net(self, name_):
        actor_hidden_activation = self.model_config['actor_hidden_activation']
        actor_hiddens = self.model_config['actor_hiddens']

        # Build the policy network.
        actor_net = nn.Sequential()

        activation = get_activation_fn(actor_hidden_activation, framework="torch")
        ins = self.obs_ins
        for i, n in enumerate(actor_hiddens):
            actor_net.add_module(
                f"{name_}_hidden_{i}",
                SlimFC(
                    ins,
                    n,
                    initializer=torch.nn.init.xavier_uniform_,
                    activation_fn=activation,
                ),
            )
            ins = n

        # also includes log_std in continuous case
        n_act_out = self.action_space.n if self._is_action_discrete \
            else 2 * self.action_dim
        actor_net.add_module(
            f"{name_}_out",
            SlimFC(
                ins,
                n_act_out,
                initializer=torch.nn.init.xavier_uniform_,
                activation_fn=None,
            ),
        )

        return actor_net

    def _build_q_net(self, name_):
        # TODO: only supports continuous actions at the moment
        # actions are concatenated with flattened obs
        critic_hidden_activation = self.model_config['critic_hidden_activation']
        critic_hiddens = self.model_config['critic_hiddens']

        activation = get_activation_fn(critic_hidden_activation, framework="torch")
        q_net = nn.Sequential()
        ins = self.obs_ins if self._is_action_discrete else \
            self.obs_ins + self.action_dim
        for i, n in enumerate(critic_hiddens):
            q_net.add_module(
                f"{name_}_hidden_{i}",
                SlimFC(
                    ins,
                    n,
                    initializer=torch.nn.init.xavier_uniform_,
                    activation_fn=activation,
                ),
            )
            ins = n

        q_net.add_module(
            f"{name_}_out",
            SlimFC(
                ins,
                self.action_space.n if self._is_action_discrete else 1,
                initializer=torch.nn.init.xavier_uniform_,
                activation_fn=None,
            ),
        )
        return q_net

    def _get_q_value(self,
                     model_out: TensorType,
                     actions: TensorType,
                     q_model: TorchModelV2) -> TensorType:

        if self._is_action_discrete:
            rows = torch.arange(len(actions)).to(actions)
            q_vals = q_model(model_out)[rows, actions].unsqueeze(-1)
        else:
            q_vals = q_model(torch.cat([model_out, actions], -1))

        return q_vals

    def get_q_values(self, model_out: TensorType, actions: TensorType) -> TensorType:
        """Return the Q estimates for the most recent forward pass.

        This implements Q(s, a).

        Args:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Tensor): Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim].

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        return self._get_q_value(model_out, actions, self.q_model)

    def get_twin_q_values(
        self, model_out: TensorType, actions: TensorType
    ) -> TensorType:
        """Same as get_q_values but using the twin Q net.

        This implements the twin Q(s, a).

        Args:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Optional[Tensor]): Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim].

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        return  self._get_q_value(model_out, actions, self.twin_q_model)

    def get_policy_output(self, model_out: TensorType) -> TensorType:
        """Return the action output for the most recent forward pass.

        This outputs the support for pi(s). For continuous action spaces, this
        is the action directly. For discrete, is is the mean / std dev.

        Args:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].

        Returns:
            tensor of shape [BATCH_SIZE, action_out_size]
        """
        return self.actor_model(model_out)

    def policy_variables(
        self, as_dict: bool = False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        """Return the list of variables for the policy net."""
        if as_dict:
            return self.actor_model.state_dict()
        return list(self.actor_model.parameters())

    def q_variables(
        self, as_dict=False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        """Return the list of variables for Q / twin Q nets."""
        if as_dict:
            return {
                **self.q_model.state_dict(),
                **(self.twin_q_model.state_dict() if self.twin_q_model else {}),
            }
        return list(self.q_model.parameters()) + (
            list(self.twin_q_model.parameters()) if self.twin_q_model else []
        )
