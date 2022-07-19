import gym
import numpy as np
from typing import Union, List, Dict

from ray.rllib.models.torch.mingpt import (
    GPTConfig,
    GPT,
    configure_gpt_optimizer,
)
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import (
    ModelConfigDict,
    TensorType,
)

torch, nn = try_import_torch()


class DTTorchModel(TorchModelV2, nn.Module):
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
        self.model = self._build_model()

    def _build_model(self):
        block_size = self.model_config["max_seq_len"] * 3 - 1
        gpt_config = GPTConfig(
            block_size=block_size,
            n_layer=self.model_config["n_layer"],
            n_head=self.model_config["n_head"],
            n_embed=self.model_config["n_embed"],
            embed_pdrop=self.model_config["embed_pdrop"],
            resid_pdrop=self.model_config["resid_pdrop"],
            attn_pdrop=self.model_config["attn_pdrop"],
        )
        gpt = GPT(gpt_config)

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
        n_act_out = (
            self.action_space.n if self._is_action_discrete else 2 * self.action_dim
        )
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
        # actions are concatenated with flattened obs
        critic_hidden_activation = self.model_config["critic_hidden_activation"]
        critic_hiddens = self.model_config["critic_hiddens"]

        activation = get_activation_fn(critic_hidden_activation, framework="torch")
        q_net = nn.Sequential()
        ins = (
            self.obs_ins if self._is_action_discrete else self.obs_ins + self.action_dim
        )
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

    def _get_q_value(
        self, model_out: TensorType, actions: TensorType, q_model: TorchModelV2
    ) -> TensorType:

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
            model_out: obs embeddings from the model layers.
                Shape: [BATCH_SIZE, num_outputs].
            actions: Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim].

        Returns:
            The q_values based on Q(S,A).
            Shape: [BATCH_SIZE].
        """
        return self._get_q_value(model_out, actions, self.q_model)

    def get_twin_q_values(
        self, model_out: TensorType, actions: TensorType
    ) -> TensorType:
        """Same as get_q_values but using the twin Q net.

        This implements the twin Q(s, a).

        Args:
            model_out: obs embeddings from the model layers.
                Shape: [BATCH_SIZE, num_outputs].
            actions: Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim].

        Returns:
            The q_values based on Q_{twin}(S,A).
            Shape: [BATCH_SIZE].
        """
        return self._get_q_value(model_out, actions, self.twin_q_model)

    def get_policy_output(self, model_out: TensorType) -> TensorType:
        """Return the action output for the most recent forward pass.

        This outputs the support for pi(s). For continuous action spaces, this
        is the action directly. For discrete, it is the mean / std dev.

        Args:
            model_out: obs embeddings from the model layers.
                Shape: [BATCH_SIZE, num_outputs].

        Returns:
            The output of pi(s).
            Shape: [BATCH_SIZE, action_out_size].
        """
        return self.actor_model(model_out)

