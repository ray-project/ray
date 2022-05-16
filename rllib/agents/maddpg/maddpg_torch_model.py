from typing import Dict, List, Union

import numpy as np
from gym.spaces import Box, Discrete
from ray.rllib.agents.ddpg.noop_model import TorchNoopModel
from ray.rllib.models import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import (
    ModelConfigDict,
    TensorType,
    TrainerConfigDict,
)

torch, nn = try_import_torch()


def _make_continuous_space(space):
    if isinstance(space, Box):
        return space
    elif isinstance(space, Discrete):
        return Box(low=np.zeros((space.n,)), high=np.ones((space.n,)))
    else:
        raise UnsupportedSpaceException("Space {} is not supported.".format(space))


def build_maddpg_models(
    policy: Policy, obs_space: Box, action_space: Box, config: TrainerConfigDict
) -> ModelV2:

    config["model"]["multiagent"] = config[
        "multiagent"
    ]  # Needed for critic obs_space and act_space
    if policy.config["use_state_preprocessor"]:
        default_model = None  # catalog decides
        num_outputs = 256  # arbitrary, thrown away
        config["model"]["no_final_linear"] = True
    else:
        default_model = TorchNoopModel
        num_outputs = np.prod(obs_space.shape)

    policy.model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework=config["framework"],
        model_interface=MADDPGTorchModel,
        default_model=default_model,
        name="maddpg_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        add_layer_norm=(
            policy.config["exploration_config"].get("type") == "ParameterNoise"
        ),
    )

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework=config["framework"],
        model_interface=MADDPGTorchModel,
        default_model=default_model,
        name="target_maddpg_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        add_layer_norm=(
            policy.config["exploration_config"].get("type") == "ParameterNoise"
        ),
    )

    return policy.model


class MADDPGTorchModel(TorchModelV2, nn.Module):
    """
    Extension of TorchModelV2 for MADDPG
    Note that the critic takes in the joint state and action over all agents
    Data flow:
        obs -> forward() -> model_out
        model_out -> get_policy_output() -> pi(s)
        model_out, actions -> get_q_values() -> Q(s, a)
    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass.
    """

    def __init__(
        self,
        observation_space: Box,
        action_space: Box,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
        # Extra MADDPGActionModel args:
        actor_hiddens: List[int] = [256, 256],
        actor_hidden_activation: str = "relu",
        critic_hiddens: List[int] = [256, 256],
        critic_hidden_activation: str = "relu",
        add_layer_norm: bool = False,
    ):

        nn.Module.__init__(self)
        TorchModelV2.__init__(
            self, observation_space, action_space, num_outputs, model_config, name
        )

        self.bounded = np.logical_and(
            self.action_space.bounded_above, self.action_space.bounded_below
        ).any()
        self.action_dim = np.product(self.action_space.shape)

        # Build the policy network.
        self.policy_model = nn.Sequential()
        ins = int(np.product(observation_space.shape))
        self.obs_ins = ins
        activation = get_activation_fn(actor_hidden_activation, framework="torch")
        for i, n in enumerate(actor_hiddens):
            self.policy_model.add_module(
                "action_{}".format(i),
                SlimFC(
                    ins,
                    n,
                    initializer=torch.nn.init.xavier_uniform_,
                    activation_fn=activation,
                ),
            )
            # Add LayerNorm after each Dense.
            if add_layer_norm:
                self.policy_model.add_module(
                    "LayerNorm_A_{}".format(i), nn.LayerNorm(n)
                )
            ins = n

        self.policy_model.add_module(
            "action_out",
            SlimFC(
                ins,
                self.action_dim,
                initializer=torch.nn.init.xavier_uniform_,
                activation_fn=None,
            ),
        )
        # Build MADDPG Critic and Target Critic

        obs_space_n = [
            _make_continuous_space(space)
            for _, (_, space, _, _) in model_config["multiagent"]["policies"].items()
        ]
        act_space_n = [
            _make_continuous_space(space)
            for _, (_, _, space, _) in model_config["multiagent"]["policies"].items()
        ]
        self.critic_obs = np.sum([obs_space.shape[0] for obs_space in obs_space_n])
        self.critic_act = np.sum([act_space.shape[0] for act_space in act_space_n])

        # Build the Q-net(s), including target Q-net(s).
        def build_q_net(name_):
            activation = get_activation_fn(critic_hidden_activation, framework="torch")
            # For continuous actions: Feed obs and actions (concatenated)
            # through the NN. For discrete actions, only obs.
            q_net = nn.Sequential()
            ins = self.critic_obs + self.critic_act
            for i, n in enumerate(critic_hiddens):
                q_net.add_module(
                    "{}_hidden_{}".format(name_, i),
                    SlimFC(
                        ins,
                        n,
                        initializer=nn.init.xavier_uniform_,
                        activation_fn=activation,
                    ),
                )
                ins = n

            q_net.add_module(
                "{}_out".format(name_),
                SlimFC(
                    ins,
                    1,
                    initializer=torch.nn.init.xavier_uniform_,
                    activation_fn=None,
                ),
            )
            return q_net

        self.q_model = build_q_net("q")

        self.view_requirements[SampleBatch.ACTIONS] = ViewRequirement(
            SampleBatch.ACTIONS
        )
        self.view_requirements["new_actions"] = ViewRequirement("new_actions")
        self.view_requirements["t"] = ViewRequirement("t")
        self.view_requirements[SampleBatch.NEXT_OBS] = ViewRequirement(
            data_col=SampleBatch.OBS, shift=1, space=self.obs_space
        )

    def get_q_values(
        self, model_out_n: List[TensorType], act_n: List[TensorType]
    ) -> TensorType:
        """Return the Q estimates for the most recent forward pass.
        This implements Q(s, a).
        Args:
            model_out_n (List[Tensor]): obs embeddings from the model layers
             of each agent, of shape [BATCH_SIZE, num_outputs].
            actions (Tensor): Actions from each agent to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim].
        Returns:
            tensor of shape [BATCH_SIZE].
        """
        model_out_n = torch.cat(model_out_n, -1)
        act_n = torch.cat(act_n, dim=-1)
        return self.q_model(torch.cat([model_out_n, act_n], -1))

    def get_policy_output(self, model_out: TensorType) -> TensorType:
        """Return the action output for the most recent forward pass.
        This outputs the logits over the action space for discrete actions.
        Args:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
        Returns:
            tensor of shape [BATCH_SIZE, action_out_size]
        """
        return self.policy_model(model_out)

    def policy_variables(
        self, as_dict: bool = False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        """Return the list of variables for the policy net."""
        if as_dict:
            return self.policy_model.state_dict()
        return list(self.policy_model.parameters())

    def q_variables(
        self, as_dict=False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        """Return the list of variables for Q / twin Q nets."""
        if as_dict:
            return self.q_model.state_dict()
        return list(self.q_model.parameters())
