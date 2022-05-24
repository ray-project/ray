from typing import Union, Type, TYPE_CHECKING, List

import torch

from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.algorithms.cql import CQLTorchPolicy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.algorithms.ddpg.ddpg_torch_policy import ddpg_actor_critic_loss
# from ray.rllib.algorithms.sac.sac_tf_policy import build_sac_model
from ray.rllib.algorithms.ddpg.ddpg_tf_policy import build_ddpg_models
from ray.rllib.algorithms.ddpg.ddpg_torch_model import DDPGTorchModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.algorithms.ddpg.noop_model import NoopModel, TorchNoopModel


from ray.rllib.agents import TrainerConfig
import gym
import numpy as np

# if TYPE_CHECKING:
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.policy import Policy
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.utils.typing import (
    TrainerConfigDict,
    TensorType,
    LocalOptimizer,
    GradInfoDict,
)


from ray.rllib.utils.annotations import (
    DeveloperAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    override,
    is_overridden,
)


# def crr_loss(
#     policy: Policy,
#     model: ModelV2,
#     dist: Type[TorchDistributionWrapper],
#     batch: SampleBatch
# ) -> Union[TensorType, List[TensorType]]:
#     return ddpg_actor_critic_loss(policy, model, dist, train_batch=batch)
#
#
#
# CRRTorchPolicy = build_policy_class(
#     name="CRRTorchPolicy",
#     framework="torch",
#     loss_fn=crr_loss,
# )


class CRRTorchPolicy(TorchPolicyV2):

    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict,
    ):

        self.target_model = None  # assign it in self.make_model

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        # by here your model should include the following
        # (We assume state s is already encoded and there is no need to use RNNs/other models to encode observations into states):
        # 1. a nn representing the actor pi(a|s)
        #   1.1* in case of continuous actions it should be normal / squashed normal dist if the action space is bounded?
        #   1.2 in case of of discrete set of actions the output of the model should be a discrete distribution over action classes
        # 2. a nn representing the critic Q(s, a)
        #   2.1* in case of continuous actions it should take in concat([s,a]) and output a single scalar
        #   2.2 in case of discrete actions it should take in s and output a logit for each action class as well as a scale for matching the reward scale.
        # 3. for critic it should have n_critic copies of the Q function nn
        # 4. for each critic it should have a target model copy

    def make_model(self) -> ModelV2:
        # copying ddpg build model to here to be explicit
        model_config = self.config['model']

        if self.config["use_state_preprocessor"]:
            default_model = None  # catalog decides
            num_outputs = 256  # arbitrary
            model_config["no_final_linear"] = True
        else:
            default_model = TorchNoopModel
            num_outputs = int(np.product(self.observation_space.shape))

        # TODO: why do we even have to go through this get_model_v2 function?
        self.model = ModelCatalog.get_model_v2(
            obs_space=self.observation_space,
            action_space=self.action_space,
            num_outputs=num_outputs,
            model_config=model_config,
            framework=self.config["framework"],
            model_interface=DDPGTorchModel,  # use this model for interface (get_q, get_q_twin, .etc)
            default_model=default_model,
            name="model",
            actor_hidden_activation=self.config["actor_hidden_activation"],
            actor_hiddens=self.config["actor_hiddens"],
            critic_hidden_activation=self.config["critic_hidden_activation"],
            critic_hiddens=self.config["critic_hiddens"],
            twin_q=self.config["twin_q"],
            add_layer_norm=(
                self.config["exploration_config"].get("type") == "ParameterNoise"
            ),
        )

        # TODO: this is a bad python pattern to assign attributes that do not exist in the constructor
        self.target_model = ModelCatalog.get_model_v2(
            obs_space=self.observation_space,
            action_space=self.action_space,
            num_outputs=num_outputs,
            model_config=model_config,
            framework=self.config["framework"],
            model_interface=DDPGTorchModel,  # use this model for interface (get_q, get_q_twin, .etc)
            default_model=default_model,
            name="target_model",
            actor_hidden_activation=self.config["actor_hidden_activation"],
            actor_hiddens=self.config["actor_hiddens"],
            critic_hidden_activation=self.config["critic_hidden_activation"],
            critic_hiddens=self.config["critic_hiddens"],
            twin_q=self.config["twin_q"],
            add_layer_norm=(
                self.config["exploration_config"].get("type") == "ParameterNoise"
            ),
        )

        return self.model

    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:

        ############ update the actor
        # compute the weights assigned to every transition (s_t, a_t)
        train_batch['action_weights'] = self._compute_action_weights(model, dist_class, train_batch)

        # compute actor loss
        actor_loss = self._compute_actor_loss(train_batch, dist_class)

        ############ update the critic
        # standard critic update with pessimistic Q-learning (e.g. DQN)
        critic_loss = self._compute_critic_loss(model, dist_class, train_batch)

        return {'actor': actor_loss, 'critic': critic_loss}

    def _get_q_value(self, model: ModelV2, model_out: TensorType, actions: TensorType) -> TensorType:
        # helper function to compute the pessimistic q value
        q1 = model.get_q_values(model_out, actions)
        q2 = model.get_twin_q_values(model_out, actions)
        return torch.minimum(q1, q2)

    def _compute_adv(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        # uses mean|max to compute estimate of advantages
        # continuous/discrete action spaces:
        #   A(s_t, a_t) = Q(s_t, a_t) - max_{a^j} Q(s_t, a^j) where a^j is m times sampled from the policy p(a | s_t)
        #   A(s_t, a_t) = Q(s_t, a_t) - avg( Q(s_t, a^j) ) where a^j is m times sampled from the policy p(a | s_t)
        # questions: Do we use pessimistic q approximate or the normal one?
        advantage_type = self.config['avantage_type']
        n_action_sample = self.config['n_action_sample']
        batch_size = len(train_batch)
        out_t, _ = model(train_batch)

        # construct pi(s_t) for sampling actions
        pi_s_t = dist_class(model.get_policy_output(out_t))
        policy_actions = pi_s_t.sample(n_action_sample) # samples
        flat_actions = policy_actions(-1, *self.action_space.shape)

        reshaped_s_t = train_batch[SampleBatch.OBS].view(batch_size, 1, *self.observation_space.shape)
        reshaped_s_t = reshaped_s_t.expand(
            batch_size, n_action_sample, *self.observation_space.shape
        )
        flat_s_t = reshaped_s_t.reshape(-1, *self.observation_space.shape)

        input_next = SampleBatch(
            **{SampleBatch.OBS: flat_s_t, SampleBatch.ACTIONS: flat_actions}
        )
        out_next, _ = model(input_next)

        flat_v_t = self._get_q_value(model, out_next, flat_actions)
        reshaped_v_t = flat_v_t.reshape(batch_size, -1, 1)

        if advantage_type == 'mean':
            v_t = reshaped_v_t.mean(dim=1)
        elif advantage_type == 'max':
            v_t = reshaped_v_t.max(dim=1)
        else:
            raise ValueError(
                f'Invalid advantage type: {advantage_type}.'
            )

        q_t = model.get_q_values(out_t, train_batch[SampleBatch.ACTIONS])

        return q_t - v_t

    def _compute_action_weights(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        # uses bin|exp to compute action weights
        # 1(A>=0) or exp(A/temp)

        weight_type = self.config['weight_type']
        advantages = self._compute_adv(model, dist_class, train_batch)

        if weight_type == "bin":
            weights = (advantages > 0.0).float()
        elif weight_type == "exp":
            temperature = self.config['temperature']
            max_weight = self.config['max_weight']
            weights =  (advantages / temperature).exp().clamp(0.0, max_weight)
        else:
            raise ValueError(f"invalid weight type: {weight_type}.")

        return weights

    def _compute_actor_loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:

        # loss = - (train_batch.weights * train_batch.logp).mean()
        return None

    # def _compute_critic_loss(
    #     self,
    #     model: ModelV2,
    #     dist_class: Type[TorchDistributionWrapper],
    #     train_batch: SampleBatch
    # ):
    #     # target, use target model to compute the target
    #     q_next = model.value_function(train_batch)
    #     target = train_batch.reward + discount * q_next
    #
    #     # cross entropy or MSE depending on the action space
    #
    #     return loss



if __name__ == '__main__':

    obs_space = gym.spaces.Box(np.array((-1, -1)), np.array((1, 1)))
    act_space =  gym.spaces.Box(np.array((-1,-1)), np.array((1, 1)))
    config = TrainerConfig().framework(framework='torch').to_dict()
    print(config['framework'])
    CRRTorchPolicy(obs_space, act_space, config=config)