from typing import Union, Type, TYPE_CHECKING, List
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.algorithms.cql import CQLTorchPolicy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.algorithms.ddpg.ddpg_torch_policy import ddpg_actor_critic_loss

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

        # self.model = ...
        # self.target_model = deepcopy(self.model)

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
        breakpoint()

    # def make_model(self) -> ModelV2:
    #     pass

    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:

        ############ update the actor
        # compute the advantages using Q(s_t, a_t) on the train_batch
        train_batch.advantages = self._compute_adv(model, dist_class, train_batch)

        # compute the weights assigned to every transition (s_t, a_t)
        train_batch.action_weights = self._compute_action_weights(train_batch)

        # compute actor loss
        actor_loss = self._compute_actor_loss(train_batch, dist_class)

        ############ update the critic
        # standard critic update with pessimistic Q-learning (e.g. DQN)
        critic_loss = self._compute_critic_loss(model, dist_class, train_batch)

        return {'actor': actor_loss, 'critic': critic_loss}

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
        return []

    def _compute_action_weights(self, train_batch: SampleBatch):
        # uses bin|exp to compute action weights
        # 1(A>=0) or exp(A/temp)
        return None

    def _compute_actor_loss(self, train_batch: SampleBatch, dist_class: Type[TorchDistributionWrapper]):
        # loss = - (train_batch.weights * train_batch.logp).mean()
        return None

    def _compute_critic_loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch
    ):
        # target, use target model to compute the target
        q_next = model.value_function(train_batch)
        target = train_batch.reward + discount * q_next

        # cross entropy or MSE depending on the action space

        return loss



if __name__ == '__main__':

    obs_space = gym.spaces.Box(np.array((-1, -1)), np.array((1, 1)))
    act_space =  gym.spaces.Box(np.array((-1,-1)), np.array((1,1)))
    config = TrainerConfig().framework(framework='torch').to_dict()
    print(config['framework'])
    CRRTorchPolicy(obs_space, act_space, config=config)