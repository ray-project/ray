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

        breakpoint()

    # def make_model(self) -> ModelV2:
    #     pass

if __name__ == '__main__':

    obs_space = gym.spaces.Box(np.array((-1, -1)), np.array((1, 1)))
    act_space =  gym.spaces.Box(np.array((-1,-1)), np.array((1,1)))
    config = TrainerConfig().framework(framework='torch').to_dict()
    print(config['framework'])
    CRRTorchPolicy(obs_space, act_space, config=config)