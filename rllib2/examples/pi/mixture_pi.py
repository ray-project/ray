###################################################################
########### Mixed action space policy
# action_space = {'torques': Box(-1, 1)^6, 'gripper': Discrete(n=2)}
# a ~ MixedDistribution({
#   'torques' ~ squashedNorm(mu(s), std(s)),
#   'gripper' ~ Categorical([p1(s), p2(s)])
# })
# example usage of the mixed_dist output
# >>> actor_loss = mixed_dist.log_prob(a)
# >>> {'torques': Tensor(0.63), 'gripper': Tensor(0.63)}
###################################################################

# see how easy it is to extend the base pi class to support mixed action spaces
from gym.spaces import Box, Tuple, Discrete

from rllib2.models.torch.pi_distribution import PiDistribution, combine_distributions
from rllib.models.torch.torch_action_dist import TorchCategorical, TorchSquashedGaussian
from rllib2.examples.pi.normal_pi import PiNormalDistribution
from rllib2.models.types import SpecDict, Spec, TensorDict

class PiCategoricalDistribution(PiDistribution):
    def __init__(self, inputs: TensorDict, config: PiDistConfig):
        self.config = config
        self.dist = TorchCategorical(inputs["logits"])

    def behavioral_sample(self, shape):
        return self.dist.sample()

    def target_sample(self, shape):
        return self.dist.deterministic_sample()

    def input_spec(self) -> SpecDict:
        return SpecDict(
            {
                "logits": Spec(
                    shape="b a", b=self.config.batch_size, h=self.config.act_dim
                )
            }
        )

dist_cls = combine_distributions(
    {"torques": PiNormalDistribution, "gripper": PiCategoricalDistribution}
)


config = PiConfig(
    observation_space=Box(low=-1, high=1, shape=(10,)),
    action_space=Tuple((Box(low=-1, high=1, shape=(1,)), Discrete(2)),
    is_deterministic=False,
    free_log_std=True,
    action_dist_class=dist_cls,
    # squash_actions=True,
)
