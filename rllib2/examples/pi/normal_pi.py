################################################################
###########  a ~ N(mu(s), std(s))
################################################################

from typing import List

from gym.spaces import Box
import torch

from rllib2.models.torch.pi import Pi
from rllib2.models.torch.pi_distribution import PiDistribution
from rllib.models.torch.torch_action_dist import TorchDiagGaussian
from rllib2.models.types import TensorDict, SpecDict, Spec


# Instead of using TorchDiagGaussian, we could also just copy/paste the code
# into a PiDistribution class with some minor modifications, like so

class PiNormalDistribution(PiDistribution):
    def __init__(self, inputs: TensorDict, config: PiDistConfig):
        self.config = config
        if config.free_log_std:
            self.dist = TorchDiagGaussian(inputs["mean"], inputs["variance"])
        else:
            self.dist = TorchDiagGaussian(inputs["mean"], self.input_spec()["mean"].sample(fill_value=config.log_std))

    def behavioral_sample(self, shape):
        return self.dist.sample()

    def target_sample(self, shape):
        return self.dist.mean

    def input_spec(self) -> SpecDict:
        return SpecDict(
            {
                "mean": Spec(
                    shape="b a", b=self.config.batch_size, h=self.config.act_dim
                ),
                "variance": Spec(
                    shape="b a", b=self.config.batch_size, h=self.config.act_dim
                ),
            }
        )

config = PiConfig(
    observation_space=Box(low=-1, high=1, shape=(10,)),
    action_space=Box(low=-1, high=1, shape=(2,)),
    is_deterministic=False,
    free_log_std=True,
    # Pi._make_action_dist will apparently just read the config and return the
    # correct torch.Distribution object
    # But we can also hard-code a specific distribution
    action_dist_class=PiNormalDistribution,
    squash_actions=True,
)
pi = Pi(config)
print(pi)
