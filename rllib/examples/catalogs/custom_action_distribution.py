# @HybridAPIStack

"""
This example shows two modifications:
1. How to write a custom action distribution
2. How to inject a custom action distribution into a Catalog
"""
# __sphinx_doc_begin__
import gymnasium as gym
import torch

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.distribution.distribution import Distribution
from ray.rllib.core.distribution.torch.torch_distribution import TorchDeterministic
from ray.rllib.core.rl_module.rl_module import RLModuleSpec


# Define a simple categorical distribution that can be used for PPO
class CustomTorchCategorical(Distribution):
    def __init__(self, logits):
        self.torch_dist = torch.distributions.categorical.Categorical(logits=logits)

    def sample(self, sample_shape=torch.Size(), **kwargs):
        return self.torch_dist.sample(sample_shape)

    def rsample(self, sample_shape=torch.Size(), **kwargs):
        return self._dist.rsample(sample_shape)

    def logp(self, value, **kwargs):
        return self.torch_dist.log_prob(value)

    def entropy(self):
        return self.torch_dist.entropy()

    def kl(self, other, **kwargs):
        return torch.distributions.kl.kl_divergence(self.torch_dist, other.torch_dist)

    @staticmethod
    def required_input_dim(space, **kwargs):
        return int(space.n)

    @classmethod
    # This method is used to create distributions from logits inside RLModules.
    # You can use this to inject arguments into the constructor of this distribution
    # that are not the logits themselves.
    def from_logits(cls, logits):
        return CustomTorchCategorical(logits=logits)

    # This method is used to create a deterministic distribution for the
    # DefaultPPORLModule.forward_inference.
    def to_deterministic(self):
        return TorchDeterministic(loc=torch.argmax(self.logits, dim=-1))


# See if we can create this distribution and sample from it to interact with our
# target environment
env = gym.make("CartPole-v1")
dummy_logits = torch.randn([env.action_space.n])
dummy_dist = CustomTorchCategorical.from_logits(dummy_logits)
action = dummy_dist.sample()
env = gym.make("CartPole-v1")
env.reset()
env.step(action.numpy())


# Define a simple catalog that returns our custom distribution when
# get_action_dist_cls is called
class CustomPPOCatalog(PPOCatalog):
    def get_action_dist_cls(self, framework):
        # The distribution we wrote will only work with torch
        assert framework == "torch"
        return CustomTorchCategorical


# Train with our custom action distribution
algo = (
    PPOConfig()
    .environment("CartPole-v1")
    .rl_module(rl_module_spec=RLModuleSpec(catalog_class=CustomPPOCatalog))
    .build()
)
results = algo.train()
print(results)
# __sphinx_doc_end__
