import gym
import torch

import ray
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import FCConfig
from ray.rllib.core.optim.marl_optimizer import DefaultMARLOptimizer
from ray.rllib.core.optim.tests.test_rl_optimizer import BCTorchModule, BCTorchOptimizer


ray.init()
torch.manual_seed(1)
env = gym.make("CartPole-v1")
module_config = FCConfig(
    input_dim=sum(env.observation_space.shape),
    output_dim=sum(env.action_space.shape or [env.action_space.n]),
    hidden_layers=[32],
)
module = BCTorchModule(module_config).as_multi_agent()

optim = DefaultMARLOptimizer(module, BCTorchOptimizer, {})

print(optim.get_state())
