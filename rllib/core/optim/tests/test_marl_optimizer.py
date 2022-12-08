import gym

from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import FCConfig
from ray.rllib.core.optim.marl_optimizer import DefaultMARLOptimizer
from ray.rllib.core.optim.tests.test_rl_optimizer import BCTorchModule, BCTorchOptimizer
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import check


class TestDefaultMarlOptimizer:
    def test_default_marl_optimizer_set_state_get_state(self):
        env = gym.make("CartPole-v1")
        module_config = FCConfig(
            input_dim=sum(env.observation_space.shape),
            output_dim=sum(env.action_space.shape or [env.action_space.n]),
            hidden_layers=[32],
        )
        module = BCTorchModule(module_config).as_multi_agent()
        module_2 = BCTorchModule(module_config).as_multi_agent()

        optim1 = DefaultMARLOptimizer(module, BCTorchOptimizer, {"lr": 0.01})
        optim2 = DefaultMARLOptimizer(
            module_2,
            {DEFAULT_POLICY_ID: BCTorchOptimizer},
            {DEFAULT_POLICY_ID: {"lr": 0.02}},
        )

        check(
            optim1.get_state()[DEFAULT_POLICY_ID]["module"]["param_groups"][0]["lr"],
            optim2.get_state()[DEFAULT_POLICY_ID]["module"]["param_groups"][0]["lr"],
            false=True,
        )
        optim2.set_state(optim1.get_state())
        check(
            optim2.get_state()[DEFAULT_POLICY_ID]["module"]["param_groups"][0]["lr"],
            0.01,
        )
