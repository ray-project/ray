import gymnasium as gym
import numpy as np
import shutil
import tempfile
import tree
import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModuleSpec,
    MultiAgentRLModule,
)
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.rllib.utils.numpy import convert_to_numpy


PPO_MODULES = {"tf2": PPOTfRLModule, "torch": PPOTorchRLModule}
NUM_AGENTS = 2


class TestE2ERLModuleLoad(unittest.TestCase):
    """Test RLModule loading from rl module spec across a multi node cluster."""

    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    @staticmethod
    def get_ppo_config(num_agents=NUM_AGENTS):
        def policy_mapping_fn(agent_id, episode, worker, **kwargs):
            # policy_id is policy_i where i is the agent id
            pol_id = f"policy_{agent_id}"
            return pol_id

        scaling_config = {
            "num_learner_workers": 2,
            "num_gpus_per_learner_worker": 1,
        }

        policies = {f"policy_{i}" for i in range(num_agents)}

        config = (
            PPOConfig()
            .experimental(_enable_new_api_stack=True)
            .rollouts(rollout_fragment_length=4)
            .environment(MultiAgentCartPole, env_config={"num_agents": num_agents})
            .training(num_sgd_iter=1, train_batch_size=8, sgd_minibatch_size=8)
            .multi_agent(policies=policies, policy_mapping_fn=policy_mapping_fn)
            .resources(**scaling_config)
        )
        return config

    def test_e2e_load_simple_marl_module(self):
        """Test if we can train a PPO algorithm with a checkpointed MARL module e2e."""
        config = self.get_ppo_config()
        env = MultiAgentCartPole({"num_agents": NUM_AGENTS})
        for fw in framework_iterator(config, frameworks=["tf2", "torch"]):
            # create a marl_module to load and save it to a checkpoint directory
            module_specs = {}
            module_class = PPO_MODULES[fw]
            for i in range(NUM_AGENTS):
                module_specs[f"policy_{i}"] = SingleAgentRLModuleSpec(
                    module_class=module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict={"fcnet_hiddens": [32 * (i + 1)]},
                    catalog_class=PPOCatalog,
                )
            marl_module_spec = MultiAgentRLModuleSpec(module_specs=module_specs)
            marl_module = marl_module_spec.build()
            marl_module_weights = convert_to_numpy(marl_module.get_state())
            marl_checkpoint_path = tempfile.mkdtemp()
            marl_module.save_to_checkpoint(marl_checkpoint_path)

            # create a new MARL_spec with the checkpoint from the previous one
            marl_module_spec_from_checkpoint = MultiAgentRLModuleSpec(
                module_specs=module_specs,
                load_state_path=marl_checkpoint_path,
            )
            config.experimental(_enable_new_api_stack=True)
            config.rl_module(rl_module_spec=marl_module_spec_from_checkpoint)

            # Create the algorithm with multiple nodes and check if the weights
            # are the same as the original MARL Module.
            algo = config.build()
            algo_module_weights = algo.learner_group.get_weights()
            check(algo_module_weights, marl_module_weights)
            algo.train()
            algo.stop()
            del algo
            shutil.rmtree(marl_checkpoint_path)

    def test_e2e_load_complex_marl_module(self):
        """Test if we can train a PPO algorithm with a cpkt MARL and RL module e2e."""
        config = self.get_ppo_config()
        env = MultiAgentCartPole({"num_agents": NUM_AGENTS})
        for fw in framework_iterator(config, frameworks=["tf2", "torch"]):
            # create a marl_module to load and save it to a checkpoint directory
            module_specs = {}
            module_class = PPO_MODULES[fw]
            for i in range(NUM_AGENTS):
                module_specs[f"policy_{i}"] = SingleAgentRLModuleSpec(
                    module_class=module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict={"fcnet_hiddens": [32 * (i + 1)]},
                    catalog_class=PPOCatalog,
                )
            marl_module_spec = MultiAgentRLModuleSpec(module_specs=module_specs)
            marl_module = marl_module_spec.build()
            marl_checkpoint_path = tempfile.mkdtemp()
            marl_module.save_to_checkpoint(marl_checkpoint_path)

            # create a RLModule to load and override the "policy_1" module with
            module_to_swap_in = SingleAgentRLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [64]},
                catalog_class=PPOCatalog,
            ).build()

            module_to_swap_in_path = tempfile.mkdtemp()
            module_to_swap_in.save_to_checkpoint(module_to_swap_in_path)

            # create a new MARL_spec with the checkpoint from the marl_checkpoint
            # and the module_to_swap_in_checkpoint
            module_specs["policy_1"] = SingleAgentRLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [64]},
                catalog_class=PPOCatalog,
                load_state_path=module_to_swap_in_path,
            )
            marl_module_spec_from_checkpoint = MultiAgentRLModuleSpec(
                module_specs=module_specs,
                load_state_path=marl_checkpoint_path,
            )
            config.experimental(_enable_new_api_stack=True)
            config.rl_module(rl_module_spec=marl_module_spec_from_checkpoint)

            # create the algorithm with multiple nodes and check if the weights
            # are the same as the original MARL Module
            algo = config.build()
            algo_module_weights = algo.learner_group.get_weights()

            marl_module_with_swapped_in_module = MultiAgentRLModule()
            marl_module_with_swapped_in_module.add_module(
                "policy_0", marl_module["policy_0"]
            )
            marl_module_with_swapped_in_module.add_module("policy_1", module_to_swap_in)

            check(
                algo_module_weights,
                convert_to_numpy(marl_module_with_swapped_in_module.get_state()),
            )
            algo.train()
            algo.stop()
            del algo
            shutil.rmtree(marl_checkpoint_path)

    def test_e2e_load_rl_module(self):
        """Test if we can train a PPO algorithm with a cpkt RL module e2e."""
        scaling_config = {
            "num_learner_workers": 2,
            "num_gpus_per_learner_worker": 1,
        }

        config = (
            PPOConfig()
            .experimental(_enable_new_api_stack=True)
            .rollouts(rollout_fragment_length=4)
            .environment("CartPole-v1")
            .training(num_sgd_iter=1, train_batch_size=8, sgd_minibatch_size=8)
            .resources(**scaling_config)
        )
        env = gym.make("CartPole-v1")
        for fw in framework_iterator(config, frameworks=["tf2", "torch"]):
            # create a marl_module to load and save it to a checkpoint directory
            module_class = PPO_MODULES[fw]
            module_spec = SingleAgentRLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
                catalog_class=PPOCatalog,
            )
            module = module_spec.build()

            module_ckpt_path = tempfile.mkdtemp()
            module.save_to_checkpoint(module_ckpt_path)

            module_to_load_spec = SingleAgentRLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
                catalog_class=PPOCatalog,
                load_state_path=module_ckpt_path,
            )

            config.experimental(_enable_new_api_stack=True)
            config.rl_module(rl_module_spec=module_to_load_spec)

            # create the algorithm with multiple nodes and check if the weights
            # are the same as the original MARL Module
            algo = config.build()
            algo_module_weights = algo.learner_group.get_weights()

            check(
                algo_module_weights[DEFAULT_POLICY_ID],
                convert_to_numpy(module.get_state()),
            )
            algo.train()
            algo.stop()
            del algo
            shutil.rmtree(module_ckpt_path)

    def test_e2e_load_complex_marl_module_with_modules_to_load(self):
        """Test if we can train a PPO algorithm with a cpkt MARL and RL module e2e.

        Additionally, check if we can set modules to load so that we can exclude
        a module from our ckpted MARL module from being loaded.

        """
        num_agents = 3
        config = self.get_ppo_config(num_agents=num_agents)
        env = MultiAgentCartPole({"num_agents": num_agents})
        for fw in framework_iterator(config, frameworks=["tf2", "torch"]):
            # create a marl_module to load and save it to a checkpoint directory
            module_specs = {}
            module_class = PPO_MODULES[fw]
            for i in range(num_agents):
                module_specs[f"policy_{i}"] = SingleAgentRLModuleSpec(
                    module_class=module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict={"fcnet_hiddens": [32 * (i + 1)]},
                    catalog_class=PPOCatalog,
                )
            marl_module_spec = MultiAgentRLModuleSpec(module_specs=module_specs)
            marl_module = marl_module_spec.build()
            marl_checkpoint_path = tempfile.mkdtemp()
            marl_module.save_to_checkpoint(marl_checkpoint_path)

            # create a RLModule to load and override the "policy_1" module with
            module_to_swap_in = SingleAgentRLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [64]},
                catalog_class=PPOCatalog,
            ).build()

            module_to_swap_in_path = tempfile.mkdtemp()
            module_to_swap_in.save_to_checkpoint(module_to_swap_in_path)

            # create a new MARL_spec with the checkpoint from the marl_checkpoint
            # and the module_to_swap_in_checkpoint
            module_specs["policy_1"] = SingleAgentRLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [64]},
                catalog_class=PPOCatalog,
                load_state_path=module_to_swap_in_path,
            )
            marl_module_spec_from_checkpoint = MultiAgentRLModuleSpec(
                module_specs=module_specs,
                load_state_path=marl_checkpoint_path,
                modules_to_load={
                    "policy_0",
                },
            )
            config.experimental(_enable_new_api_stack=True)
            config.rl_module(rl_module_spec=marl_module_spec_from_checkpoint)

            # create the algorithm with multiple nodes and check if the weights
            # are the same as the original MARL Module
            algo = config.build()
            algo_module_weights = algo.learner_group.get_weights()

            # weights of "policy_0" should be the same as in the loaded marl module
            # since we specified it as being apart of the modules_to_load
            check(
                algo_module_weights["policy_0"],
                convert_to_numpy(marl_module["policy_0"].get_state()),
            )
            # weights of "policy_1" should be the same as in the module_to_swap_in since
            # we specified its load path separately in an rl_module_spec inside of the
            # marl_module_spec_from_checkpoint
            check(
                algo_module_weights["policy_1"],
                convert_to_numpy(module_to_swap_in.get_state()),
            )
            # weights of "policy_2" should be different from the loaded marl module
            # since we didn't specify it as being apart of the modules_to_load
            policy_2_algo_module_weight_sum = np.sum(
                [
                    np.sum(s)
                    for s in tree.flatten(
                        convert_to_numpy(algo_module_weights["policy_2"])
                    )
                ]
            )
            policy_2_marl_module_weight_sum = np.sum(
                [
                    np.sum(s)
                    for s in tree.flatten(
                        convert_to_numpy(marl_module["policy_2"].get_state())
                    )
                ]
            )
            check(
                policy_2_algo_module_weight_sum,
                policy_2_marl_module_weight_sum,
                false=True,
            )

            algo.train()
            algo.stop()
            del algo
            shutil.rmtree(marl_checkpoint_path)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
