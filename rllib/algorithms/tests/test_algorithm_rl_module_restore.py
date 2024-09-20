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
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.core.rl_module.multi_rl_module import (
    MultiRLModuleSpec,
    MultiRLModule,
)
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.numpy import convert_to_numpy


PPO_MODULES = {"tf2": PPOTfRLModule, "torch": PPOTorchRLModule}
NUM_AGENTS = 2


class TestAlgorithmRLModuleRestore(unittest.TestCase):
    """Test RLModule loading from rl module spec across a local node."""

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
            "num_learners": 0,
            "num_gpus_per_learner": 0,
        }

        policies = {f"policy_{i}" for i in range(num_agents)}

        config = (
            PPOConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .env_runners(rollout_fragment_length=4)
            .learners(**scaling_config)
            .environment(MultiAgentCartPole, env_config={"num_agents": num_agents})
            .training(num_epochs=1, train_batch_size=8, minibatch_size=8)
            .multi_agent(policies=policies, policy_mapping_fn=policy_mapping_fn)
        )
        return config

    def test_e2e_load_simple_multi_rl_module(self):
        """Test if we can train a PPO algo with a checkpointed MultiRLModule e2e."""
        config = self.get_ppo_config()
        env = MultiAgentCartPole({"num_agents": NUM_AGENTS})
        # create a multi_rl_module to load and save it to a checkpoint directory
        module_specs = {}
        module_class = PPO_MODULES["torch"]
        for i in range(NUM_AGENTS):
            module_specs[f"policy_{i}"] = RLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space[0],
                action_space=env.action_space[0],
                # If we want to use this externally created module in the algorithm,
                # we need to provide the same config as the algorithm.
                model_config_dict=config.model_config
                | {"fcnet_hiddens": [32 * (i + 1)]},
                catalog_class=PPOCatalog,
            )
        multi_rl_module_spec = MultiRLModuleSpec(module_specs=module_specs)
        multi_rl_module = multi_rl_module_spec.build()
        multi_rl_module_weights = convert_to_numpy(multi_rl_module.get_state())
        marl_checkpoint_path = tempfile.mkdtemp()
        multi_rl_module.save_to_path(marl_checkpoint_path)

        # create a new MARL_spec with the checkpoint from the previous one
        multi_rl_module_spec_from_checkpoint = MultiRLModuleSpec(
            module_specs=module_specs,
            load_state_path=marl_checkpoint_path,
        )
        config = config.api_stack(enable_rl_module_and_learner=True).rl_module(
            rl_module_spec=multi_rl_module_spec_from_checkpoint,
        )

        # create the algorithm with multiple nodes and check if the weights
        # are the same as the original MultiRLModule
        algo = config.build()
        algo_module_weights = algo.learner_group.get_weights()
        check(algo_module_weights, multi_rl_module_weights)
        algo.train()
        algo.stop()
        del algo
        shutil.rmtree(marl_checkpoint_path)

    def test_e2e_load_complex_multi_rl_module(self):
        """Test if we can train a PPO algorithm with a cpkt MARL and RL module e2e."""
        config = self.get_ppo_config()
        env = MultiAgentCartPole({"num_agents": NUM_AGENTS})
        # create a multi_rl_module to load and save it to a checkpoint directory
        module_specs = {}
        module_class = PPO_MODULES["torch"]
        for i in range(NUM_AGENTS):
            module_specs[f"policy_{i}"] = RLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space[0],
                action_space=env.action_space[0],
                # If we want to use this externally created module in the algorithm,
                # we need to provide the same config as the algorithm.
                model_config_dict=config.model_config
                | {"fcnet_hiddens": [32 * (i + 1)]},
                catalog_class=PPOCatalog,
            )
        multi_rl_module_spec = MultiRLModuleSpec(module_specs=module_specs)
        multi_rl_module = multi_rl_module_spec.build()
        marl_checkpoint_path = tempfile.mkdtemp()
        multi_rl_module.save_to_path(marl_checkpoint_path)

        # create a RLModule to load and override the "policy_1" module with
        module_to_swap_in = RLModuleSpec(
            module_class=module_class,
            observation_space=env.observation_space[0],
            action_space=env.action_space[0],
            # Note, we need to pass in the default model config for the algorithm
            # to be able to use this module later.
            model_config_dict=config.model_config | {"fcnet_hiddens": [64]},
            catalog_class=PPOCatalog,
        ).build()

        module_to_swap_in_path = tempfile.mkdtemp()
        module_to_swap_in.save_to_path(module_to_swap_in_path)

        # create a new MARL_spec with the checkpoint from the marl_checkpoint
        # and the module_to_swap_in_checkpoint
        module_specs["policy_1"] = RLModuleSpec(
            module_class=module_class,
            observation_space=env.observation_space[0],
            action_space=env.action_space[0],
            model_config_dict={"fcnet_hiddens": [64]},
            catalog_class=PPOCatalog,
            load_state_path=module_to_swap_in_path,
        )
        multi_rl_module_spec_from_checkpoint = MultiRLModuleSpec(
            module_specs=module_specs,
            load_state_path=marl_checkpoint_path,
        )
        config = config.api_stack(enable_rl_module_and_learner=True).rl_module(
            rl_module_spec=multi_rl_module_spec_from_checkpoint,
        )

        # create the algorithm with multiple nodes and check if the weights
        # are the same as the original MultiRLModule
        algo = config.build()
        algo_module_weights = algo.learner_group.get_weights()

        multi_rl_module_with_swapped_in_module = MultiRLModule()
        multi_rl_module_with_swapped_in_module.add_module(
            "policy_0", multi_rl_module["policy_0"]
        )
        multi_rl_module_with_swapped_in_module.add_module("policy_1", module_to_swap_in)

        check(
            algo_module_weights,
            convert_to_numpy(multi_rl_module_with_swapped_in_module.get_state()),
        )
        algo.train()
        algo.stop()
        del algo
        shutil.rmtree(marl_checkpoint_path)

    def test_e2e_load_rl_module(self):
        """Test if we can train a PPO algorithm with a cpkt RL module e2e."""
        scaling_config = {
            "num_learners": 0,
            "num_gpus_per_learner": 0,
        }

        config = (
            PPOConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .env_runners(rollout_fragment_length=4)
            .learners(**scaling_config)
            .environment("CartPole-v1")
            .training(num_epochs=1, train_batch_size=8, minibatch_size=8)
        )
        env = gym.make("CartPole-v1")
        # create a multi_rl_module to load and save it to a checkpoint directory
        module_class = PPO_MODULES["torch"]
        module_spec = RLModuleSpec(
            module_class=module_class,
            observation_space=env.observation_space,
            action_space=env.action_space,
            # If we want to use this externally created module in the algorithm,
            # we need to provide the same config as the algorithm.
            model_config_dict=config.model_config | {"fcnet_hiddens": [32]},
            catalog_class=PPOCatalog,
        )
        module = module_spec.build()

        module_ckpt_path = tempfile.mkdtemp()
        module.save_to_path(module_ckpt_path)

        module_to_load_spec = RLModuleSpec(
            module_class=module_class,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config_dict={"fcnet_hiddens": [32]},
            catalog_class=PPOCatalog,
            load_state_path=module_ckpt_path,
        )

        config = config.api_stack(enable_rl_module_and_learner=True).rl_module(
            rl_module_spec=module_to_load_spec,
        )

        # create the algorithm with multiple nodes and check if the weights
        # are the same as the original MultiRLModule
        algo = config.build()
        algo_module_weights = algo.learner_group.get_weights()

        check(
            algo_module_weights[DEFAULT_MODULE_ID],
            convert_to_numpy(module.get_state()),
        )
        algo.train()
        algo.stop()
        del algo
        shutil.rmtree(module_ckpt_path)

    def test_e2e_load_complex_multi_rl_module_with_modules_to_load(self):
        """Test if we can train a PPO algorithm with a cpkt MARL and RL module e2e.

        Additionally, check if we can set modules to load so that we can exclude
        a module from our ckpted MultiRLModule from being loaded.

        """
        num_agents = 3
        config = self.get_ppo_config(num_agents=num_agents)
        env = MultiAgentCartPole({"num_agents": num_agents})
        # create a multi_rl_module to load and save it to a checkpoint directory
        module_specs = {}
        module_class = PPO_MODULES["torch"]
        for i in range(num_agents):
            module_specs[f"policy_{i}"] = RLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space[0],
                action_space=env.action_space[0],
                # Note, we need to pass in the default model config for the
                # algorithm to be able to use this module later.
                model_config_dict=config.model_config
                | {"fcnet_hiddens": [32 * (i + 1)]},
                catalog_class=PPOCatalog,
            )
        multi_rl_module_spec = MultiRLModuleSpec(module_specs=module_specs)
        multi_rl_module = multi_rl_module_spec.build()
        marl_checkpoint_path = tempfile.mkdtemp()
        multi_rl_module.save_to_path(marl_checkpoint_path)

        # create a RLModule to load and override the "policy_1" module with
        module_to_swap_in = RLModuleSpec(
            module_class=module_class,
            observation_space=env.observation_space[0],
            action_space=env.action_space[0],
            # Note, we need to pass in the default model config for the algorithm
            # to be able to use this module later.
            model_config_dict=config.model_config | {"fcnet_hiddens": [64]},
            catalog_class=PPOCatalog,
        ).build()

        module_to_swap_in_path = tempfile.mkdtemp()
        module_to_swap_in.save_to_path(module_to_swap_in_path)

        # create a new MARL_spec with the checkpoint from the marl_checkpoint
        # and the module_to_swap_in_checkpoint
        module_specs["policy_1"] = RLModuleSpec(
            module_class=module_class,
            observation_space=env.observation_space[0],
            action_space=env.action_space[0],
            model_config_dict={"fcnet_hiddens": [64]},
            catalog_class=PPOCatalog,
            load_state_path=module_to_swap_in_path,
        )
        multi_rl_module_spec_from_checkpoint = MultiRLModuleSpec(
            module_specs=module_specs,
            load_state_path=marl_checkpoint_path,
            modules_to_load={
                "policy_0",
            },
        )
        config = config.api_stack(enable_rl_module_and_learner=True).rl_module(
            rl_module_spec=multi_rl_module_spec_from_checkpoint,
        )

        # create the algorithm with multiple nodes and check if the weights
        # are the same as the original MultiRLModule
        algo = config.build()
        algo_module_weights = algo.learner_group.get_weights()

        # weights of "policy_0" should be the same as in the loaded MultiRLModule
        # since we specified it as being apart of the modules_to_load
        check(
            algo_module_weights["policy_0"],
            convert_to_numpy(multi_rl_module["policy_0"].get_state()),
        )
        # weights of "policy_1" should be the same as in the module_to_swap_in since
        # we specified its load path separately in an rl_module_spec inside of the
        # multi_rl_module_spec_from_checkpoint
        check(
            algo_module_weights["policy_1"],
            convert_to_numpy(module_to_swap_in.get_state()),
        )
        # weights of "policy_2" should be different from the loaded MultiRLModule
        # since we didn't specify it as being apart of the modules_to_load
        policy_2_algo_module_weight_sum = np.sum(
            [
                np.sum(s)
                for s in tree.flatten(convert_to_numpy(algo_module_weights["policy_2"]))
            ]
        )
        policy_2_multi_rl_module_weight_sum = np.sum(
            [
                np.sum(s)
                for s in tree.flatten(
                    convert_to_numpy(multi_rl_module["policy_2"].get_state())
                )
            ]
        )
        check(
            policy_2_algo_module_weight_sum,
            policy_2_multi_rl_module_weight_sum,
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
