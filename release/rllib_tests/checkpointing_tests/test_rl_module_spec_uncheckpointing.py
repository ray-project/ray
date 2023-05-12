import gymnasium as gym
import tempfile
import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.rllib.utils.numpy import convert_to_numpy


MODULES = [DiscreteBCTorchModule, DiscreteBCTFModule]
PPO_MODULES = {"tf2": PPOTfRLModule, "torch": PPOTorchRLModule}


@ray.remote(num_gpus=1)
def uncheckpoint_module_and_get_state(marl_module_spec):
    # This function will be run on a separate ray node to test if the checkpoint is
    # being copied across nodes correctly.
    marl_module = marl_module_spec.build()
    return marl_module.get_state()


class TestRLModuleSpecUncheckpointing(unittest.TestCase):
    """Test RLModule Spec uncheckpointing across a multi node cluster."""

    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_uncheckpoint_different_node(self):
        """Test if building a MultiAgentRLModule with heterogeneous checkpoints works.

        This tests if we can load some modules from single agent rl modules and the rest
        from a multi agent rl module checkpoint.

        """
        env = gym.make("CartPole-v1")
        num_agents = 2
        for module_class in MODULES:
            module_specs = {}
            for i in range(num_agents):
                module_specs[f"module_{i}"] = SingleAgentRLModuleSpec(
                    module_class=module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict={"fcnet_hiddens": [32 * (i + 1)]},
                )

            marl_spec = MultiAgentRLModuleSpec(module_specs=module_specs)
            marl_module = marl_spec.build()
            self.assertIsInstance(marl_module, MultiAgentRLModule)

            single_agent_rl_spec = module_specs["module_1"]
            single_agent_rl_module = single_agent_rl_spec.build()
            self.assertIsInstance(single_agent_rl_module, module_class)

            with tempfile.TemporaryDirectory() as single_agent_checkpoint_path:
                single_agent_rl_module.save_to_checkpoint(single_agent_checkpoint_path)

                spec_with_load_path = MultiAgentRLModuleSpec(
                    module_specs=module_specs,
                    load_state_path=single_agent_checkpoint_path,
                )
                with tempfile.TemporaryDirectory() as marl_checkpoint_path:
                    marl_module.save_to_checkpoint(marl_checkpoint_path)

                    module_specs["module_1"] = SingleAgentRLModuleSpec(
                        module_class=module_class,
                        observation_space=env.observation_space,
                        action_space=env.action_space,
                        model_config_dict={"fcnet_hiddens": [32 * (2)]},
                        load_state_path=single_agent_checkpoint_path,
                    )

                    spec_with_load_path = MultiAgentRLModuleSpec(
                        module_specs=module_specs,
                        load_state_path=marl_checkpoint_path,
                    )
                    module_from_checkpoint_state = ray.get(
                        uncheckpoint_module_and_get_state.remote(spec_with_load_path)
                    )
                    # module 0 in the loaded marl_module should be the same as module_0
                    # in the original module
                    check(
                        marl_module.get_state()["module_0"],
                        module_from_checkpoint_state["module_0"],
                    )
                    # module 1 in the loaded marl_module should have the same state as
                    # the single_agent_rl_module, not the original marl_module
                    check(
                        single_agent_rl_module.get_state(),
                        module_from_checkpoint_state["module_1"],
                    )

    def test_e2e_uncheckpointing_test(self):
        """Test if we can build a PPO algorithm with a checkpointed MARL module e2e."""

        # number of agents for the multi agent cartpole env
        num_agents = 2

        env = MultiAgentCartPole({"num_agents": num_agents})

        def policy_mapping_fn(agent_id, episode, worker, **kwargs):
            # policy_id is policy_i where i is the agent id
            pol_id = f"policy_{agent_id}"
            return pol_id

        scaling_config = {
            "num_learner_workers": 2,
            "num_gpus_per_learner_worker": 1,
        }

        config = (
            PPOConfig()
            .rollouts(rollout_fragment_length=4)
            .environment(MultiAgentCartPole, env_config={"num_agents": num_agents})
            .training(num_sgd_iter=1, train_batch_size=8, sgd_minibatch_size=8)
            .multi_agent(
                policies={"policy_0", "policy_1"}, policy_mapping_fn=policy_mapping_fn
            )
            .training(_enable_learner_api=True)
            .resources(**scaling_config)
        )
        for fw in framework_iterator(
            config, frameworks=["tf2", "torch"], with_eager_tracing=True
        ):
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

            # create a separate MARL_spec to make a checkpoint
            marl_module_spec = MultiAgentRLModuleSpec(module_specs=module_specs)
            marl_module = marl_module_spec.build()
            marl_module_weights = convert_to_numpy(marl_module.get_state())

            with tempfile.TemporaryDirectory() as marl_checkpoint_path:
                marl_module.save_to_checkpoint(marl_checkpoint_path)

                # create a new MARL_spec with the checkpoint from the previous one
                marl_module_spec_from_checkpoint = MultiAgentRLModuleSpec(
                    module_specs=module_specs,
                    load_state_path=marl_checkpoint_path,
                )
                config = config.rl_module(
                    rl_module_spec=marl_module_spec_from_checkpoint,
                    _enable_rl_module_api=True,
                )

                # create the algorithm with multiple nodes and check if the weights
                # are the same as the original MARL Module
                algo = config.build()
                algo_module_weights = algo.learner_group.get_weights()
                check(algo_module_weights, marl_module_weights)
                algo.train()
                algo.stop()
                del algo


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
