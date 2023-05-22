import tempfile
import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModuleSpec,
)
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.rllib.utils.numpy import convert_to_numpy


MODULES = [DiscreteBCTorchModule, DiscreteBCTFModule]
PPO_MODULES = {"tf2": PPOTfRLModule, "torch": PPOTorchRLModule}


class TestE2ERLModuleRestore(unittest.TestCase):
    """Test RLModule Spec uncheckpointing across a multi node cluster."""

    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

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
