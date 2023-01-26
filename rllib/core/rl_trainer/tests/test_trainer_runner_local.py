import gymnasium as gym
import unittest

from ray.rllib.utils.framework import try_import_tf
import ray

from ray.rllib.core.rl_trainer.trainer_runner import TrainerRunner
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_rl_trainer import BCTfRLTrainer
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, MultiAgentBatch
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader
from ray.rllib.core.testing.utils import add_module_to_runner_or_trainer


tf1, tf, tfv = try_import_tf()
tf1.executing_eagerly()


class TestTrainerRunnerLocal(unittest.TestCase):
    """This test is a trainer test setup for no gpus."""

    # TODO: Make a unittest that does not need 2 gpus to run.
    # So that the user can run it locally as well.
    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_trainer_runner_no_gpus(self):
        env = gym.make("CartPole-v1")
        trainer_class = BCTfRLTrainer
        trainer_cfg = dict(
            module_class=DiscreteBCTFModule,
            module_kwargs={
                "observation_space": env.observation_space,
                "action_space": env.action_space,
                "model_config": {"hidden_dim": 32},
            },
            optimizer_config={"lr": 1e-3},
        )
        runner = TrainerRunner(
            trainer_class, trainer_cfg, compute_config=dict(num_gpus=0)
        )

        local_trainer = trainer_class(**trainer_cfg)
        local_trainer.build()

        # make the state of the trainer and the local runner identical
        local_trainer.set_state(runner.get_state()[0])

        reader = get_cartpole_dataset_reader(batch_size=500)
        batch = reader.next()
        batch = batch.as_multi_agent()
        check(local_trainer.update(batch), runner.update(batch)[0])

        new_module_id = "test_module"

        add_module_to_runner_or_trainer("tf", env, new_module_id, runner)
        add_module_to_runner_or_trainer("tf", env, new_module_id, local_trainer)

        # make the state of the trainer and the local runner identical
        local_trainer.set_state(runner.get_state()[0])

        # do another update
        batch = reader.next()
        ma_batch = MultiAgentBatch(
            {new_module_id: batch, DEFAULT_POLICY_ID: batch}, env_steps=batch.count
        )
        check(local_trainer.update(ma_batch), runner.update(ma_batch)[0])

        check(local_trainer.get_state(), runner.get_state()[0])


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
