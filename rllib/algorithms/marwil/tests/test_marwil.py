import unittest
from pathlib import Path

import gymnasium as gym
import numpy as np

import ray
import ray.rllib.algorithms.marwil as marwil
from ray.rllib.core import COMPONENT_RL_MODULE, DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.utils import unflatten_dict
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import check

torch, _ = try_import_torch()


class TestMARWIL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_marwil_compilation_discrete_actions(self):
        """Test whether a MARWILAlgorithm can be built with all frameworks.

        Learns from a historic-data file.
        To generate this data, first run:
        $ ./train.py --run=PPO --env=CartPole-v1 \
          --stop='{"timesteps_total": 50000}' \
          --config='{"output": "/tmp/out", "batch_mode": "complete_episodes"}'
        """
        data_path = "tests/data/cartpole/cartpole-v1_large"
        base_path = Path(__file__).parents[3]
        print(f"base_path={base_path}")
        data_path = "local://" / base_path / data_path
        print(f"data_path={data_path}")

        config = (
            marwil.MARWILConfig()
            .environment(env="CartPole-v1")
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .offline_data(
                input_=[data_path.as_posix()],
                dataset_num_iters_per_learner=1,
                input_read_method_kwargs={"override_num_blocks": 2},
                map_batches_kwargs={"concurrency": 2, "num_cpus": 2},
                iter_batches_kwargs={"prefetch_batches": 1},
            )
            .training(
                lr=0.0008,
                train_batch_size_per_learner=2000,
                beta=0.5,
            )
            .evaluation(
                evaluation_interval=3,
                evaluation_num_env_runners=1,
                evaluation_duration=5,
                evaluation_parallel_to_training=True,
            )
        )

        num_iterations = 3

        algo = config.build()
        for i in range(num_iterations):
            print(algo.train())
        algo.stop()

    def test_marwil_compilation_cont_actions(self):
        """Test whether MARWIL runs with cont. actions.

        Learns from a historic-data file.
        """
        data_path = "tests/data/pendulum/pendulum-v1_large"
        base_path = Path(__file__).parents[3]
        print(f"base_path={base_path}")
        data_path = "local://" + base_path.joinpath(data_path).as_posix()
        print(f"data_path={data_path}")

        config = (
            marwil.MARWILConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment(env="Pendulum-v1")
            .env_runners(num_env_runners=1)
            .training(
                train_batch_size_per_learner=2000,
            )
            .offline_data(
                # Learn from offline data.
                input_=[data_path],
                dataset_num_iters_per_learner=1,
                input_read_method_kwargs={"override_num_blocks": 2},
                map_batches_kwargs={"concurrency": 2, "num_cpus": 2},
                iter_batches_kwargs={"prefetch_batches": 1},
            )
            # Evaluate on actual environment.
            .evaluation(
                evaluation_num_env_runners=1,
                evaluation_interval=3,
                evaluation_duration=5,
                evaluation_parallel_to_training=True,
            )
        )

        num_iterations = 3

        algo = config.build()
        for i in range(num_iterations):
            print(algo.train())
        algo.stop()

    def test_marwil_loss_function(self):
        """Test MARWIL's loss function."""

        data_path = "tests/data/cartpole/cartpole-v1_large"
        base_path = Path(__file__).parents[3]
        print(f"base_path={base_path}")
        data_path = "local://" + base_path.joinpath(data_path).as_posix()
        print(f"data_path={data_path}")

        config = (
            marwil.MARWILConfig()
            .environment(
                observation_space=gym.spaces.Box(
                    np.array([-4.8, -np.inf, -0.41887903, -np.inf]),
                    np.array([4.8, np.inf, 0.41887903, np.inf]),
                    (4,),
                    np.float32,
                ),
                action_space=gym.spaces.Discrete(2),
            )
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .offline_data(
                input_=[data_path],
                dataset_num_iters_per_learner=1,
            )
            .training(
                train_batch_size_per_learner=2000,
            )
        )  # Learn from offline data.

        algo = config.build(env="CartPole-v1")

        # Sample a batch from the offline data.
        batch = algo.offline_data.data.take_batch(2000)

        # Get the module state.
        module_state = algo.offline_data.learner_handles[0].get_state(
            component=COMPONENT_RL_MODULE,
        )[COMPONENT_RL_MODULE]

        # Create the prelearner and compute advantages and values.
        offline_prelearner = OfflinePreLearner(
            config=config,
            module_spec=algo.offline_data.module_spec,
            module_state=module_state,
            spaces=algo.offline_data.spaces[INPUT_ENV_SPACES],
        )
        # Note, for `ray.data`'s pipeline everything has to be a dictionary
        # therefore the batch is embedded into another dictionary.
        batch = unflatten_dict(offline_prelearner(batch))
        if Columns.LOSS_MASK in batch[DEFAULT_MODULE_ID]:
            loss_mask = (
                batch[DEFAULT_MODULE_ID][Columns.LOSS_MASK].detach().cpu().numpy()
            )
            num_valid = np.sum(loss_mask)

            def possibly_masked_mean(data_):
                return np.sum(data_[loss_mask]) / num_valid

        else:
            possibly_masked_mean = np.mean

        # Calculate our own expected values (to then compare against the
        # agent's loss output).
        module = algo.learner_group._learner.module[DEFAULT_MODULE_ID].unwrapped()
        fwd_out = module.forward_train(dict(batch[DEFAULT_MODULE_ID]))
        advantages = (
            batch[DEFAULT_MODULE_ID][Columns.VALUE_TARGETS].detach().cpu().numpy()
            - module.compute_values(batch[DEFAULT_MODULE_ID]).detach().cpu().numpy()
        )
        advantages_squared = possibly_masked_mean(np.square(advantages))
        c_2 = 100.0 + 1e-8 * (advantages_squared - 100.0)
        c = np.sqrt(c_2)
        exp_advantages = np.exp(config.beta * (advantages / c))
        action_dist_cls = (
            algo.learner_group._learner.module[DEFAULT_MODULE_ID]
            .unwrapped()
            .get_train_action_dist_cls()
        )
        # Note we need the actual model's logits not the ones from the data set
        # stored in `batch[Columns.ACTION_DIST_INPUTS]`.
        action_dist = action_dist_cls.from_logits(fwd_out[Columns.ACTION_DIST_INPUTS])
        logp = action_dist.logp(batch[DEFAULT_MODULE_ID][Columns.ACTIONS])
        logp = logp.detach().cpu().numpy()

        # Calculate all expected loss components.
        expected_vf_loss = 0.5 * advantages_squared
        expected_pol_loss = -1.0 * possibly_masked_mean(exp_advantages * logp)
        expected_loss = expected_pol_loss + config.vf_coeff * expected_vf_loss

        # Calculate the algorithm's loss (to check against our own
        # calculation above).
        total_loss = algo.learner_group._learner.compute_loss_for_module(
            module_id=DEFAULT_MODULE_ID,
            batch=dict(batch[DEFAULT_MODULE_ID]),
            fwd_out=fwd_out,
            config=config,
        )
        learner_results = algo.learner_group._learner.metrics.peek(DEFAULT_MODULE_ID)

        # Check all components.
        check(learner_results[VF_LOSS_KEY], expected_vf_loss, decimals=4)
        check(learner_results[POLICY_LOSS_KEY], expected_pol_loss, decimals=4)
        # Check the total loss.
        check(total_loss, expected_loss, decimals=3)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
