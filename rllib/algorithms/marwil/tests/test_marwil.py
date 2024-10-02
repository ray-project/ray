import numpy as np
from pathlib import Path
import unittest

import ray
import ray.rllib.algorithms.marwil as marwil
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY
from ray.rllib.offline.offline_prelearner import OfflinePreLearner
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
)
from ray.rllib.utils.test_utils import check

torch, _ = try_import_torch()


class TestMARWIL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_marwil_compilation_and_learning_from_offline_file(self):
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
            .evaluation(
                evaluation_interval=3,
                evaluation_num_env_runners=1,
                evaluation_duration=5,
                evaluation_parallel_to_training=True,
            )
            .offline_data(
                input_=[data_path.as_posix()],
                dataset_num_iters_per_learner=1,
            )
            .training(
                lr=0.0008,
                train_batch_size_per_learner=2000,
                beta=0.5,
            )
        )

        num_iterations = 350
        min_reward = 100.0

        algo = config.build()
        learnt = False
        for i in range(num_iterations):
            results = algo.train()
            print(results)

            eval_results = results.get(EVALUATION_RESULTS, {})
            if eval_results:
                episode_return_mean = eval_results[ENV_RUNNER_RESULTS][
                    EPISODE_RETURN_MEAN
                ]
                print(f"iter={i}, R={episode_return_mean}")
                # Learn until some reward is reached on an actual live env.
                if episode_return_mean > min_reward:
                    print("BC has learnt the task!")
                    learnt = True
                    break

        if not learnt:
            raise ValueError(
                f"`MARWIL` did not reach {min_reward} reward from expert offline data!"
            )

        algo.stop()

    def test_marwil_cont_actions_from_offline_file(self):
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
            .env_runners(num_env_runners=1)
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            # Evaluate on actual environment.
            .evaluation(
                evaluation_num_env_runners=1,
                evaluation_interval=3,
                evaluation_duration=5,
                evaluation_parallel_to_training=True,
            )
            .training(
                train_batch_size_per_learner=2000,
            )
            .offline_data(
                # Learn from offline data.
                input_=[data_path],
                dataset_num_iters_per_learner=1,
            )
        )

        num_iterations = 3

        algo = config.build(env="Pendulum-v1")
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
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(num_env_runners=0)
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

        # Create the prelearner and compute advantages and values.
        offline_prelearner = OfflinePreLearner(config, algo.learner_group._learner)
        # Note, for `ray.data`'s pipeline everything has to be a dictionary
        # therefore the batch is embedded into another dictionary.
        batch = offline_prelearner(batch)["batch"][0]
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
        fwd_out = (
            algo.learner_group._learner.module[DEFAULT_MODULE_ID]
            .unwrapped()
            .forward_train({k: v for k, v in batch[DEFAULT_MODULE_ID].items()})
        )
        advantages = (
            batch[DEFAULT_MODULE_ID][Columns.VALUE_TARGETS].detach().cpu().numpy()
            - fwd_out["vf_preds"].detach().cpu().numpy()
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
            batch={k: v for k, v in batch[DEFAULT_MODULE_ID].items()},
            fwd_out=fwd_out,
            config=config,
        )
        learner_results = algo.learner_group._learner.metrics.peek(DEFAULT_MODULE_ID)

        # Check all components.
        check(
            learner_results[VF_LOSS_KEY].detach().cpu().numpy(),
            expected_vf_loss,
            decimals=4,
        )
        check(
            learner_results[POLICY_LOSS_KEY].detach().cpu().numpy(),
            expected_pol_loss,
            decimals=4,
        )
        # Check the total loss.
        check(total_loss.detach().cpu().numpy(), expected_loss, decimals=3)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
