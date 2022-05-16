import numpy as np
import os
from pathlib import Path
import unittest

import ray
import ray.rllib.algorithms.marwil as marwil
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.offline import JsonReader
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestMARWIL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_marwil_compilation_and_learning_from_offline_file(self):
        """Test whether a MARWILTrainer can be built with all frameworks.

        Learns from a historic-data file.
        To generate this data, first run:
        $ ./train.py --run=PPO --env=CartPole-v0 \
          --stop='{"timesteps_total": 50000}' \
          --config='{"output": "/tmp/out", "batch_mode": "complete_episodes"}'
        """
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        config = marwil.DEFAULT_CONFIG.copy()
        config["num_workers"] = 2
        config["env"] = "CartPole-v0"
        config["evaluation_num_workers"] = 1
        config["evaluation_interval"] = 3
        config["evaluation_duration"] = 5
        config["evaluation_parallel_to_training"] = True
        # Evaluate on actual environment.
        config["evaluation_config"] = {"input": "sampler"}
        # Learn from offline data.
        config["input"] = [data_file]
        num_iterations = 350
        min_reward = 70.0

        # Test for all frameworks.
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = marwil.MARWILTrainer(config=config)
            learnt = False
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)

                eval_results = results.get("evaluation")
                if eval_results:
                    print(
                        "iter={} R={} ".format(i, eval_results["episode_reward_mean"])
                    )
                    # Learn until some reward is reached on an actual live env.
                    if eval_results["episode_reward_mean"] > min_reward:
                        print("learnt!")
                        learnt = True
                        break

            if not learnt:
                raise ValueError(
                    "MARWILTrainer did not reach {} reward from expert "
                    "offline data!".format(min_reward)
                )

            check_compute_single_action(trainer, include_prev_action_reward=True)

            trainer.stop()

    def test_marwil_cont_actions_from_offline_file(self):
        """Test whether MARWILTrainer runs with cont. actions.

        Learns from a historic-data file.
        To generate this data, first run:
        $ ./train.py --run=PPO --env=Pendulum-v1 \
          --stop='{"timesteps_total": 50000}' \
          --config='{"output": "/tmp/out", "batch_mode": "complete_episodes"}'
        """
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/pendulum/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        config = marwil.DEFAULT_CONFIG.copy()
        config["num_workers"] = 1
        config["evaluation_num_workers"] = 1
        config["evaluation_interval"] = 3
        config["evaluation_duration"] = 5
        config["evaluation_parallel_to_training"] = True
        # Evaluate on actual environment.
        config["evaluation_config"] = {"input": "sampler"}
        # Learn from offline data.
        config["input"] = [data_file]
        config["input_evaluation"] = []  # disable (data has no action-probs)
        num_iterations = 3

        # Test for all frameworks.
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = marwil.MARWILTrainer(config=config, env="Pendulum-v1")
            for i in range(num_iterations):
                print(trainer.train())
            trainer.stop()

    def test_marwil_loss_function(self):
        """
        To generate the historic data used in this test case, first run:
        $ ./train.py --run=PPO --env=CartPole-v0 \
          --stop='{"timesteps_total": 50000}' \
          --config='{"output": "/tmp/out", "batch_mode": "complete_episodes"}'
        """
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/small.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))
        config = marwil.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        # Learn from offline data.
        config["input"] = [data_file]

        for fw, sess in framework_iterator(config, session=True):
            reader = JsonReader(inputs=[data_file])
            batch = reader.next()

            trainer = marwil.MARWILTrainer(config=config, env="CartPole-v0")
            policy = trainer.get_policy()
            model = policy.model

            # Calculate our own expected values (to then compare against the
            # agent's loss output).
            cummulative_rewards = compute_advantages(
                batch, 0.0, config["gamma"], 1.0, False, False
            )["advantages"]
            if fw == "torch":
                cummulative_rewards = torch.tensor(cummulative_rewards)
            if fw != "tf":
                batch = policy._lazy_tensor_dict(batch)
            model_out, _ = model(batch)
            vf_estimates = model.value_function()
            if fw == "tf":
                model_out, vf_estimates = policy.get_session().run(
                    [model_out, vf_estimates]
                )
            adv = cummulative_rewards - vf_estimates
            if fw == "torch":
                adv = adv.detach().cpu().numpy()
            adv_squared = np.mean(np.square(adv))
            c_2 = 100.0 + 1e-8 * (adv_squared - 100.0)
            c = np.sqrt(c_2)
            exp_advs = np.exp(config["beta"] * (adv / c))
            dist = policy.dist_class(model_out, model)
            logp = dist.logp(batch["actions"])
            if fw == "torch":
                logp = logp.detach().cpu().numpy()
            elif fw == "tf":
                logp = sess.run(logp)
            # Calculate all expected loss components.
            expected_vf_loss = 0.5 * adv_squared
            expected_pol_loss = -1.0 * np.mean(exp_advs * logp)
            expected_loss = expected_pol_loss + config["vf_coeff"] * expected_vf_loss

            # Calculate the algorithm's loss (to check against our own
            # calculation above).
            batch.set_get_interceptor(None)
            postprocessed_batch = policy.postprocess_trajectory(batch)
            loss_func = (
                marwil.marwil_tf_policy.marwil_loss
                if fw != "torch"
                else marwil.marwil_torch_policy.marwil_loss
            )
            if fw != "tf":
                policy._lazy_tensor_dict(postprocessed_batch)
                loss_out = loss_func(
                    policy, model, policy.dist_class, postprocessed_batch
                )
            else:
                loss_out, v_loss, p_loss = policy.get_session().run(
                    [policy._loss, policy.loss.v_loss, policy.loss.p_loss],
                    feed_dict=policy._get_loss_inputs_dict(
                        postprocessed_batch, shuffle=False
                    ),
                )

            # Check all components.
            if fw == "torch":
                check(policy.v_loss, expected_vf_loss, decimals=4)
                check(policy.p_loss, expected_pol_loss, decimals=4)
            elif fw == "tf":
                check(v_loss, expected_vf_loss, decimals=4)
                check(p_loss, expected_pol_loss, decimals=4)
            else:
                check(policy.loss.v_loss, expected_vf_loss, decimals=4)
                check(policy.loss.p_loss, expected_pol_loss, decimals=4)
            check(loss_out, expected_loss, decimals=3)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
