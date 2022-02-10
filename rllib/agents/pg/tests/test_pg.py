from gym.spaces import Box, Dict, Discrete, Tuple
import numpy as np
import unittest

import ray
import ray.rllib.agents.pg as pg
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.numpy import fc
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)
from ray import tune


class TestPG(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_pg_compilation(self):
        """Test whether a PGTrainer can be built with all frameworks."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 1
        config["rollout_fragment_length"] = 500
        # Test with filter to see whether they work w/o preprocessing.
        config["observation_filter"] = "MeanStdFilter"
        num_iterations = 1

        image_space = Box(-1.0, 1.0, shape=(84, 84, 3))
        simple_space = Box(-1.0, 1.0, shape=(3,))

        tune.register_env(
            "random_dict_env",
            lambda _: RandomEnv(
                {
                    "observation_space": Dict(
                        {
                            "a": simple_space,
                            "b": Discrete(2),
                            "c": image_space,
                        }
                    ),
                    "action_space": Box(-1.0, 1.0, shape=(1,)),
                }
            ),
        )
        tune.register_env(
            "random_tuple_env",
            lambda _: RandomEnv(
                {
                    "observation_space": Tuple(
                        [simple_space, Discrete(2), image_space]
                    ),
                    "action_space": Box(-1.0, 1.0, shape=(1,)),
                }
            ),
        )

        for _ in framework_iterator(config, with_eager_tracing=True):
            # Test for different env types (discrete w/ and w/o image, + cont).
            for env in [
                "random_dict_env",
                "random_tuple_env",
                "MsPacmanNoFrameskip-v4",
                "CartPole-v0",
                "FrozenLake-v1",
            ]:
                print(f"env={env}")
                trainer = pg.PGTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    check_train_results(results)
                    print(results)

                check_compute_single_action(trainer, include_prev_action_reward=True)

    def test_pg_loss_functions(self):
        """Tests the PG loss function math."""
        config = pg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["gamma"] = 0.99
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = "linear"

        # Fake CartPole episode of n time steps.
        train_batch = SampleBatch(
            {
                SampleBatch.OBS: np.array(
                    [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]]
                ),
                SampleBatch.ACTIONS: np.array([0, 1, 1]),
                SampleBatch.REWARDS: np.array([1.0, 1.0, 1.0]),
                SampleBatch.DONES: np.array([False, False, True]),
                SampleBatch.EPS_ID: np.array([1234, 1234, 1234]),
                SampleBatch.AGENT_INDEX: np.array([0, 0, 0]),
            }
        )

        for fw, sess in framework_iterator(config, session=True):
            dist_cls = Categorical if fw != "torch" else TorchCategorical
            trainer = pg.PGTrainer(config=config, env="CartPole-v0")
            policy = trainer.get_policy()
            vars = policy.model.trainable_variables()
            if sess:
                vars = policy.get_session().run(vars)

            # Post-process (calculate simple (non-GAE) advantages) and attach
            # to train_batch dict.
            # A = [0.99^2 * 1.0 + 0.99 * 1.0 + 1.0, 0.99 * 1.0 + 1.0, 1.0] =
            # [2.9701, 1.99, 1.0]
            train_batch_ = pg.post_process_advantages(policy, train_batch.copy())
            if fw == "torch":
                train_batch_ = policy._lazy_tensor_dict(train_batch_)

            # Check Advantage values.
            check(train_batch_[Postprocessing.ADVANTAGES], [2.9701, 1.99, 1.0])

            # Actual loss results.
            if sess:
                results = policy.get_session().run(
                    policy._loss,
                    feed_dict=policy._get_loss_inputs_dict(train_batch_, shuffle=False),
                )
            else:
                results = (pg.pg_tf_loss if fw in ["tf2", "tfe"] else pg.pg_torch_loss)(
                    policy, policy.model, dist_class=dist_cls, train_batch=train_batch_
                )

            # Calculate expected results.
            if fw != "torch":
                expected_logits = fc(
                    fc(train_batch_[SampleBatch.OBS], vars[0], vars[1], framework=fw),
                    vars[2],
                    vars[3],
                    framework=fw,
                )
            else:
                expected_logits = fc(
                    fc(train_batch_[SampleBatch.OBS], vars[2], vars[3], framework=fw),
                    vars[0],
                    vars[1],
                    framework=fw,
                )
            expected_logp = dist_cls(expected_logits, policy.model).logp(
                train_batch_[SampleBatch.ACTIONS]
            )
            adv = train_batch_[Postprocessing.ADVANTAGES]
            if sess:
                expected_logp = sess.run(expected_logp)
            elif fw == "torch":
                expected_logp = expected_logp.detach().cpu().numpy()
                adv = adv.detach().cpu().numpy()
            else:
                expected_logp = expected_logp.numpy()
            expected_loss = -np.mean(expected_logp * adv)
            check(results, expected_loss, decimals=4)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
