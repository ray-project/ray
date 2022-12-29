import numpy as np
import unittest

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF2Policy
from ray.rllib.algorithms.ppo.ppo_torch_policy import PPOTorchPolicy
from ray.rllib.evaluation.postprocessing import (
    compute_gae_for_sample_batch,
    Postprocessing,
)
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.numpy import fc
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_off_policyness,
    check_train_results,
    framework_iterator,
    check_inference_w_connectors,
)


# Fake CartPole episode of n time steps.
FAKE_BATCH = SampleBatch(
    {
        SampleBatch.OBS: np.array(
            [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
            dtype=np.float32,
        ),
        SampleBatch.ACTIONS: np.array([0, 1, 1]),
        SampleBatch.PREV_ACTIONS: np.array([0, 1, 1]),
        SampleBatch.REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
        SampleBatch.PREV_REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
        SampleBatch.TERMINATEDS: np.array([False, False, True]),
        SampleBatch.TRUNCATEDS: np.array([False, False, False]),
        SampleBatch.VF_PREDS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
        SampleBatch.ACTION_DIST_INPUTS: np.array(
            [[-2.0, 0.5], [-3.0, -0.3], [-0.1, 2.5]], dtype=np.float32
        ),
        SampleBatch.ACTION_LOGP: np.array([-0.5, -0.1, -0.2], dtype=np.float32),
        SampleBatch.EPS_ID: np.array([0, 0, 0]),
        SampleBatch.AGENT_INDEX: np.array([0, 0, 0]),
    }
)


class MyCallbacks(DefaultCallbacks):
    @staticmethod
    def _check_lr_torch(policy, policy_id):
        for j, opt in enumerate(policy._optimizers):
            for p in opt.param_groups:
                assert p["lr"] == policy.cur_lr, "LR scheduling error!"

    @staticmethod
    def _check_lr_tf(policy, policy_id):
        lr = policy.cur_lr
        sess = policy.get_session()
        if sess:
            lr = sess.run(lr)
            optim_lr = sess.run(policy._optimizer._lr)
        else:
            lr = lr.numpy()
            optim_lr = policy._optimizer.lr.numpy()
        assert lr == optim_lr, "LR scheduling error!"

    def on_train_result(self, *, algorithm, result: dict, **kwargs):
        stats = result["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY]
        # Learning rate should go to 0 after 1 iter.
        check(stats["cur_lr"], 5e-5 if algorithm.iteration == 1 else 0.0)
        # Entropy coeff goes to 0.05, then 0.0 (per iter).
        check(stats["entropy_coeff"], 0.1 if algorithm.iteration == 1 else 0.05)

        algorithm.workers.foreach_policy(
            self._check_lr_torch
            if algorithm.config["framework"] == "torch"
            else self._check_lr_tf
        )


class TestPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ppo_compilation_w_connectors(self):
        """Test whether PPO can be built with all frameworks w/ connectors."""

        # Build a PPOConfig object.
        config = (
            ppo.PPOConfig()
            .training(
                num_sgd_iter=2,
                # Setup lr schedule for testing.
                lr_schedule=[[0, 5e-5], [128, 0.0]],
                # Set entropy_coeff to a faulty value to proof that it'll get
                # overridden by the schedule below (which is expected).
                entropy_coeff=100.0,
                entropy_coeff_schedule=[[0, 0.1], [256, 0.0]],
                train_batch_size=128,
                model=dict(
                    # Settings in case we use an LSTM.
                    lstm_cell_size=10,
                    max_seq_len=20,
                ),
            )
            .rollouts(
                num_rollout_workers=1,
                # Test with compression.
                compress_observations=True,
                enable_connectors=True,
            )
            .callbacks(MyCallbacks)
        )  # For checking lr-schedule correctness.

        num_iterations = 2

        for fw in framework_iterator(config, with_eager_tracing=True):
            for env in ["FrozenLake-v1", "ALE/MsPacman-v5"]:
                print("Env={}".format(env))
                for lstm in [False, True]:
                    print("LSTM={}".format(lstm))
                    config.training(
                        model=dict(
                            use_lstm=lstm,
                            lstm_use_prev_action=lstm,
                            lstm_use_prev_reward=lstm,
                        )
                    )

                    algo = config.build(env=env)
                    policy = algo.get_policy()
                    entropy_coeff = algo.get_policy().entropy_coeff
                    lr = policy.cur_lr
                    if fw == "tf":
                        entropy_coeff, lr = policy.get_session().run(
                            [entropy_coeff, lr]
                        )
                    check(entropy_coeff, 0.1)
                    check(lr, config.lr)

                    for i in range(num_iterations):
                        results = algo.train()
                        check_train_results(results)
                        print(results)

                    check_inference_w_connectors(policy, env_name=env)
                    algo.stop()

    def test_ppo_compilation_and_schedule_mixins(self):
        """Test whether PPO can be built with all frameworks."""

        # Build a PPOConfig object.
        config = (
            ppo.PPOConfig()
            .training(
                # Setup lr schedule for testing.
                lr_schedule=[[0, 5e-5], [256, 0.0]],
                # Set entropy_coeff to a faulty value to proof that it'll get
                # overridden by the schedule below (which is expected).
                entropy_coeff=100.0,
                entropy_coeff_schedule=[[0, 0.1], [512, 0.0]],
                train_batch_size=256,
                sgd_minibatch_size=128,
                num_sgd_iter=2,
                model=dict(
                    # Settings in case we use an LSTM.
                    lstm_cell_size=10,
                    max_seq_len=20,
                ),
            )
            .rollouts(
                num_rollout_workers=1,
                # Test with compression.
                compress_observations=True,
            )
            .callbacks(MyCallbacks)
        )  # For checking lr-schedule correctness.

        num_iterations = 2

        for fw in framework_iterator(config, with_eager_tracing=True):
            for env in ["FrozenLake-v1", "ALE/MsPacman-v5"]:
                print("Env={}".format(env))
                for lstm in [False, True]:
                    print("LSTM={}".format(lstm))
                    config.training(
                        model=dict(
                            use_lstm=lstm,
                            lstm_use_prev_action=lstm,
                            lstm_use_prev_reward=lstm,
                        )
                    )

                    algo = config.build(env=env)
                    policy = algo.get_policy()
                    entropy_coeff = algo.get_policy().entropy_coeff
                    lr = policy.cur_lr
                    if fw == "tf":
                        entropy_coeff, lr = policy.get_session().run(
                            [entropy_coeff, lr]
                        )
                    check(entropy_coeff, 0.1)
                    check(lr, config.lr)

                    for i in range(num_iterations):
                        results = algo.train()
                        print(results)
                        check_train_results(results)
                        # 2 sgd iters per update, 2 minibatches per trainbatch -> 4x
                        # avg(0.0, 1.0, 2.0, 3.0) -> 1.5
                        off_policy_ness = check_off_policyness(
                            results, lower_limit=1.5, upper_limit=1.5
                        )
                        print(f"off-policy'ness={off_policy_ness}")

                    check_compute_single_action(
                        algo, include_prev_action_reward=True, include_state=lstm
                    )
                    algo.stop()

    def test_ppo_exploration_setup(self):
        """Tests, whether PPO runs with different exploration setups."""
        config = (
            ppo.PPOConfig()
            .environment(
                "FrozenLake-v1",
                env_config={"is_slippery": False, "map_name": "4x4"},
            )
            .rollouts(
                # Run locally.
                num_rollout_workers=0,
            )
        )
        obs = np.array(0)

        # Test against all frameworks.
        for fw in framework_iterator(config):
            # Default Agent should be setup with StochasticSampling.
            trainer = config.build()
            # explore=False, always expect the same (deterministic) action.
            a_ = trainer.compute_single_action(
                obs, explore=False, prev_action=np.array(2), prev_reward=np.array(1.0)
            )
            # Test whether this is really the argmax action over the logits.
            if fw != "tf":
                last_out = trainer.get_policy().model.last_output()
                if fw == "torch":
                    check(a_, np.argmax(last_out.detach().cpu().numpy(), 1)[0])
                else:
                    check(a_, np.argmax(last_out.numpy(), 1)[0])
            for _ in range(50):
                a = trainer.compute_single_action(
                    obs,
                    explore=False,
                    prev_action=np.array(2),
                    prev_reward=np.array(1.0),
                )
                check(a, a_)

            # With explore=True (default), expect stochastic actions.
            actions = []
            for _ in range(300):
                actions.append(
                    trainer.compute_single_action(
                        obs, prev_action=np.array(2), prev_reward=np.array(1.0)
                    )
                )
            check(np.mean(actions), 1.5, atol=0.2)
            trainer.stop()

    def test_ppo_free_log_std(self):
        """Tests the free log std option works."""
        config = (
            ppo.PPOConfig()
            .environment("CartPole-v1")
            .rollouts(
                num_rollout_workers=0,
            )
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10],
                    fcnet_activation="linear",
                    free_log_std=True,
                    vf_share_layers=True,
                ),
            )
        )

        for fw, sess in framework_iterator(config, session=True):
            trainer = config.build()
            policy = trainer.get_policy()

            # Check the free log std var is created.
            if fw == "torch":
                matching = [
                    v for (n, v) in policy.model.named_parameters() if "log_std" in n
                ]
            else:
                matching = [
                    v for v in policy.model.trainable_variables() if "log_std" in str(v)
                ]
            assert len(matching) == 1, matching
            log_std_var = matching[0]

            # linter yells at you if you don't pass in the parameters.
            # reason: https://docs.python-guide.org/writing/gotchas/
            # #late-binding-closures
            def get_value(fw=fw, policy=policy, log_std_var=log_std_var):
                if fw == "tf":
                    return policy.get_session().run(log_std_var)[0]
                elif fw == "torch":
                    return log_std_var.detach().cpu().numpy()[0]
                else:
                    return log_std_var.numpy()[0]

            # Check the variable is initially zero.
            init_std = get_value()
            assert init_std == 0.0, init_std
            batch = compute_gae_for_sample_batch(policy, FAKE_BATCH.copy())
            if fw == "torch":
                batch = policy._lazy_tensor_dict(batch)
            policy.learn_on_batch(batch)

            # Check the variable is updated.
            post_std = get_value()
            assert post_std != 0.0, post_std
            trainer.stop()

    def test_ppo_loss_function(self):
        """Tests the PPO loss function math."""
        config = (
            ppo.PPOConfig()
            .environment("CartPole-v1")
            .rollouts(
                num_rollout_workers=0,
            )
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10],
                    fcnet_activation="linear",
                    vf_share_layers=True,
                ),
            )
        )

        for fw, sess in framework_iterator(config, session=True):
            trainer = config.build()
            policy = trainer.get_policy()

            # Check no free log std var by default.
            if fw == "torch":
                matching = [
                    v for (n, v) in policy.model.named_parameters() if "log_std" in n
                ]
            else:
                matching = [
                    v for v in policy.model.trainable_variables() if "log_std" in str(v)
                ]
            assert len(matching) == 0, matching

            # Post-process (calculate simple (non-GAE) advantages) and attach
            # to train_batch dict.
            # A = [0.99^2 * 0.5 + 0.99 * -1.0 + 1.0, 0.99 * 0.5 - 1.0, 0.5] =
            # [0.50005, -0.505, 0.5]
            train_batch = compute_gae_for_sample_batch(policy, FAKE_BATCH.copy())
            if fw == "torch":
                train_batch = policy._lazy_tensor_dict(train_batch)

            # Check Advantage values.
            check(train_batch[Postprocessing.VALUE_TARGETS], [0.50005, -0.505, 0.5])

            # Calculate actual PPO loss.
            if fw == "tf2":
                PPOTF2Policy.loss(policy, policy.model, Categorical, train_batch)
            elif fw == "torch":
                PPOTorchPolicy.loss(
                    policy, policy.model, policy.dist_class, train_batch
                )

            vars = (
                policy.model.variables()
                if fw != "torch"
                else list(policy.model.parameters())
            )
            if fw == "tf":
                vars = policy.get_session().run(vars)
            expected_shared_out = fc(
                train_batch[SampleBatch.CUR_OBS],
                vars[0 if fw != "torch" else 2],
                vars[1 if fw != "torch" else 3],
                framework=fw,
            )
            expected_logits = fc(
                expected_shared_out,
                vars[2 if fw != "torch" else 0],
                vars[3 if fw != "torch" else 1],
                framework=fw,
            )
            expected_value_outs = fc(
                expected_shared_out, vars[4], vars[5], framework=fw
            )

            kl, entropy, pg_loss, vf_loss, overall_loss = self._ppo_loss_helper(
                policy,
                policy.model,
                Categorical if fw != "torch" else TorchCategorical,
                train_batch,
                expected_logits,
                expected_value_outs,
                sess=sess,
            )
            if sess:
                policy_sess = policy.get_session()
                k, e, pl, v, tl = policy_sess.run(
                    [
                        policy._mean_kl_loss,
                        policy._mean_entropy,
                        policy._mean_policy_loss,
                        policy._mean_vf_loss,
                        policy._total_loss,
                    ],
                    feed_dict=policy._get_loss_inputs_dict(train_batch, shuffle=False),
                )
                check(k, kl)
                check(e, entropy)
                check(pl, np.mean(-pg_loss))
                check(v, np.mean(vf_loss), decimals=4)
                check(tl, overall_loss, decimals=4)
            elif fw == "torch":
                check(policy.model.tower_stats["mean_kl_loss"], kl)
                check(policy.model.tower_stats["mean_entropy"], entropy)
                check(policy.model.tower_stats["mean_policy_loss"], np.mean(-pg_loss))
                check(
                    policy.model.tower_stats["mean_vf_loss"],
                    np.mean(vf_loss),
                    decimals=4,
                )
                check(policy.model.tower_stats["total_loss"], overall_loss, decimals=4)
            else:
                check(policy._mean_kl_loss, kl)
                check(policy._mean_entropy, entropy)
                check(policy._mean_policy_loss, np.mean(-pg_loss))
                check(policy._mean_vf_loss, np.mean(vf_loss), decimals=4)
                check(policy._total_loss, overall_loss, decimals=4)
            trainer.stop()

    def _ppo_loss_helper(
        self, policy, model, dist_class, train_batch, logits, vf_outs, sess=None
    ):
        """
        Calculates the expected PPO loss (components) given Policy,
        Model, distribution, some batch, logits & vf outputs, using numpy.
        """
        # Calculate expected PPO loss results.
        dist = dist_class(logits, policy.model)
        dist_prev = dist_class(
            train_batch[SampleBatch.ACTION_DIST_INPUTS], policy.model
        )
        expected_logp = dist.logp(train_batch[SampleBatch.ACTIONS])
        if isinstance(model, TorchModelV2):
            train_batch.set_get_interceptor(None)
            expected_rho = np.exp(
                expected_logp.detach().cpu().numpy()
                - train_batch[SampleBatch.ACTION_LOGP]
            )
            # KL(prev vs current action dist)-loss component.
            kl = np.mean(dist_prev.kl(dist).detach().cpu().numpy())
            # Entropy-loss component.
            entropy = np.mean(dist.entropy().detach().cpu().numpy())
        else:
            if sess:
                expected_logp = sess.run(expected_logp)
            expected_rho = np.exp(expected_logp - train_batch[SampleBatch.ACTION_LOGP])
            # KL(prev vs current action dist)-loss component.
            kl = dist_prev.kl(dist)
            if sess:
                kl = sess.run(kl)
            kl = np.mean(kl)
            # Entropy-loss component.
            entropy = dist.entropy()
            if sess:
                entropy = sess.run(entropy)
            entropy = np.mean(entropy)

        # Policy loss component.
        pg_loss = np.minimum(
            train_batch[Postprocessing.ADVANTAGES] * expected_rho,
            train_batch[Postprocessing.ADVANTAGES]
            * np.clip(
                expected_rho,
                1 - policy.config["clip_param"],
                1 + policy.config["clip_param"],
            ),
        )

        # Value function loss component.
        vf_loss1 = np.power(vf_outs - train_batch[Postprocessing.VALUE_TARGETS], 2.0)
        vf_clipped = train_batch[SampleBatch.VF_PREDS] + np.clip(
            vf_outs - train_batch[SampleBatch.VF_PREDS],
            -policy.config["vf_clip_param"],
            policy.config["vf_clip_param"],
        )
        vf_loss2 = np.power(vf_clipped - train_batch[Postprocessing.VALUE_TARGETS], 2.0)
        vf_loss = np.maximum(vf_loss1, vf_loss2)

        # Overall loss.
        if sess:
            policy_sess = policy.get_session()
            kl_coeff, entropy_coeff = policy_sess.run(
                [policy.kl_coeff, policy.entropy_coeff]
            )
        else:
            kl_coeff, entropy_coeff = policy.kl_coeff, policy.entropy_coeff
        overall_loss = np.mean(
            -pg_loss
            + kl_coeff * kl
            + policy.config["vf_loss_coeff"] * vf_loss
            - entropy_coeff * entropy
        )
        return kl, entropy, pg_loss, vf_loss, overall_loss


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
