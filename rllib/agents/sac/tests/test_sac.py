from gym import Env
from gym.spaces import Box
import numpy as np
import re
import unittest

import ray
import ray.rllib.agents.sac as sac
from ray.rllib.agents.sac.sac_tf_policy import sac_actor_critic_loss as tf_loss
from ray.rllib.agents.sac.sac_torch_policy import actor_critic_loss as \
    loss_torch
from ray.rllib.models.tf.tf_action_dist import Dirichlet
from ray.rllib.models.torch.torch_action_dist import TorchDirichlet
from ray.rllib.execution.replay_buffer import LocalReplayBuffer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import fc, huber_loss, relu
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.test_utils import check, check_compute_single_action, \
    framework_iterator
from ray.rllib.utils.torch_ops import convert_to_torch_tensor

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class SimpleEnv(Env):
    def __init__(self, config):
        if config.get("simplex_actions", False):
            self.action_space = Simplex((2, ))
        else:
            self.action_space = Box(0.0, 1.0, (1, ))
        self.observation_space = Box(0.0, 1.0, (1, ))
        self.max_steps = config.get("max_steps", 100)
        self.state = None
        self.steps = None

    def reset(self):
        self.state = self.observation_space.sample()
        self.steps = 0
        return self.state

    def step(self, action):
        self.steps += 1
        # Reward is 1.0 - (max(actions) - state).
        [r] = 1.0 - np.abs(np.max(action) - self.state)
        d = self.steps >= self.max_steps
        self.state = self.observation_space.sample()
        return self.state, r, d, {}


class TestSAC(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_sac_compilation(self):
        """Tests whether an SACTrainer can be built with all frameworks."""
        config = sac.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["twin_q"] = True
        config["soft_horizon"] = True
        config["clip_actions"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        config["prioritized_replay"] = True
        num_iterations = 1
        for _ in framework_iterator(config):
            # Test for different env types (discrete w/ and w/o image, + cont).
            for env in [
                    "Pendulum-v0", "MsPacmanNoFrameskip-v4", "CartPole-v0"
            ]:
                print("Env={}".format(env))
                config["use_state_preprocessor"] = \
                    env == "MsPacmanNoFrameskip-v4"
                trainer = sac.SACTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    print(results)
                check_compute_single_action(trainer)
                trainer.stop()

    def test_sac_loss_function(self):
        """Tests SAC loss function results across all frameworks."""
        config = sac.DEFAULT_CONFIG.copy()
        # Run locally.
        config["num_workers"] = 0
        config["learning_starts"] = 0
        config["twin_q"] = False
        config["gamma"] = 0.99
        # Switch on deterministic loss so we can compare the loss values.
        config["_deterministic_loss"] = True
        # Use very simple nets.
        config["Q_model"]["fcnet_hiddens"] = [10]
        config["policy_model"]["fcnet_hiddens"] = [10]
        # Make sure, timing differences do not affect trainer.train().
        config["min_iter_time_s"] = 0
        # Test SAC with Simplex action space.
        config["env_config"] = {"simplex_actions": True}

        map_ = {
            # Normal net.
            "default_policy/sequential/action_1/kernel": "action_model."
            "action_0._model.0.weight",
            "default_policy/sequential/action_1/bias": "action_model."
            "action_0._model.0.bias",
            "default_policy/sequential/action_out/kernel": "action_model."
            "action_out._model.0.weight",
            "default_policy/sequential/action_out/bias": "action_model."
            "action_out._model.0.bias",
            "default_policy/sequential_1/q_hidden_0/kernel": "q_net."
            "q_hidden_0._model.0.weight",
            "default_policy/sequential_1/q_hidden_0/bias": "q_net."
            "q_hidden_0._model.0.bias",
            "default_policy/sequential_1/q_out/kernel": "q_net."
            "q_out._model.0.weight",
            "default_policy/sequential_1/q_out/bias": "q_net."
            "q_out._model.0.bias",
            "default_policy/value_out/kernel": "_value_branch."
            "_model.0.weight",
            "default_policy/value_out/bias": "_value_branch."
            "_model.0.bias",
            "default_policy/log_alpha": "log_alpha",
            # Target net.
            "default_policy/sequential_2/action_1/kernel": "action_model."
            "action_0._model.0.weight",
            "default_policy/sequential_2/action_1/bias": "action_model."
            "action_0._model.0.bias",
            "default_policy/sequential_2/action_out/kernel": "action_model."
            "action_out._model.0.weight",
            "default_policy/sequential_2/action_out/bias": "action_model."
            "action_out._model.0.bias",
            "default_policy/sequential_3/q_hidden_0/kernel": "q_net."
            "q_hidden_0._model.0.weight",
            "default_policy/sequential_3/q_hidden_0/bias": "q_net."
            "q_hidden_0._model.0.bias",
            "default_policy/sequential_3/q_out/kernel": "q_net."
            "q_out._model.0.weight",
            "default_policy/sequential_3/q_out/bias": "q_net."
            "q_out._model.0.bias",
            "default_policy/value_out_1/kernel": "_value_branch."
            "_model.0.weight",
            "default_policy/value_out_1/bias": "_value_branch."
            "_model.0.bias",
            "default_policy/log_alpha_1": "log_alpha",
        }

        env = SimpleEnv
        batch_size = 100
        if env is SimpleEnv:
            obs_size = (batch_size, 1)
            actions = np.random.random(size=(batch_size, 2))
        elif env == "CartPole-v0":
            obs_size = (batch_size, 4)
            actions = np.random.randint(0, 2, size=(batch_size, ))
        else:
            obs_size = (batch_size, 3)
            actions = np.random.random(size=(batch_size, 1))

        # Batch of size=n.
        input_ = self._get_batch_helper(obs_size, actions, batch_size)

        # Simply compare loss values AND grads of all frameworks with each
        # other.
        prev_fw_loss = weights_dict = None
        expect_c, expect_a, expect_e, expect_t = None, None, None, None
        # History of tf-updated NN-weights over n training steps.
        tf_updated_weights = []
        # History of input batches used.
        tf_inputs = []
        for fw, sess in framework_iterator(
                config, frameworks=("tf", "torch"), session=True):
            # Generate Trainer and get its default Policy object.
            trainer = sac.SACTrainer(config=config, env=env)
            policy = trainer.get_policy()
            p_sess = None
            if sess:
                p_sess = policy.get_session()

            # Set all weights (of all nets) to fixed values.
            if weights_dict is None:
                # Start with the tf vars-dict.
                assert fw in ["tf2", "tf", "tfe"]
                weights_dict = policy.get_weights()
                if fw == "tfe":
                    log_alpha = weights_dict[10]
                    weights_dict = self._translate_tfe_weights(
                        weights_dict, map_)
            else:
                assert fw == "torch"  # Then transfer that to torch Model.
                model_dict = self._translate_weights_to_torch(
                    weights_dict, map_)
                policy.model.load_state_dict(model_dict)
                policy.target_model.load_state_dict(model_dict)

            if fw == "tf":
                log_alpha = weights_dict["default_policy/log_alpha"]
            elif fw == "torch":
                # Actually convert to torch tensors (by accessing everything).
                input_ = policy._lazy_tensor_dict(input_)
                input_ = {k: input_[k] for k in input_.keys()}
                log_alpha = policy.model.log_alpha.detach().cpu().numpy()[0]

            # Only run the expectation once, should be the same anyways
            # for all frameworks.
            if expect_c is None:
                expect_c, expect_a, expect_e, expect_t = \
                    self._sac_loss_helper(input_, weights_dict,
                                          sorted(weights_dict.keys()),
                                          log_alpha, fw,
                                          gamma=config["gamma"], sess=sess)

            # Get actual outs and compare to expectation AND previous
            # framework. c=critic, a=actor, e=entropy, t=td-error.
            if fw == "tf":
                c, a, e, t, tf_c_grads, tf_a_grads, tf_e_grads = \
                    p_sess.run([
                        policy.critic_loss,
                        policy.actor_loss,
                        policy.alpha_loss,
                        policy.td_error,
                        policy.optimizer().compute_gradients(
                            policy.critic_loss[0],
                            policy.model.q_variables()),
                        policy.optimizer().compute_gradients(
                            policy.actor_loss,
                            policy.model.policy_variables()),
                        policy.optimizer().compute_gradients(
                            policy.alpha_loss, policy.model.log_alpha)],
                        feed_dict=policy._get_loss_inputs_dict(
                            input_, shuffle=False))
                tf_c_grads = [g for g, v in tf_c_grads]
                tf_a_grads = [g for g, v in tf_a_grads]
                tf_e_grads = [g for g, v in tf_e_grads]

            elif fw == "tfe":
                with tf.GradientTape() as tape:
                    tf_loss(policy, policy.model, None, input_)
                c, a, e, t = policy.critic_loss, policy.actor_loss, \
                    policy.alpha_loss, policy.td_error
                vars = tape.watched_variables()
                tf_c_grads = tape.gradient(c[0], vars[6:10])
                tf_a_grads = tape.gradient(a, vars[2:6])
                tf_e_grads = tape.gradient(e, vars[10])

            elif fw == "torch":
                loss_torch(policy, policy.model, None, input_)
                c, a, e, t = policy.critic_loss, policy.actor_loss, \
                    policy.alpha_loss, policy.td_error

                # Test actor gradients.
                policy.actor_optim.zero_grad()
                assert all(v.grad is None for v in policy.model.q_variables())
                assert all(
                    v.grad is None for v in policy.model.policy_variables())
                assert policy.model.log_alpha.grad is None
                a.backward()
                # `actor_loss` depends on Q-net vars (but these grads must
                # be ignored and overridden in critic_loss.backward!).
                assert not any(v.grad is None
                               for v in policy.model.q_variables())
                assert not all(
                    torch.mean(v.grad) == 0
                    for v in policy.model.policy_variables())
                assert not all(
                    torch.min(v.grad) == 0
                    for v in policy.model.policy_variables())
                assert policy.model.log_alpha.grad is None
                # Compare with tf ones.
                torch_a_grads = [
                    v.grad for v in policy.model.policy_variables()
                ]
                for tf_g, torch_g in zip(tf_a_grads, torch_a_grads):
                    if tf_g.shape != torch_g.shape:
                        check(tf_g, np.transpose(torch_g.detach().cpu()))
                    else:
                        check(tf_g, torch_g)

                # Test critic gradients.
                policy.critic_optims[0].zero_grad()
                assert all(
                    torch.mean(v.grad) == 0.0
                    for v in policy.model.q_variables())
                assert all(
                    torch.min(v.grad) == 0.0
                    for v in policy.model.q_variables())
                assert policy.model.log_alpha.grad is None
                c[0].backward()
                assert not all(
                    torch.mean(v.grad) == 0
                    for v in policy.model.q_variables())
                assert not all(
                    torch.min(v.grad) == 0 for v in policy.model.q_variables())
                assert policy.model.log_alpha.grad is None
                # Compare with tf ones.
                torch_c_grads = [v.grad for v in policy.model.q_variables()]
                for tf_g, torch_g in zip(tf_c_grads, torch_c_grads):
                    if tf_g.shape != torch_g.shape:
                        check(tf_g, np.transpose(torch_g.detach().cpu()))
                    else:
                        check(tf_g, torch_g)
                # Compare (unchanged(!) actor grads) with tf ones.
                torch_a_grads = [
                    v.grad for v in policy.model.policy_variables()
                ]
                for tf_g, torch_g in zip(tf_a_grads, torch_a_grads):
                    if tf_g.shape != torch_g.shape:
                        check(tf_g, np.transpose(torch_g.detach().cpu()))
                    else:
                        check(tf_g, torch_g)

                # Test alpha gradient.
                policy.alpha_optim.zero_grad()
                assert policy.model.log_alpha.grad is None
                e.backward()
                assert policy.model.log_alpha.grad is not None
                check(policy.model.log_alpha.grad, tf_e_grads)

            check(c, expect_c)
            check(a, expect_a)
            check(e, expect_e)
            check(t, expect_t)

            # Store this framework's losses in prev_fw_loss to compare with
            # next framework's outputs.
            if prev_fw_loss is not None:
                check(c, prev_fw_loss[0])
                check(a, prev_fw_loss[1])
                check(e, prev_fw_loss[2])
                check(t, prev_fw_loss[3])

            prev_fw_loss = (c, a, e, t)

            # Update weights from our batch (n times).
            for update_iteration in range(10):
                print("train iteration {}".format(update_iteration))
                if fw == "tf":
                    in_ = self._get_batch_helper(obs_size, actions, batch_size)
                    tf_inputs.append(in_)
                    # Set a fake-batch to use
                    # (instead of sampling from replay buffer).
                    buf = LocalReplayBuffer.get_instance_for_testing()
                    buf._fake_batch = in_
                    trainer.train()
                    updated_weights = policy.get_weights()
                    # Net must have changed.
                    if tf_updated_weights:
                        check(
                            updated_weights[
                                "default_policy/sequential/action_1/kernel"],
                            tf_updated_weights[-1][
                                "default_policy/sequential/action_1/kernel"],
                            false=True)
                    tf_updated_weights.append(updated_weights)

                # Compare with updated tf-weights. Must all be the same.
                else:
                    tf_weights = tf_updated_weights[update_iteration]
                    in_ = tf_inputs[update_iteration]
                    # Set a fake-batch to use
                    # (instead of sampling from replay buffer).
                    buf = LocalReplayBuffer.get_instance_for_testing()
                    buf._fake_batch = in_
                    trainer.train()
                    # Compare updated model.
                    for tf_key in sorted(tf_weights.keys())[2:10]:
                        tf_var = tf_weights[tf_key]
                        torch_var = policy.model.state_dict()[map_[tf_key]]
                        if tf_var.shape != torch_var.shape:
                            check(
                                tf_var,
                                np.transpose(torch_var.detach().cpu()),
                                rtol=0.05)
                        else:
                            check(tf_var, torch_var, rtol=0.05)
                    # And alpha.
                    check(policy.model.log_alpha,
                          tf_weights["default_policy/log_alpha"])
                    # Compare target nets.
                    for tf_key in sorted(tf_weights.keys())[10:18]:
                        tf_var = tf_weights[tf_key]
                        torch_var = policy.target_model.state_dict()[map_[
                            tf_key]]
                        if tf_var.shape != torch_var.shape:
                            check(
                                tf_var,
                                np.transpose(torch_var.detach().cpu()),
                                rtol=0.05)
                        else:
                            check(tf_var, torch_var, rtol=0.05)

    def _get_batch_helper(self, obs_size, actions, batch_size):
        return {
            SampleBatch.CUR_OBS: np.random.random(size=obs_size),
            SampleBatch.ACTIONS: actions,
            SampleBatch.REWARDS: np.random.random(size=(batch_size, )),
            SampleBatch.DONES: np.random.choice(
                [True, False], size=(batch_size, )),
            SampleBatch.NEXT_OBS: np.random.random(size=obs_size),
            "weights": np.random.random(size=(batch_size, )),
        }

    def _sac_loss_helper(self, train_batch, weights, ks, log_alpha, fw, gamma,
                         sess):
        """Emulates SAC loss functions for tf and torch."""
        # ks:
        # 0=log_alpha
        # 1=target log-alpha (not used)

        # 2=action hidden bias
        # 3=action hidden kernel
        # 4=action out bias
        # 5=action out kernel

        # 6=Q hidden bias
        # 7=Q hidden kernel
        # 8=Q out bias
        # 9=Q out kernel

        # 14=target Q hidden bias
        # 15=target Q hidden kernel
        # 16=target Q out bias
        # 17=target Q out kernel
        alpha = np.exp(log_alpha)
        # cls = TorchSquashedGaussian if fw == "torch" else SquashedGaussian
        cls = TorchDirichlet if fw == "torch" else Dirichlet
        model_out_t = train_batch[SampleBatch.CUR_OBS]
        model_out_tp1 = train_batch[SampleBatch.NEXT_OBS]
        target_model_out_tp1 = train_batch[SampleBatch.NEXT_OBS]

        # get_policy_output
        action_dist_t = cls(
            fc(
                relu(
                    fc(model_out_t,
                       weights[ks[3]],
                       weights[ks[2]],
                       framework=fw)), weights[ks[5]], weights[ks[4]]), None)
        policy_t = action_dist_t.deterministic_sample()
        log_pis_t = action_dist_t.logp(policy_t)
        if sess:
            log_pis_t = sess.run(log_pis_t)
            policy_t = sess.run(policy_t)
        log_pis_t = np.expand_dims(log_pis_t, -1)

        # Get policy output for t+1.
        action_dist_tp1 = cls(
            fc(
                relu(
                    fc(model_out_tp1,
                       weights[ks[3]],
                       weights[ks[2]],
                       framework=fw)), weights[ks[5]], weights[ks[4]]), None)
        policy_tp1 = action_dist_tp1.deterministic_sample()
        log_pis_tp1 = action_dist_tp1.logp(policy_tp1)
        if sess:
            log_pis_tp1 = sess.run(log_pis_tp1)
            policy_tp1 = sess.run(policy_tp1)
        log_pis_tp1 = np.expand_dims(log_pis_tp1, -1)

        # Q-values for the actually selected actions.
        # get_q_values
        q_t = fc(
            relu(
                fc(np.concatenate(
                    [model_out_t, train_batch[SampleBatch.ACTIONS]], -1),
                   weights[ks[7]],
                   weights[ks[6]],
                   framework=fw)),
            weights[ks[9]],
            weights[ks[8]],
            framework=fw)

        # Q-values for current policy in given current state.
        # get_q_values
        q_t_det_policy = fc(
            relu(
                fc(np.concatenate([model_out_t, policy_t], -1),
                   weights[ks[7]],
                   weights[ks[6]],
                   framework=fw)),
            weights[ks[9]],
            weights[ks[8]],
            framework=fw)

        # Target q network evaluation.
        # target_model.get_q_values
        if fw == "tf":
            q_tp1 = fc(
                relu(
                    fc(np.concatenate([target_model_out_tp1, policy_tp1], -1),
                       weights[ks[15]],
                       weights[ks[14]],
                       framework=fw)),
                weights[ks[17]],
                weights[ks[16]],
                framework=fw)
        else:
            assert fw == "tfe"
            q_tp1 = fc(
                relu(
                    fc(np.concatenate([target_model_out_tp1, policy_tp1], -1),
                       weights[ks[7]],
                       weights[ks[6]],
                       framework=fw)),
                weights[ks[9]],
                weights[ks[8]],
                framework=fw)

        q_t_selected = np.squeeze(q_t, axis=-1)
        q_tp1 -= alpha * log_pis_tp1
        q_tp1_best = np.squeeze(q_tp1, axis=-1)
        dones = train_batch[SampleBatch.DONES]
        rewards = train_batch[SampleBatch.REWARDS]
        if fw == "torch":
            dones = dones.float().numpy()
            rewards = rewards.numpy()
        q_tp1_best_masked = (1.0 - dones) * q_tp1_best
        q_t_selected_target = rewards + gamma * q_tp1_best_masked
        base_td_error = np.abs(q_t_selected - q_t_selected_target)
        td_error = base_td_error
        critic_loss = [
            np.mean(train_batch["weights"] *
                    huber_loss(q_t_selected_target - q_t_selected))
        ]
        target_entropy = -np.prod((1, ))
        alpha_loss = -np.mean(log_alpha * (log_pis_t + target_entropy))
        actor_loss = np.mean(alpha * log_pis_t - q_t_det_policy)

        return critic_loss, actor_loss, alpha_loss, td_error

    def _translate_weights_to_torch(self, weights_dict, map_):
        model_dict = {
            map_[k]: convert_to_torch_tensor(
                np.transpose(v) if re.search("kernel", k) else np.array([v])
                if re.search("log_alpha", k) else v)
            for k, v in weights_dict.items()
            if re.search("(sequential(/|_1)|value_out/|log_alpha)", k)
        }
        return model_dict

    def _translate_tfe_weights(self, weights_dict, map_):
        model_dict = {
            "default_policy/log_alpha": None,
            "default_policy/log_alpha_target": None,
            "default_policy/sequential/action_1/kernel": weights_dict[2],
            "default_policy/sequential/action_1/bias": weights_dict[3],
            "default_policy/sequential/action_out/kernel": weights_dict[4],
            "default_policy/sequential/action_out/bias": weights_dict[5],
            "default_policy/sequential_1/q_hidden_0/kernel": weights_dict[6],
            "default_policy/sequential_1/q_hidden_0/bias": weights_dict[7],
            "default_policy/sequential_1/q_out/kernel": weights_dict[8],
            "default_policy/sequential_1/q_out/bias": weights_dict[9],
            "default_policy/value_out/kernel": weights_dict[0],
            "default_policy/value_out/bias": weights_dict[1],
        }
        return model_dict


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
