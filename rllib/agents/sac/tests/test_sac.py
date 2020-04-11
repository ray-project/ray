from gym import Env
from gym.spaces import Box
import numpy as np
import re
import unittest

import ray.rllib.agents.sac as sac
from ray.rllib.agents.sac.sac_torch_policy import actor_critic_loss as \
    loss_torch
from ray.rllib.models.tf.tf_action_dist import SquashedGaussian
from ray.rllib.models.torch.torch_action_dist import TorchSquashedGaussian
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import fc, relu
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.rllib.utils.torch_ops import convert_to_torch_tensor

tf = try_import_tf()
torch, _ = try_import_torch()


class SimpleEnv(Env):
    def __init__(self, config):
        self.action_space = Box(0.0, 1.0, (1, ))
        self.observation_space = Box(0.0, 1.0, (1, ))
        self.max_steps = config.get("max_steps", 100)
        self.state = None
        self.steps = None

    def reset(self):
        self.state = self.observation_space.sample()  #.random.random(size=(1,))
        self.steps = 0
        return self.state

    def step(self, action):
        self.steps += 1
        # Reward is 1.0 - (action - state).
        [r] = 1.0 - np.abs(action - self.state)
        d = self.steps >= self.max_steps
        self.state = self.observation_space.sample()  # np.random.random(size=(1,))
        return self.state, r, d, {}


class TestSAC(unittest.TestCase):
    #TODO:
    #Even easier: Compare stochastic sampling behavior tf squashed gaussian vs torch one.
    #Same for NN initialization.
    #Question here: Why does torch Pendulum produce crazy mean/std numbers as inputs to the dist only after 2 iterations?
    #Add Pendulum to loss test below:
    #Let tf generate: - the NN weights (already done) and - a first actual batch.
    #Then use that batch for updating tf AND pytorch, then compare weights on both again.
    
    def test_sac_pytorch_learning_cont(self):
        config = sac.DEFAULT_CONFIG.copy()
        config["use_pytorch"] = True
        config["_use_beta_distribution"] = False
        config["num_workers"] = 0  # Run locally.
        config["twin_q"] = False
        config["normalize_actions"] = True
        config["clip_actions"] = False
        config["metrics_smoothing_episodes"] = 5
        config["no_done_at_end"] = True
        config["learning_starts"] = 3000
        config["buffer_size"] = 10000
        config["initial_alpha"] = 1.0
        config["soft_horizon"] = True
        #config["env"] = SimpleEnv
        #config["optimization"]["critic_learning_rate"] = 0.1

        #config["Q_model"]["fcnet_hiddens"] = [32]
        #config["Q_model"]["fcnet_activation"] = "linear"
        #config["policy_model"]["fcnet_hiddens"] = [32]
        #config["policy_model"]["fcnet_activation"] = "linear"

        num_iterations = 2000

        trainer = sac.SACTrainer(config=config, env="Pendulum-v0")
        for i in range(num_iterations):
            results = trainer.train()
            print(results)

    def test_sac_compilation(self):
        """Test whether an SACTrainer can be built with all frameworks."""
        config = sac.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["twin_q"] = True
        config["soft_horizon"] = True
        config["clip_actions"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        num_iterations = 2
        # eager (discrete and cont. actions).
        for _ in framework_iterator(config, ("torch", "tf")):
            for env in [
                    "CartPole-v0", "MsPacmanNoFrameskip-v4", "Pendulum-v0"]:
                print("Env={}".format(env))
                config["use_state_preprocessor"] = \
                    env == "MsPacmanNoFrameskip-v4"
                trainer = sac.SACTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    print(results)

    def test_sac_loss_function(self):
        """Tests SAC function results across all frameworks."""
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
        #config["Q_model"]["fcnet_activation"] = "linear"
        config["policy_model"]["fcnet_hiddens"] = [10]
        #config["policy_model"]["fcnet_activation"] = "linear"

        batch_size = 100
        for env in [SimpleEnv]:  #"Pendulum-v0"]:  #, "CartPole-v0"]:
            if env is SimpleEnv:
                obs_size = (batch_size, 1)
                actions = np.random.random(size=(batch_size, 1))
            elif env == "CartPole-v0":
                obs_size = (batch_size, 4)
                actions = np.random.randint(0, 2, size=(batch_size,))
            else:
                obs_size = (batch_size, 3)
                actions = np.random.random(size=(batch_size, 1))

            # Batch of size=n.
            input_ = {
                SampleBatch.CUR_OBS: np.random.random(size=obs_size),
                SampleBatch.ACTIONS: actions,
                SampleBatch.REWARDS: np.random.random(size=(batch_size,)),
                SampleBatch.DONES: np.random.choice(
                    [True, False], size=(batch_size,)),
                SampleBatch.NEXT_OBS: np.random.random(size=obs_size)
            }

            # Simply compare loss values AND grads of all frameworks with each
            # other.
            prev_fw_loss = weights_dict = None
            expect_c, expect_a, expect_e, expect_t = None, None, None, None
            for fw, sess in framework_iterator(config,
                                               frameworks=("tf", "torch"),
                                               session=True):
                # Generate Trainer and get its default Policy object.
                trainer = sac.SACTrainer(config=config, env=env)
                policy = trainer.get_policy()
                p_sess = None
                if sess:
                    p_sess = policy.get_session()

                # Set all weights (of all nets) to fixed values.
                if weights_dict is None:
                    assert fw == "tf"  # Start with the tf vars-dict.
                    weights_dict = policy.get_weights()
                else:
                    assert fw == "torch"  # Then transfer that to torch Model.
                    model_dict = self._translate_weights_to_torch(policy, weights_dict)
                    policy.model.load_state_dict(model_dict)
                    policy.target_model.load_state_dict(model_dict)

                if fw == "tf":
                    log_alpha = weights_dict["default_policy/log_alpha"]
                elif fw == "torch":
                    # Actually convert to torch tensors.
                    input_ = policy._lazy_tensor_dict(input_)
                    input_ = {k: input_[k] for k in input_.keys()}
                    log_alpha = policy.model.log_alpha.detach().numpy()[0]

                # Only run the expectation once, should be the same anyways
                # for all frameworks.
                if expect_c is None:
                    expect_c, expect_a, expect_e, expect_t = \
                        self._sac_loss_helper(input_, weights_dict,
                            list(weights_dict.keys()), log_alpha, fw,
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
                            feed_dict=policy._get_loss_inputs_dict(input_,
                                                                   shuffle=False))
                    tf_c_grads = [g for g, v in tf_c_grads]
                    tf_a_grads = [g for g, v in tf_a_grads]
                    tf_e_grads = [g for g, v in tf_e_grads]

                elif fw == "torch":
                    loss_torch(
                        policy, policy.model, None, input_)
                    c, a, e, t = policy.critic_loss, policy.actor_loss, \
                        policy.alpha_loss, policy.td_error

                    # Test actor gradients.
                    policy.actor_optim.zero_grad()
                    assert all(v.grad is None for v in policy.model.q_variables())
                    assert all(v.grad is None for v in policy.model.policy_variables())
                    assert policy.model.log_alpha.grad is None
                    a.backward()
                    # `actor_loss` depends on Q-net vars (but these grads must be ignored and overridden in critic_loss.backward!).
                    assert not any(v.grad is None for v in policy.model.q_variables())
                    assert not all(torch.mean(v.grad) == 0 for v in policy.model.policy_variables())
                    assert not all(torch.min(v.grad) == 0 for v in policy.model.policy_variables())
                    assert policy.model.log_alpha.grad is None
                    # Compare with tf ones.
                    torch_a_grads = [v.grad for v in policy.model.policy_variables()]
                    for tf_g, torch_g in zip(tf_a_grads, torch_a_grads):
                        if tf_g.shape != torch_g.shape:
                            check(tf_g, np.transpose(torch_g))
                        else:
                            check(tf_g, torch_g)

                    # Test critic gradients.
                    policy.critic_optim.zero_grad()
                    assert all(torch.mean(v.grad) == 0.0 for v in policy.model.q_variables())
                    assert all(torch.min(v.grad) == 0.0 for v in policy.model.q_variables())
                    #assert all(v.grad is None for v in policy.model.policy_variables())
                    assert policy.model.log_alpha.grad is None
                    c[0].backward()
                    assert not all(torch.mean(v.grad) == 0 for v in policy.model.q_variables())
                    assert not all(torch.min(v.grad) == 0 for v in policy.model.q_variables())
                    #assert all(v.grad is None for v in policy.model.policy_variables())
                    assert policy.model.log_alpha.grad is None
                    # Compare with tf ones.
                    torch_c_grads = [v.grad for v in policy.model.q_variables()]
                    for tf_g, torch_g in zip(tf_c_grads, torch_c_grads):
                        if tf_g.shape != torch_g.shape:
                            check(tf_g, np.transpose(torch_g))
                        else:
                            check(tf_g, torch_g)
                    # Compare (unchanged(!) actor grads) with tf ones.
                    torch_a_grads = [v.grad for v in policy.model.policy_variables()]
                    for tf_g, torch_g in zip(tf_a_grads, torch_a_grads):
                        if tf_g.shape != torch_g.shape:
                            check(tf_g, np.transpose(torch_g))
                        else:
                            check(tf_g, torch_g)

                    # Test alpha gradient.
                    policy.alpha_optim.zero_grad()
                    assert policy.model.log_alpha.grad is None
                    e.backward()
                    assert policy.model.log_alpha.grad is not None
                    check(policy.model.log_alpha.grad, tf_e_grads)

                print("actual losses: c={}, a={}, e={}, t={}".format(c, a, e, t))

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

                # Update weights from our batch.
                policy.learn_on_batch(input_)
                if fw == "tf":
                    updated_weights_dict = policy.get_weights()
                    # Target net should stay the same.
                    check(updated_weights_dict["default_policy/sequential_2/action_1/kernel"], weights_dict["default_policy/sequential_2/action_1/kernel"])
                    # Main net must have changed.
                    check(updated_weights_dict["default_policy/sequential/action_1/kernel"], weights_dict["default_policy/sequential/action_1/kernel"], false=True)
                # Compare with updated tf-weights. Must all be the same.
                else:
                    # Compare updated model.
                    for tf_var, torch_var in zip(list(updated_weights_dict.values())[:8], list(policy.model.state_dict().values())):
                        if tf_var.shape != torch_var.shape:
                            check(tf_var, np.transpose(torch_var))
                        else:
                            check(tf_var, torch_var)
                    # And alpha.
                    check(policy.model.log_alpha, updated_weights_dict["default_policy/log_alpha"])
                    # Compare target nets.
                    for tf_var, torch_var in zip(list(updated_weights_dict.values())[9:], list(policy.target_model.state_dict().values())):
                        if tf_var.shape != torch_var.shape:
                            check(tf_var, np.transpose(torch_var))
                        else:
                            check(tf_var, torch_var)

                prev_fw_loss = (c, a, e, t)

    def _sac_loss_helper(self, train_batch, weights, ks, log_alpha, fw, gamma,
                         sess):
        # ks:
        # 0=action hidden kernel
        # 1=action hidden bias
        # 2=action out kernel
        # 3=action out bias
        # 4=Q hidden kernel
        # 5=Q hidden bias
        # 6=Q out kernel
        # 7=Q out bias
        # 8=log_alpha
        # 13=target Q hidden kernel
        # 14=target Q hidden bias
        # 15=target Q out kernel
        # 16=target Q out bias
        alpha = np.exp(log_alpha)
        cls = TorchSquashedGaussian if fw == "torch" else SquashedGaussian
        model_out_t = train_batch[SampleBatch.CUR_OBS]
        model_out_tp1 = train_batch[SampleBatch.NEXT_OBS]
        target_model_out_tp1 = train_batch[SampleBatch.NEXT_OBS]

        # get_policy_output
        action_dist_t = cls(fc(relu(fc(
            model_out_t, weights[ks[0]], weights[ks[1]], framework=fw)),
            weights[ks[2]], weights[ks[3]]), None)
        policy_t = action_dist_t.deterministic_sample()
        log_pis_t = action_dist_t.logp(policy_t)
        if sess:
            log_pis_t = sess.run(log_pis_t)
            policy_t = sess.run(policy_t)
        log_pis_t = np.expand_dims(log_pis_t, -1)

        # Get policy output for t+1.
        action_dist_tp1 = cls(fc(relu(fc(
            model_out_tp1, weights[ks[0]], weights[ks[1]], framework=fw)),
            weights[ks[2]], weights[ks[3]]), None)
        policy_tp1 = action_dist_tp1.deterministic_sample()
        log_pis_tp1 = action_dist_tp1.logp(policy_tp1)
        if sess:
            log_pis_tp1 = sess.run(log_pis_tp1)
            policy_tp1 = sess.run(policy_tp1)
        log_pis_tp1 = np.expand_dims(log_pis_tp1, -1)

        # Q-values for the actually selected actions.
        # get_q_values
        q_t = fc(relu(fc(np.concatenate(
            [model_out_t, train_batch[SampleBatch.ACTIONS]], -1),
            weights[ks[4]], weights[ks[5]], framework=fw)),
            weights[ks[6]], weights[ks[7]], framework=fw)

        # Q-values for current policy in given current state.
        # get_q_values
        q_t_det_policy = fc(relu(fc(np.concatenate(
            [model_out_t, policy_t], -1),
            weights[ks[4]], weights[ks[5]], framework=fw)),
            weights[ks[6]], weights[ks[7]], framework=fw)

        # Target q network evaluation.
        # target_model.get_q_values
        q_tp1 = fc(relu(fc(np.concatenate(
            [target_model_out_tp1, policy_tp1], -1),
            weights[ks[13]], weights[ks[14]], framework=fw)),
            weights[ks[15]], weights[ks[16]], framework=fw)

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
            0.5 * np.mean(
                np.power(q_t_selected_target - q_t_selected, 2.0))
        ]
        target_entropy = -np.prod((1,))
        alpha_loss = -np.mean(log_alpha * (log_pis_t + target_entropy))
        actor_loss = np.mean(alpha * log_pis_t - q_t_det_policy)

        return critic_loss, actor_loss, alpha_loss, td_error

    def _translate_weights_to_torch(self, policy, weights_dict):
        map_ = {
            "default_policy/sequential/action_1/kernel": "action_model.action_0.weight",
            "default_policy/sequential/action_1/bias": "action_model.action_0.bias",
            "default_policy/sequential/action_out/kernel": "action_model.action_out.weight",
            "default_policy/sequential/action_out/bias": "action_model.action_out.bias",
            "default_policy/sequential_1/q_hidden_0/kernel": "q_net.q_hidden_0.weight",
            "default_policy/sequential_1/q_hidden_0/bias": "q_net.q_hidden_0.bias",
            "default_policy/sequential_1/q_out/kernel": "q_net.q_out.weight",
            "default_policy/sequential_1/q_out/bias": "q_net.q_out.bias",
        }
        model_dict = {map_[k]: convert_to_torch_tensor(np.transpose(v) if re.search("kernel", k) else v) for k, v in weights_dict.items() if re.search("sequential(/|_1)", k)}
        return model_dict


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
