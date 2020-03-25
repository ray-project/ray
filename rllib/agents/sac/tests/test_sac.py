from collections import OrderedDict
import numpy as np
from tensorflow.python.eager.context import eager_mode
import unittest

import ray.rllib.agents.sac as sac
from ray.rllib.agents.sac.sac_tf_policy import actor_critic_loss as loss_tf
from ray.rllib.agents.sac.sac_torch_policy import actor_critic_loss as \
    loss_torch
from ray.rllib.models.torch.torch_action_dist import TorchSquashedGaussian
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import fc
from ray.rllib.utils.test_utils import check

tf = try_import_tf()


class TestSAC(unittest.TestCase):
    def test_sac_compilation(self):
        """Test whether an SACTrainer can be built with all frameworks."""
        config = sac.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        #config["Q_model"]["fcnet_hiddens"] = [10]
        #config["Q_model"]["fcnet_activation"] = "linear"
        #config["policy_model"]["fcnet_hiddens"] = [10]
        #config["policy_model"]["fcnet_activation"] = "linear"
        #config["learning_starts"] = 0

        num_iterations = 200

        # eager (discrete and cont. actions).
        for fw in ["torch", "eager", "tf"]:
            print("framework={}".format(fw))

            config["eager"] = fw == "eager"
            config["use_pytorch"] = fw == "torch"

            eager_ctx = None
            if fw == "eager":
                eager_ctx = eager_mode()
                eager_ctx.__enter__()
                assert tf.executing_eagerly()
            elif fw == "tf":
                assert not tf.executing_eagerly()

            for env in ["Pendulum-v0", "CartPole-v0"]:
                print("Env={}".format(env))
                trainer = sac.SACTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    print(results)

            if eager_ctx:
                eager_ctx.__exit__(None, None, None)

    def test_sac_loss_function(self):
        """Tests SAC function results across all frameworks."""
        config = sac.DEFAULT_CONFIG.copy()
        # Run locally.
        config["num_workers"] = 0
        config["learning_starts"] = 0
        # Use very simple net (layer0=10 nodes, q-layer=2 nodes (2 actions)).
        config["Q_model"]["fcnet_hiddens"] = []
        config["Q_model"]["fcnet_activation"] = "linear"
        config["policy_model"]["fcnet_hiddens"] = []
        config["policy_model"]["fcnet_activation"] = "linear"

        #weights_list = [
        #    np.array([[-0.0966, 0.1535, -0.4125], [-0.2788, 0.3478, 0.0992]]),
        #    np.array([0.2616, 0.0625]),
        #    np.array([[0.0454, 0.0117, -0.2631, 0.4376]]),
        #    np.array([0.3334]),
        #    np.array([[-0.3358, 0.1139, 0.1755, 0.2831]]),
        #    np.array([0.0259]),
        #]
        #weights_torch = OrderedDict([
        #    ("action_model.action_out.weight", weights_list[0]),
        #    ("action_model.action_out.bias", weights_list[1]),
        #    ("q_net.q_out.weight", weights_list[2]),
        #    ("q_net.q_out.bias", weights_list[3]),
        #    ("twin_q_net.twin_q_out.weight", weights_list[4]),
        #    ("twin_q_net.twin_q_out.bias", weights_list[5])
        #])
        #weights_tf = {
        #    "default_policy/sequential/action_out/kernel": weights_list[0],
        #    "default_policy/sequential/action_out/bias": weights_list[1],
        #    "default_policy/sequential_1/q_out/kernel": weights_list[2],
        #    "default_policy/sequential_1/q_out/bias": weights_list[3],
        #    "default_policy/sequential_2/twin_q_out/kernel": weights_list[0],
        #    "default_policy/sequential_2/twin_q_out/bias": weights_list[0],

        #    "default_policy/sequential_3/action_out/kernel": weights_list[0],
        #    "default_policy/sequential_3/action_out/bias": weights_list[0],
        #    "default_policy/sequential_4/q_out/kernel": weights_list[0],
        #    "default_policy/sequential_4/q_out/bias": weights_list[0],
        #    "default_policy/sequential_5/twin_q_out/kernel": weights_list[0],
        #    "default_policy/sequential_5/twin_q_out/bias": weights_list[0],
        #}

        # ]

        batch_size = 100
        for env in ["Pendulum-v0", "CartPole-v0"]:
            if env == "CartPole-v0":
                obs_size = (batch_size, 4)
                actions = np.random.randint(0, 2, size=(batch_size,))
            else:
                obs_size = (batch_size, 3)
                actions = np.random.random(size=(batch_size, 1))

            # Batch of size=1000.
            input_ = {
                SampleBatch.CUR_OBS: np.random.random(size=obs_size),
                SampleBatch.ACTIONS: actions,
                SampleBatch.REWARDS: np.random.random(size=(batch_size,)),
                SampleBatch.DONES: np.random.choice(
                    [True, False], size=(batch_size,)),
                SampleBatch.NEXT_OBS: np.random.random(size=obs_size)
            }

            # Simply compare outputs of all frameworks with each other.
            #prev_fw_loss = None
            for fw in ["torch", "tf", "eager"]:
                print("framework={}".format(fw))
                config["use_pytorch"] = fw == "torch"
                config["eager"] = fw == "eager"
    
                # Generate Trainer and get its default Policy object.
                trainer = sac.SACTrainer(config=config, env=env)
                policy = trainer.get_policy()
                trainer.train()
                weights = policy.model.variables() + \
                    policy.target_model.variables()
                if fw == "torch":
                    input_ = policy._lazy_tensor_dict(input_)
                    input_ = {k: input_[k] for k in input_.keys()}
                    #w_ = {k: w[k] for k in weights_torch.keys()}
                    #policy.get_weights()
                    #policy.set_weights(w_)
                #elif fw == "tf":
                    #policy.set_weights(weights_tf)
                # Get actual out and compare to previous framework.
                total_loss = (loss_torch if fw == "torch" else
                       loss_tf)(policy, policy.model, None, input_)
                loss = (total_loss, policy.actor_loss,
                        policy.critic_loss, policy.alpha_loss)
                expected_loss = self._sac_loss_helper(input_, weights)
                #if prev_fw_loss is not None:
                #    for prev, current in zip(loss, prev_fw_loss):
                #        check(prev, current)

                #prev_fw_loss = \
                #    (total_loss, policy.actor_loss, policy.critic_loss,
                #     policy.alpha_loss)

    def _sac_loss_helper(self, input_, weights, alpha):
        model_out_t = input_[SampleBatch.CUR_OBS]
        model_out_tp1 = input_[SampleBatch.NEXT_OBS]
        target_model_out_tp1 = input_[SampleBatch.NEXT_OBS]

        #alpha = torch.exp(model.log_alpha)
    
        # Sample single actions from distribution.
        #action_dist_class = get_dist_class(policy.config,
        #                                   policy.action_space)
        action_dist_t = TorchSquashedGaussian(
            # get_policy_output
            fc(model_out_t, weights[0], weights[1]), None)
        policy_t = action_dist_t.sample().numpy()
        log_pis_t = np.expand_dims(action_dist_t.sampled_action_logp().numpy(),
                                   -1)
        action_dist_tp1 = TorchSquashedGaussian(
            fc(model_out_tp1, weights[0], weights[1]), None)
        policy_tp1 = action_dist_tp1.sample().numpy()
        log_pis_tp1 = np.expand_dims(
            action_dist_tp1.sampled_action_logp().numpy(), -1)
    
        # Q-values for the actually selected actions.
        q_t = model.get_q_values(model_out_t,
                                 train_batch[SampleBatch.ACTIONS])
        if policy.config["twin_q"]:
            twin_q_t = model.get_twin_q_values(
                model_out_t, train_batch[SampleBatch.ACTIONS])
    
        # Q-values for current policy in given current state.
        q_t_det_policy = model.get_q_values(model_out_t, policy_t)
        if policy.config["twin_q"]:
            twin_q_t_det_policy = model.get_twin_q_values(
                model_out_t, policy_t)
            q_t_det_policy = torch.min(
                q_t_det_policy, twin_q_t_det_policy)
    
        # Target q network evaluation.
        q_tp1 = policy.target_model.get_q_values(target_model_out_tp1,
                                                 policy_tp1)
        # print("q_tp1={}".format(q_tp1[0]))
        if policy.config["twin_q"]:
            twin_q_tp1 = policy.target_model.get_twin_q_values(
                target_model_out_tp1, policy_tp1)
            # print("twin_q_tp1={}".format(twin_q_tp1[0]))
            # Take min over both twin-NNs.
            q_tp1 = torch.min(q_tp1, twin_q_tp1)
        # print("q_tp1(min)={}".format(q_tp1[0]))
    
        q_t_selected = torch.squeeze(q_t, dim=-1)
        if policy.config["twin_q"]:
            twin_q_t_selected = torch.squeeze(twin_q_t, dim=-1)
        q_tp1 -= alpha * log_pis_tp1
        # print("q_tp1(min)-alpha log(pi(tp1))={}".format(q_tp1[0]))
    
        q_tp1_best = torch.squeeze(input=q_tp1, dim=-1)
        q_tp1_best_masked = (1.0 - train_batch[
            SampleBatch.DONES].float()) * \
                            q_tp1_best
        # print("q_tp1_best_masked={}".format(q_tp1_best_masked[0]))
    
        # compute RHS of bellman equation
        q_t_selected_target = (train_batch[SampleBatch.REWARDS] +
                               policy.config["gamma"] ** policy.config[
                                   "n_step"] *
                               q_tp1_best_masked).detach()
    
        # Compute the TD-error (potentially clipped).
        base_td_error = torch.abs(q_t_selected - q_t_selected_target)
        if policy.config["twin_q"]:
            twin_td_error = torch.abs(twin_q_t_selected - q_t_selected_target)
            td_error = 0.5 * (base_td_error + twin_td_error)
        else:
            td_error = base_td_error
    
        critic_loss = [
            0.5 * torch.mean(
                torch.pow(q_t_selected_target - q_t_selected, 2.0))
        ]
        if policy.config["twin_q"]:
            critic_loss.append(0.5 * torch.mean(torch.pow(
                q_t_selected_target - twin_q_t_selected, 2.0)))
    
        # Auto-calculate the target entropy.
        if policy.config["target_entropy"] == "auto":
            if model.discrete:
                target_entropy = -policy.action_space.n
            else:
                target_entropy = -np.prod(policy.action_space.shape)
        else:
            target_entropy = policy.config["target_entropy"]
        target_entropy = torch.Tensor([target_entropy]).float()
    
        alpha_loss = -torch.mean(
            model.log_alpha * (log_pis_t + target_entropy).detach())
        # print("alpha_loss={}".format(alpha_loss))
        actor_loss = torch.mean(alpha * log_pis_t - q_t_det_policy)
        # print("actor_loss={}".format(actor_loss))



if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
