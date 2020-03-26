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
        config["twin_q"] = True
        config["soft_horizon"] = True
        config["clip_actions"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        #config["optimization"]["critic_learning_rate"] = 0.1

        #config["Q_model"]["fcnet_hiddens"] = [10]
        #config["Q_model"]["fcnet_activation"] = "linear"
        #config["policy_model"]["fcnet_hiddens"] = [10]
        #config["policy_model"]["fcnet_activation"] = "linear"

        num_iterations = 2000

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
        config["twin_q"] = False
        config["gamma"] = 0.99
        # Use very simple net (layer0=10 nodes, q-layer=2 nodes (2 actions)).
        config["Q_model"]["fcnet_hiddens"] = []
        config["Q_model"]["fcnet_activation"] = "linear"
        config["policy_model"]["fcnet_hiddens"] = []
        config["policy_model"]["fcnet_activation"] = "linear"

        batch_size = 100
        for env in ["Pendulum-v0"]:  #, "CartPole-v0"]:
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
                if fw == "eager":
                    continue

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
                    alpha = np.exp(policy.model.log_alpha.detach().numpy())
                elif fw == "tf":
                    weights = policy.get_session().run(weights)
                    # Pop out alpha to make weights same as torch weights.
                    alpha = weights.pop(4)
                # Get actual out and compare to previous framework.
                total_loss = (loss_torch if fw == "torch" else
                       loss_tf)(policy, policy.model, None, input_, deterministic=True)
                loss = (
                    policy.actor_loss, policy.critic_loss, policy.alpha_loss)
                expected_loss = self._sac_loss_helper(input_, weights, alpha)
                check(loss, expected_loss)

    def _sac_loss_helper(self, train_batch, weights, alpha):
        model_out_t = train_batch[SampleBatch.CUR_OBS]
        model_out_tp1 = train_batch[SampleBatch.NEXT_OBS]
        target_model_out_tp1 = train_batch[SampleBatch.NEXT_OBS]
    
        action_dist_t = TorchSquashedGaussian(
            # get_policy_output
            fc(model_out_t, weights[0], weights[1]), None)
        policy_t = action_dist_t.deterministic_sample().detach().numpy()
        log_pis_t = np.expand_dims(action_dist_t.sampled_action_logp().detach().numpy(),
                                   -1)
        action_dist_tp1 = TorchSquashedGaussian(
            fc(model_out_tp1, weights[0], weights[1]), None)
        policy_tp1 = action_dist_tp1.deterministic_sample().detach().numpy()
        log_pis_tp1 = np.expand_dims(
            action_dist_tp1.sampled_action_logp().detach().numpy(), -1)
    
        # Q-values for the actually selected actions.
        # get_q_values
        q_t = fc(np.concatenate([model_out_t, train_batch[SampleBatch.ACTIONS]], -1), weights[2], weights[3])

        # Q-values for current policy in given current state.
        # get_q_values
        q_t_det_policy = fc(np.concatenate([model_out_t, policy_t], -1), weights[2], weights[3])

        # Target q network evaluation.
        # target_model.get_q_values
        q_tp1 = fc(np.concatenate([target_model_out_tp1, policy_tp1], -1), weights[6], weights[7])
        q_t_selected = np.squeeze(q_t, axis=-1)
        q_tp1 -= alpha * log_pis_tp1
        q_tp1_best = np.squeeze(q_tp1, axis=-1)
        q_tp1_best_masked = (1.0 - train_batch[SampleBatch.DONES].float().numpy()) * \
                            q_tp1_best
        # compute RHS of bellman equation
        q_t_selected_target = train_batch[SampleBatch.REWARDS].numpy() + \
                              0.99 * q_tp1_best_masked
        # Compute the TD-error (potentially clipped).
        base_td_error = np.abs(q_t_selected - q_t_selected_target)
        td_error = base_td_error

        critic_loss = [
            0.5 * np.mean(
                np.power(q_t_selected_target - q_t_selected, 2.0))
        ]
        # Auto-calculate the target entropy.
        target_entropy = -np.prod((1,))
    
        alpha_loss = -np.mean(np.log(alpha) * (log_pis_t + target_entropy))
        actor_loss = np.mean(alpha * log_pis_t - q_t_det_policy)
        return actor_loss, critic_loss, alpha_loss


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
