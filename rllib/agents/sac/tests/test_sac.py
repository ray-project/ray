from gym import Env
from gym.spaces import Box
import numpy as np
import unittest

import ray.rllib.agents.sac as sac
from ray.rllib.agents.sac.sac_tf_policy import actor_critic_loss as loss_tf
from ray.rllib.agents.sac.sac_torch_policy import actor_critic_loss as \
    loss_torch
from ray.rllib.models.tf.tf_action_dist import SquashedGaussian
from ray.rllib.models.torch.torch_action_dist import TorchSquashedGaussian
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import fc
from ray.rllib.utils.test_utils import check, framework_iterator

tf = try_import_tf()


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
    def test_sac_pytorch_learning_cont(self):
        config = sac.DEFAULT_CONFIG.copy()
        config["use_pytorch"] = True
        config["num_workers"] = 0  # Run locally.
        config["twin_q"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        config["initial_alpha"] = 0.1
        #config["env"] = SimpleEnv
        #config["optimization"]["critic_learning_rate"] = 0.1

        config["Q_model"]["fcnet_hiddens"] = [32]
        #config["Q_model"]["fcnet_activation"] = "linear"
        config["policy_model"]["fcnet_hiddens"] = [32]
        #config["policy_model"]["fcnet_activation"] = "linear"

        num_iterations = 2000

        trainer = sac.SACTrainer(config=config, env=SimpleEnv)
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
        #config["optimization"]["critic_learning_rate"] = 0.1

        #config["Q_model"]["fcnet_hiddens"] = [10]
        #config["Q_model"]["fcnet_activation"] = "linear"
        #config["policy_model"]["fcnet_hiddens"] = [10]
        #config["policy_model"]["fcnet_activation"] = "linear"

        num_iterations = 2

        # eager (discrete and cont. actions).
        for _ in framework_iterator(config):
            for env in [
                    "CartPole-v0",
                    "Pendulum-v0",
            ]:
                print("Env={}".format(env))
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
        # Use very simple nets.
        config["Q_model"]["fcnet_hiddens"] = [10]
        config["Q_model"]["fcnet_activation"] = "linear"
        config["policy_model"]["fcnet_hiddens"] = [10]
        config["policy_model"]["fcnet_activation"] = "linear"

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
            for fw in framework_iterator(config):
                # Generate Trainer and get its default Policy object.
                trainer = sac.SACTrainer(config=config, env=env)
                policy = trainer.get_policy()
                # Set all weights (of all nets) to fixed values.
                if weights_dict is None:
                    weights_dict = policy.get_weights()
                else:
                    policy.set_weights(weights_dict)

                #trainer.train()

                #weights = policy.model.variables() + \
                #    policy.target_model.variables()
                if fw == "torch":
                    input_ = policy._lazy_tensor_dict(input_)
                    input_ = {k: input_[k] for k in input_.keys()}
                    alpha = np.exp(policy.model.log_alpha.detach().numpy())
                elif fw == "tf":
                #    weights = policy.get_session().run(weights)
                #    # Pop out alpha to make weights same as torch weights.
                    alpha = weights_dict["default_policy/log_alpha"]

                # Get actual out and compare to previous framework.
                if fw == "torch":
                    loss_torch(
                        policy, policy.model, None, input_, deterministic=True)
                    c, a, e = policy.critic_loss, policy.actor_loss, policy.alpha_loss
                else:
                    c, a, e = policy.get_session().run([
                        policy.critic_loss,
                        policy.actor_loss,
                        policy.alpha_loss],
                        feed_dict=policy._get_loss_inputs_dict(input_,
                                                               shuffle=False))
                #loss = (
                #    policy.actor_loss, policy.critic_loss, policy.alpha_loss)
                expected_loss = self._sac_loss_helper(input_, weights_dict, alpha, fw)
                check(loss, expected_loss)

    def _sac_loss_helper(self, train_batch, weights, alpha, fw):
        cls = TorchSquashedGaussian if fw == "torch" else SquashedGaussian
        model_out_t = train_batch[SampleBatch.CUR_OBS]
        model_out_tp1 = train_batch[SampleBatch.NEXT_OBS]
        target_model_out_tp1 = train_batch[SampleBatch.NEXT_OBS]

        # get_policy_output
        action_dist_t = cls(fc(model_out_t, weights[0], weights[1]), None)
        policy_t = action_dist_t.deterministic_sample().detach().numpy()
        log_pis_t = np.expand_dims(action_dist_t.sampled_action_logp().detach().numpy(),
                                   -1)
        action_dist_tp1 = cls(fc(model_out_tp1, weights[0], weights[1]), None)
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
