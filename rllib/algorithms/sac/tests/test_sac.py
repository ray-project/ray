import gymnasium as gym
from gymnasium.spaces import Box, Dict, Discrete, Tuple
import numpy as np
import re
import unittest

import ray
from ray.rllib.algorithms import sac
from ray.rllib.examples.envs.classes.random_env import RandomEnv
from ray.rllib.examples._old_api_stack.models.batch_norm_model import (
    KerasBatchNormModel,
    TorchBatchNormModel,
)
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.tf.tf_action_dist import Dirichlet
from ray.rllib.models.torch.torch_action_dist import TorchDirichlet
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import fc, huber_loss, relu
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray import tune

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class SimpleEnv(gym.Env):
    def __init__(self, config):
        if config.get("simplex_actions", False):
            self.action_space = Simplex((2,))
        else:
            self.action_space = Box(0.0, 1.0, (1,))
        self.observation_space = Box(0.0, 1.0, (1,))
        self.max_steps = config.get("max_steps", 100)
        self.state = None
        self.steps = None

    def reset(self, *, seed=None, options=None):
        self.state = self.observation_space.sample()
        self.steps = 0
        return self.state, {}

    def step(self, action):
        self.steps += 1
        # Reward is 1.0 - (max(actions) - state).
        [rew] = 1.0 - np.abs(np.max(action) - self.state)
        terminated = False
        truncated = self.steps >= self.max_steps
        self.state = self.observation_space.sample()
        return self.state, rew, terminated, truncated, {}


class TestSAC(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        np.random.seed(42)
        torch.manual_seed(42)
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_sac_compilation(self):
        """Tests whether SAC can be built with all frameworks."""
        config = (
            sac.SACConfig()
            .training(
                n_step=3,
                twin_q=True,
                replay_buffer_config={
                    "capacity": 40000,
                },
                num_steps_sampled_before_learning_starts=0,
                store_buffer_in_checkpoints=True,
                train_batch_size=10,
            )
            .env_runners(num_env_runners=0, rollout_fragment_length=10)
        )
        num_iterations = 1

        ModelCatalog.register_custom_model("batch_norm", KerasBatchNormModel)
        ModelCatalog.register_custom_model("batch_norm_torch", TorchBatchNormModel)

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

        for fw in framework_iterator(config):
            # Test for different env types (discrete w/ and w/o image, + cont).
            for env in [
                "random_dict_env",
                "random_tuple_env",
                "CartPole-v1",
            ]:
                print("Env={}".format(env))
                config.environment(env)
                # Test making the Q-model a custom one for CartPole, otherwise,
                # use the default model.
                config.q_model_config["custom_model"] = (
                    "batch_norm{}".format("_torch" if fw == "torch" else "")
                    if env == "CartPole-v1"
                    else None
                )
                algo = config.build()
                for i in range(num_iterations):
                    results = algo.train()
                    check_train_results(results)
                    print(results)
                check_compute_single_action(algo)

                # Test, whether the replay buffer is saved along with
                # a checkpoint (no point in doing it for all frameworks since
                # this is framework agnostic).
                if fw == "tf" and env == "CartPole-v1":
                    checkpoint = algo.save()
                    new_algo = config.build()
                    new_algo.restore(checkpoint)
                    # Get some data from the buffer and compare.
                    data = algo.local_replay_buffer.replay_buffers[
                        "default_policy"
                    ]._storage[: 42 + 42]
                    new_data = new_algo.local_replay_buffer.replay_buffers[
                        "default_policy"
                    ]._storage[: 42 + 42]
                    check(data, new_data)
                    new_algo.stop()

                algo.stop()

    def test_sac_dict_obs_order(self):
        dict_space = Dict(
            {
                "img": Box(low=0, high=1, shape=(42, 42, 3)),
                "cont": Box(low=0, high=100, shape=(3,)),
            }
        )

        # Dict space .sample() returns an ordered dict.
        # Make sure the keys in samples are ordered differently.
        dict_samples = [
            {k: v for k, v in reversed(dict_space.sample().items())} for _ in range(10)
        ]

        class NestedDictEnv(gym.Env):
            def __init__(self):
                self.action_space = Box(low=-1.0, high=1.0, shape=(2,))
                self.observation_space = dict_space
                self.steps = 0

            def reset(self, *, seed=None, options=None):
                self.steps = 0
                return dict_samples[0], {}

            def step(self, action):
                self.steps += 1
                terminated = False
                truncated = self.steps >= 5
                return dict_samples[self.steps], 1, terminated, truncated, {}

        tune.register_env("nested", lambda _: NestedDictEnv())
        config = (
            sac.SACConfig()
            .environment("nested")
            .training(
                replay_buffer_config={
                    "capacity": 10,
                },
                num_steps_sampled_before_learning_starts=0,
                train_batch_size=5,
            )
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=5,
            )
            .experimental(_disable_preprocessor_api=True)
        )
        num_iterations = 1

        for _ in framework_iterator(config):
            algo = config.build()
            for _ in range(num_iterations):
                results = algo.train()
                check_train_results(results)
                print(results)
            check_compute_single_action(algo)

    def _get_batch_helper(self, obs_size, actions, batch_size):
        return SampleBatch(
            {
                SampleBatch.CUR_OBS: np.random.random(size=obs_size),
                SampleBatch.ACTIONS: actions,
                SampleBatch.REWARDS: np.random.random(size=(batch_size,)),
                SampleBatch.TERMINATEDS: np.random.choice(
                    [True, False], size=(batch_size,)
                ),
                SampleBatch.NEXT_OBS: np.random.random(size=obs_size),
                "weights": np.random.random(size=(batch_size,)),
                "batch_indexes": [0] * batch_size,
            }
        )

    def _sac_loss_helper(self, train_batch, weights, ks, log_alpha, fw, gamma, sess):
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
                relu(fc(model_out_t, weights[ks[1]], weights[ks[0]], framework=fw)),
                weights[ks[9]],
                weights[ks[8]],
            ),
            None,
        )
        policy_t = action_dist_t.deterministic_sample()
        log_pis_t = action_dist_t.logp(policy_t)
        if sess:
            log_pis_t = sess.run(log_pis_t)
            policy_t = sess.run(policy_t)
        log_pis_t = np.expand_dims(log_pis_t, -1)

        # Get policy output for t+1.
        action_dist_tp1 = cls(
            fc(
                relu(fc(model_out_tp1, weights[ks[1]], weights[ks[0]], framework=fw)),
                weights[ks[9]],
                weights[ks[8]],
            ),
            None,
        )
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
                fc(
                    np.concatenate([model_out_t, train_batch[SampleBatch.ACTIONS]], -1),
                    weights[ks[3]],
                    weights[ks[2]],
                    framework=fw,
                )
            ),
            weights[ks[11]],
            weights[ks[10]],
            framework=fw,
        )

        # Q-values for current policy in given current state.
        # get_q_values
        q_t_det_policy = fc(
            relu(
                fc(
                    np.concatenate([model_out_t, policy_t], -1),
                    weights[ks[3]],
                    weights[ks[2]],
                    framework=fw,
                )
            ),
            weights[ks[11]],
            weights[ks[10]],
            framework=fw,
        )

        # Target q network evaluation.
        # target_model.get_q_values
        if fw == "tf":
            q_tp1 = fc(
                relu(
                    fc(
                        np.concatenate([target_model_out_tp1, policy_tp1], -1),
                        weights[ks[7]],
                        weights[ks[6]],
                        framework=fw,
                    )
                ),
                weights[ks[15]],
                weights[ks[14]],
                framework=fw,
            )
        else:
            assert fw == "tf2"
            q_tp1 = fc(
                relu(
                    fc(
                        np.concatenate([target_model_out_tp1, policy_tp1], -1),
                        weights[ks[7]],
                        weights[ks[6]],
                        framework=fw,
                    )
                ),
                weights[ks[9]],
                weights[ks[8]],
                framework=fw,
            )

        q_t_selected = np.squeeze(q_t, axis=-1)
        q_tp1 -= alpha * log_pis_tp1
        q_tp1_best = np.squeeze(q_tp1, axis=-1)
        dones = train_batch[SampleBatch.TERMINATEDS]
        rewards = train_batch[SampleBatch.REWARDS]
        if fw == "torch":
            dones = dones.float().numpy()
            rewards = rewards.numpy()
        q_tp1_best_masked = (1.0 - dones) * q_tp1_best
        q_t_selected_target = rewards + gamma * q_tp1_best_masked
        base_td_error = np.abs(q_t_selected - q_t_selected_target)
        td_error = base_td_error
        critic_loss = [
            np.mean(
                train_batch["weights"] * huber_loss(q_t_selected_target - q_t_selected)
            )
        ]
        target_entropy = -np.prod((1,))
        alpha_loss = -np.mean(log_alpha * (log_pis_t + target_entropy))
        actor_loss = np.mean(alpha * log_pis_t - q_t_det_policy)

        return critic_loss, actor_loss, alpha_loss, td_error

    def _translate_weights_to_torch(self, weights_dict, map_):
        model_dict = {
            map_[k]: convert_to_torch_tensor(
                np.transpose(v)
                if re.search("kernel", k)
                else np.array([v])
                if re.search("log_alpha", k)
                else v
            )
            for i, (k, v) in enumerate(weights_dict.items())
            if i < 13
        }

        return model_dict

    def _translate_tf2_weights(self, weights_dict, map_):
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
