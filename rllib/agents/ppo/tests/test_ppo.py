import numpy as np
import unittest

import ray
from ray.rllib.agents.impala.vtrace_policy import BEHAVIOR_LOGITS
import ray.rllib.agents.ppo as ppo
from ray.rllib.agents.ppo.ppo_tf_policy import postprocess_ppo_gae as \
    postprocess_ppo_gae_tf, PPOLoss as PPOLossTf
from ray.rllib.agents.ppo.ppo_torch_policy import postprocess_ppo_gae as \
    postprocess_ppo_gae_torch, PPOLoss as PPOLossTorch
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.policy import ACTION_LOGP
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.numpy import fc
from ray.rllib.utils.test_utils import check


class TestPPO(unittest.TestCase):

    ray.init()

    def test_ppo_compilation(self):
        """Test whether a PPOTrainer can be built with both frameworks."""
        config = ppo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        # tf.
        trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")

        num_iterations = 2
        for i in range(num_iterations):
            trainer.train()

        # Torch.
        config["use_pytorch"] = True
        config["simple_optimizer"] = True
        trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")
        for i in range(num_iterations):
            trainer.train()

    def test_ppo_loss_function(self):
        """Tests the PPO loss function math."""
        config = ppo.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["eager"] = True
        config["gamma"] = 0.99
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = "linear"

        # Fake CartPole episode of n time steps.
        train_batch = {
            SampleBatch.CUR_OBS: np.array([
                [0.1, 0.2, 0.3, 0.4],
                [0.5, 0.6, 0.7, 0.8],
                [0.9, 1.0, 1.1, 1.2]
            ]),
            SampleBatch.ACTIONS: np.array([0, 1, 1]),
            SampleBatch.REWARDS: np.array([1.0, -1.0, .5], dtype=np.float32),
            SampleBatch.DONES: np.array([False, False, True]),
            SampleBatch.VF_PREDS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
            BEHAVIOR_LOGITS: np.array([[-2., 0.5, -0.2], [-3., -0.3, 1.1], [-0.1, 2.5, -0.2]], dtype=np.float32),
            ACTION_LOGP: np.array([-0.5, -0.1, -0.2], dtype=np.float32)
        }

        # tf.
        trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")
        policy = trainer.get_policy()
        vars = policy.model.trainable_variables()

        # Post-process (calculate simple (non-GAE) advantages) and attach to
        # train_batch dict.
        # A = [0.99^2 * 0.5 + 0.99 * -1.0 + 1.0, 0.99 * 0.5 - 1.0, 0.5] =
        # [0.50005, -0.505, 0.5]
        train_batch = postprocess_ppo_gae_tf(policy, train_batch)
        # Check Advantage values.
        check(train_batch[Postprocessing.VALUE_TARGETS], [0.50005, -0.505, 0.5])

        # Actual loss results.
        loss_obj = PPOLossTf(
            Categorical,
            policy.model,
            train_batch[Postprocessing.VALUE_TARGETS],
            train_batch[Postprocessing.ADVANTAGES],
            train_batch[SampleBatch.ACTIONS],
            train_batch[BEHAVIOR_LOGITS],
            train_batch[ACTION_LOGP],
            train_batch[SampleBatch.VF_PREDS],
            Categorical(train_batch[BEHAVIOR_LOGITS], policy.model),
            policy.model.value_function(),
            policy.kl_coeff,
            None,  # RNN valid mask
            entropy_coeff=policy.entropy_coeff,
            clip_param=policy.config["clip_param"],
            vf_clip_param=policy.config["vf_clip_param"],
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            use_gae=policy.config["use_gae"]
        )

        # Calculate expected results.
        expected_logits = fc(
            fc(
                train_batch[SampleBatch.CUR_OBS],
                vars[0].numpy(), vars[1].numpy()
            ),
            vars[2].numpy(), vars[3].numpy()
        )
        expected_logp = Categorical(expected_logits, policy.model).logp(
            train_batch[SampleBatch.ACTIONS]
        )
        expected_loss = -np.mean(  # TODO: calculate
            expected_logp * train_batch[Postprocessing.ADVANTAGES]
        )
        check(loss_obj.loss.numpy(), expected_loss, decimals=4)

        # Torch.
        config["use_pytorch"] = True
        trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")
        policy = trainer.get_policy()
        train_batch = policy._lazy_tensor_dict(train_batch)
        results = PPOLossTorch(
            TorchCategorical,
            policy.model,
            train_batch[Postprocessing.VALUE_TARGETS],
            train_batch[Postprocessing.ADVANTAGES],
            train_batch[SampleBatch.ACTIONS],
            train_batch[BEHAVIOR_LOGITS],
            train_batch[ACTION_LOGP],
            train_batch[SampleBatch.VF_PREDS],
            TorchCategorical(train_batch[BEHAVIOR_LOGITS], policy.model),
            policy.model.value_function(),
            policy.kl_coeff,
            None,  # RNN valid mask
            entropy_coeff=policy.entropy_coeff,
            clip_param=policy.config["clip_param"],
            vf_clip_param=policy.config["vf_clip_param"],
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            use_gae=policy.config["use_gae"]
        )
        expected_logits = policy.model._last_output
        expected_logp = TorchCategorical(expected_logits, policy.model).logp(
            train_batch[SampleBatch.ACTIONS]
        )
        # TODO: calculate.
        expected_loss = -np.mean(
            expected_logp.detach().numpy() *
            train_batch[Postprocessing.ADVANTAGES].numpy()
        )
        check(results.detach().numpy(), expected_loss, decimals=4)

