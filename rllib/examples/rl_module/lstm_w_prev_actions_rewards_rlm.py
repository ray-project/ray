from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.ppo.ppo_rl_module import PPORLModule
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.framework import try_import_tf, try_import_torch
import gymnasium as gym

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


class LSTMwPrevRewardsActionsRLM(PPORLModule):
    """RLModule taking previous n rewards and/or actions as input (plus observations)."""

    def __init__(self, config: RLModuleConfig):
        super().__init__(config)

    def _forward_inference(self, batch, *args, **kwargs):
        batch = self._preprocess_batch(batch)
        return super()._forward_inference(batch, *args, **kwargs)

    def _forward_train(self, batch, *args, **kwargs):
        batch = self._preprocess_batch(batch)
        return super()._forward_train(batch, *args, **kwargs)

    def _forward_exploration(self, batch, *args, **kwargs):
        batch = self._preprocess_batch(batch)
        return super()._forward_exploration(batch, *args, **kwargs)


class TorchFrameStackingCartPoleRLM(FrameStackingCartPoleRLMBase, PPOTorchRLModule):
    @staticmethod
    def _preprocess_batch(batch):
        shape = batch["prev_n_obs"].shape
        obs = batch["prev_n_obs"].reshape((shape[0], shape[1] * shape[2]))
        batch[SampleBatch.OBS] = obs
        return batch


class TfFrameStackingCartPoleRLM(FrameStackingCartPoleRLMBase, PPOTfRLModule):
    @staticmethod
    def _preprocess_batch(batch):
        shape = batch["prev_n_obs"].shape
        obs = tf.reshape(batch["prev_n_obs"], (shape[0], shape[1] * shape[2]))
        batch[SampleBatch.OBS] = obs
        return batch
