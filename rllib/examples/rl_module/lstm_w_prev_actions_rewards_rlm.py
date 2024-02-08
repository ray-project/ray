import abc

from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.ppo.ppo_rl_module import PPORLModule
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch

_, tf, _ = try_import_tf()
torch, _ = try_import_torch()


class LSTMwPrevRewardsActionsRLMBase(PPORLModule):
    """RLModule taking previous n rewards and/or actions as input (and observations)."""

    def __init__(self, config: RLModuleConfig):
        super().__init__(config)

    def _forward_inference(self, batch, *args, **kwargs):
        batch = self._preprocess_batch(batch)
        return super()._forward_inference(batch, *args, **kwargs)

    def _forward_exploration(self, batch, *args, **kwargs):
        batch = self._preprocess_batch(batch)
        return super()._forward_exploration(batch, *args, **kwargs)

    def _forward_train(self, batch, *args, **kwargs):
        batch = self._preprocess_batch(batch)
        return super()._forward_train(batch, *args, **kwargs)

    @abc.abstractmethod
    def _preprocess_batch(self, batch):
        """Override to merge the columns prev. rewards/actions into the obs column.

        Args:
            batch: The input batch, including the two and the actual obs column.

        Returns:
            The input arg `batch`, but with an altered SampleBatch.OBS column, which
            now includes the prev n rewards and the prev n actions.
        """

class TorchLSTMwPrevRewardsActionsRLM(
    LSTMwPrevRewardsActionsRLMBase,
    PPOTorchRLModule,
):
    @override(LSTMwPrevRewardsActionsRLMBase)
    def _preprocess_batch(self, batch):
        # Observations. shape (b, [obs dim]).
        concat_obs = [batch[SampleBatch.OBS]]
        # Previous n rewards: shape (b, n).
        if SampleBatch.PREV_REWARDS in batch:
            concat_obs.append(batch[SampleBatch.PREV_REWARDS])
        # Previous n actions: shape (b, n, [action dim]).
        # Should already be one-hot'd (by the respective connector).
        if SampleBatch.PREV_ACTIONS in batch:
            concat_obs.append(batch[SampleBatch.PREV_ACTIONS])
        batch[SampleBatch.OBS] = torch.cat(concat_obs)
        return batch


class TfLSTMwPrevRewardsActionsRLM(
    LSTMwPrevRewardsActionsRLMBase,
    PPOTfRLModule,
):
    @override(LSTMwPrevRewardsActionsRLMBase)
    def _preprocess_batch(self, batch):
        shape = batch["prev_n_obs"].shape
        obs = tf.reshape(batch["prev_n_obs"], (shape[0], shape[1] * shape[2]))
        batch[SampleBatch.OBS] = obs
        return batch
