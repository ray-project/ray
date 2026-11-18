import tree  # pip install dm_tree

from ray.rllib.core.rl_module import RLModule
from ray.rllib.examples.envs.classes.multi_agent.footsies.game import constants
from ray.rllib.policy import sample_batch
from ray.rllib.utils.spaces.space_utils import batch as batch_func


class FixedRLModule(RLModule):
    def _forward_inference(self, batch, **kwargs):
        return self._fixed_forward(batch, **kwargs)

    def _forward_exploration(self, batch, **kwargs):
        return self._fixed_forward(batch, **kwargs)

    def _forward_train(self, *args, **kwargs):
        raise NotImplementedError(
            f"RLlib: {self.__class__.__name__} should not be trained. "
            f"It is a fixed RLModule, returning a fixed action for all observations."
        )

    def _fixed_forward(self, batch, **kwargs):
        """Implements a fixed that always returns the same action."""
        raise NotImplementedError(
            "FixedRLModule: This method should be overridden by subclasses to implement a specific action."
        )


class NoopFixedRLModule(FixedRLModule):
    def _fixed_forward(self, batch, **kwargs):
        obs_batch_size = len(tree.flatten(batch[sample_batch.SampleBatch.OBS])[0])
        actions = batch_func([constants.EnvActions.NONE for _ in range(obs_batch_size)])
        return {sample_batch.SampleBatch.ACTIONS: actions}


class BackFixedRLModule(FixedRLModule):
    def _fixed_forward(self, batch, **kwargs):
        obs_batch_size = len(tree.flatten(batch[sample_batch.SampleBatch.OBS])[0])
        actions = batch_func([constants.EnvActions.BACK for _ in range(obs_batch_size)])
        return {sample_batch.SampleBatch.ACTIONS: actions}
