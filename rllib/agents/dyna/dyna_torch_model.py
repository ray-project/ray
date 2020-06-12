import gym
from gym.spaces import Discrete

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class DYNATorchModel(TorchModelV2, nn.Module):
    """Extension of standard TorchModelV2 for Env dynamics learning.

    Data flow:
        obs.cat(action) -> forward() -> next_obs|next_obs_delta
        get_next_state(obs, action) -> next_obs|next_obs_delta

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass.
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        """Initializes a DYNATorchModel object.
        """

        nn.Module.__init__(self)
        # Construct the wrapped model handing it a concat'd observation and
        # action space as "input_space" and our obs_space as "output_space".
        # TODO: (sven) get rid of these restrictions on obs/action spaces.
        assert isinstance(action_space, Discrete)
        input_space = gym.spaces.Box(
            obs_space.low[0],
            obs_space.high[0],
            shape=(obs_space.shape[0] + action_space.n, ))
        super(DYNATorchModel, self).__init__(input_space, action_space,
                                             num_outputs, model_config, name)

    def get_next_observation(self, observations, actions):
        """Return the Q estimates for the most recent forward pass.

        This implements Q(s, a).

        Arguments:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Optional[Tensor]): Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim]. If None (discrete action
                case), return Q-values for all actions.

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        actions_flat = nn.functional.one_hot(
            actions, num_classes=self.action_space.n).float()
        next_obs, _ = self.forward({
            "obs_flat": torch.cat([observations, actions_flat], -1)
        }, [], None)
        return next_obs
