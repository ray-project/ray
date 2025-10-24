from typing import Any, List, Optional

import gymnasium as gym
import numpy as np

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import softmax
from ray.rllib.utils.torch_utils import FLOAT_MIN
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI

torch, _ = try_import_torch()


@PublicAPI(stability="alpha")
class ActionMaskingOffPolicy(ConnectorV2):
    """Connector handling a simple action masking process for discrete action spaces.

    User needs to provide the key within the observation dict (observation space
    must be a Dict space), under which the multi-hot encoded allowed actions can be
    found. This is a 1D vector with the same length as the action space (must also
    be provided in the constructor) and contains 1.0s at those slots corresponding to
    actions that are allowed and 0.0s otherwise. For example, if the action space
    is Discrete(5), and the key in the observation is "allowed_actions", and the
    observation is {"xyz": ..., "allowed_actions": [1.0, 0.0, 0.0, 1.0, 0.0]}, then
    only actions 0 and 3 will be allowed in the next `env.step([actions])` call.

    - The connector will mask all non-allowed action logits to RLlib's FLOAT_MIN
    (see rllib.utils.torch_utils::FLOAT_MIN).
    - The connector will only change Columns.ACTION_DIST_INPUTS information in the
    RLModule returned batch (by masking non-allowed logits as described above).
    - The connector will NOT perform the actual action sampling step, which is still
    left for the default module-to-env connector to do.
    - The connector will only look at the last observation or info dict and extract the
    allowed actions from there.
    - The connector will not write data into the running episodes.
    - When using this connector, the RLModule does not actually need to know anything
    about action masking. The user can thus use any regular non-action-masking-aware
    model.
    - When using this connector AND the allowed actions information is coming from the
    observations (instead of info dicts), the user should make sure that the observation
    dict is properly flattened or otherwise processed before going into the RLModule,
    unless the RLModule is able to handle the additional allowed actions key.
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        allowed_actions_key: str = "allowed_actions",
        allowed_actions_location: str = "infos",
        **kwargs,
    ):
        """Initializes a ActionMasking (connector piece) instance."""
        super().__init__(input_observation_space, input_action_space, **kwargs)

        self.allowed_actions_key = allowed_actions_key
        self.allowed_actions_location = allowed_actions_location
        assert self.allowed_actions_location in ["infos", "observations"]

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        if batch.get(Columns.ACTION_DIST_INPUTS) is None:
            raise ValueError(
                f"`batch` (RLModule output) must already have a column named "
                " {Columns.ACTION_DIST_INPUTS} in it for this connector to work!"
            )

        allowed_actions = []
        for sa_episode in self.single_agent_episode_iterator(episodes):
            allowed_actions.append(
                getattr(
                    sa_episode,
                    # Call `get_observations` or `get_infos` method.
                    f"get_{self.allowed_actions_location}",
                )(indices=-1)[self.allowed_actions_key]
            )
        for i, (action, allowed, logits) in enumerate(zip(
            list(batch[Columns.ACTIONS]),
            allowed_actions,
            list(batch[Columns.ACTION_DIST_INPUTS]),
        )):
            if action in allowed:
                continue
            action_mask = np.zeros(shape=(logits.shape[0],), dtype=np.float32)
            for j in allowed:
                action_mask[j] = 1.0
            # Convert action_mask into a [0.0 || -inf]-type mask.
            action_mask = np.clip(np.log(action_mask), a_min=FLOAT_MIN, a_max=None)

            # Create new logits and just sample from them using numpy.
            new_logits = logits.detach().numpy() + action_mask
            new_action = np.random.choice(
                np.arange(0, len(new_logits)),
                p=softmax(new_logits, epsilon=FLOAT_MIN),
            )
            batch[Columns.ACTIONS][i] = new_action

        return batch
