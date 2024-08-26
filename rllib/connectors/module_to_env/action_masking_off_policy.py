from typing import Any, List, Optional

import gymnasium as gym
import numpy as np

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.torch_utils import FLOAT_MIN
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


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
        data: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        logits = data.get(Columns.ACTION_DIST_INPUTS)

        if logits is None:
            raise ValueError(
                f"`data` (RLModule output) must already have a column named "
                " {Columns.ACTION_DIST_INPUTS} in it for this connector to work!"
            )

        for sa_episode in self.single_agent_episode_iterator(episodes):
            self.add_batch_item(
                batch=data,
                column="_tmp_allowed_actions",
                item_to_add=getattr(
                    sa_episode,
                    # Call `get_observations` or `get_infos` method.
                    f"get_{self.allowed_actions_location}",
                )(indices=-1)[self.allowed_actions_key],
                single_agent_episode=sa_episode,
            )
            #self.add_batch_item(
            #    batch=data,
            #    column="_dist_inputs_to_sample_from",
            #    item_to_add=getattr(
            #        sa_episode,
            #        # Call `get_observations` or `get_infos` method.
            #        f"get_{self.allowed_actions_location}",
            #    )(indices=-1)[self.allowed_actions_key],
            #    single_agent_episode=sa_episode,
            #)

        self.foreach_batch_item_change_in_place(
            batch=data,
            column=[Columns.ACTION_DIST_INPUTS, "_tmp_allowed_actions"],
            func=self._change_logits_in_place,
        )

        # Cleanup: Remove structured allowed-actions data from batch again.
        data.pop("_tmp_allowed_actions")

        return data

    @staticmethod
    def _change_logits_in_place(logits_and_allowed, env_idx, agent_id, module_id):
        logits, allowed = logits_and_allowed

        action_mask = np.zeros(shape=(logits.shape[0],), dtype=np.float32)
        for i in allowed:
            action_mask[i] = 1.0
        # Convert action_mask into a [0.0 || -inf]-type mask.
        action_mask = np.clip(np.log(action_mask), a_min=FLOAT_MIN, a_max=None)

        # Masked logits to be returned. First mask everything.
        changed_logits = logits + action_mask
        #changed_logits = np.full(
        #    shape=(logits.shape[0],),
        #    fill_value=FLOAT_MIN,
        #    dtype=np.float32,
        #)
        # Then unmask those action slots that are allowed back to their original
        # (RLModule produced) logit values.
        #for i in allowed:
        #    changed_logits[i] = logits[i]
        return (changed_logits, allowed)

