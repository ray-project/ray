from typing import Any, Collection, List, Optional

import gymnasium as gym
from gymnasium.spaces import Box
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import flatten_inputs_to_1d_tensor
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import AgentID, EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class FlattenObservations(ConnectorV2):
    """A connector piece that flattens all observation components into a 1D array.

    - Only works on data that has already been added to the batch.
    - This connector makes the assumption that under the Columns.OBS key in batch,
    there is either a list of individual env observations to be flattened (single-agent
    case) or a dict mapping agent- and module IDs to lists of data items to be
    flattened (multi-agent case).
    - Does NOT work in a Learner pipeline as it operates on individual observation
    items (as opposed to batched/time-ranked data).
    - Therefore, assumes that the altered (flattened) observations will be written
    back into the episode by a later connector piece in the env-to-module pipeline
    (which this piece is part of as well).
    - Does NOT read any information from the given list of Episode objects.
    - Does NOT write any observations (or other data) to the given Episode objects.

    .. testcode::

        import gymnasium as gym
        import numpy as np

        from ray.rllib.connectors.env_to_module import FlattenObservations
        from ray.rllib.utils.test_utils import check

        # Some arbitrarily nested, complex observation space.
        obs_space = gym.spaces.Dict({
            "a": gym.spaces.Box(-10.0, 10.0, (), np.float32),
            "b": gym.spaces.Tuple([
                gym.spaces.Discrete(2),
                gym.spaces.Box(-1.0, 1.0, (2, 1), np.float32),
            ]),
            "c": gym.spaces.MultiDiscrete([2, 3]),
        })
        act_space = gym.spaces.Discrete(2)

        # A batch of two example items, both coming from the above defined observation
        # space.
        batch = {
            "obs": [
                # 1st example item.
                {
                    "a": np.array(-10.0, np.float32),
                    "b": (1, np.array([[-1.0], [-1.0]], np.float32)),
                    "c": np.array([0, 2]),
                },
                # 2nd example item.
                {
                    "a": np.array(10.0, np.float32),
                    "b": (0, np.array([[1.0], [1.0]], np.float32)),
                    "c": np.array([1, 1]),
                },
            ],
        }

        # Construct our connector piece.
        connector = FlattenObservations(obs_space, act_space)

        # Call our connector piece with the example data.
        output_data = connector(
            rl_module=None,  # This connector works without an RLModule.
            data=batch,
            episodes=[],  # This connector does not need the `episodes` input.
            explore=True,
            shared_data={},
        )

        # The connector does not change the number of items in the data (still 2 items).
        check(len(output_data["obs"]), 2)

        # The connector has flattened each item in the data to a 1D tensor.
        check(
            output_data["obs"][0],
            #         box()  disc(2).  box(2, 1).  multidisc(2, 3)........
            np.array([-10.0, 0.0, 1.0, -1.0, -1.0, 1.0, 0.0, 0.0, 0.0, 1.0]),
        )
        check(
            output_data["obs"][1],
            #         box()  disc(2).  box(2, 1).  multidisc(2, 3)........
            np.array([10.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0]),
        )
    """

    @override(ConnectorV2)
    def recompute_observation_space_from_input_spaces(self):
        self._input_obs_base_struct = get_base_struct_from_space(
            self.input_observation_space
        )
        if self._multi_agent:
            spaces = {}
            for agent_id, space in self._input_obs_base_struct.items():
                if self._agent_ids and agent_id not in self._agent_ids:
                    spaces[agent_id] = self._input_obs_base_struct[agent_id]
                else:
                    sample = flatten_inputs_to_1d_tensor(
                        tree.map_structure(
                            lambda s: s.sample(),
                            self._input_obs_base_struct[agent_id],
                        ),
                        self._input_obs_base_struct[agent_id],
                        batch_axis=False,
                    )
                    spaces[agent_id] = Box(
                        float("-inf"), float("inf"), (len(sample),), np.float32
                    )
            return gym.spaces.Dict(spaces)
        else:
            sample = flatten_inputs_to_1d_tensor(
                tree.map_structure(
                    lambda s: s.sample(),
                    self._input_obs_base_struct,
                ),
                self._input_obs_base_struct,
                batch_axis=False,
            )
            return Box(float("-inf"), float("inf"), (len(sample),), np.float32)

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        multi_agent: bool = False,
        agent_ids: Optional[Collection[AgentID]] = None,
        **kwargs,
    ):
        """Initializes a FlattenObservations instance.

        Args:
            multi_agent: Whether this connector operates on multi-agent observations,
                in which case, the top-level of the Dict space (where agent IDs are
                mapped to individual agents' observation spaces) is left as-is.
            agent_ids: If multi_agent is True, this argument defines a collection of
                AgentIDs for which to flatten. AgentIDs not in this collection are
                ignored.
                If None, flatten observations for all AgentIDs. None is the default.
        """
        self._input_obs_base_struct = None
        self._multi_agent = multi_agent
        self._agent_ids = agent_ids

        super().__init__(input_observation_space, input_action_space, **kwargs)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Optional[Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        observations = data.get(Columns.OBS)

        if observations is None:
            raise ValueError(
                f"`batch` must already have a column named {Columns.OBS} in it "
                f"for this connector to work!"
            )

        # Process each item under the Columns.OBS key individually and flatten
        # it. We are using the `ConnectorV2.foreach_batch_item_change_in_place` API,
        # allowing us to not worry about multi- or single-agent setups and returning
        # the new version of each item we are iterating over.
        self.foreach_batch_item_change_in_place(
            batch=data,
            column=Columns.OBS,
            func=(
                lambda item, eps_id, agent_id, module_id: (
                    # Multi-agent AND skip this AgentID.
                    item
                    if self._agent_ids and agent_id not in self._agent_ids
                    # Single-agent or flatten this AgentIDs observation.
                    else flatten_inputs_to_1d_tensor(
                        item,
                        # In the multi-agent case, we need to use the specific agent's
                        # space struct, not the multi-agent observation space dict.
                        (
                            self._input_obs_base_struct
                            if not agent_id
                            else self._input_obs_base_struct[agent_id]
                        ),
                        # Our items are bare observations (no batch axis present).
                        batch_axis=False,
                    )
                )
            ),
        )
        return data
