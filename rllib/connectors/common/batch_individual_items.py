from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import EpisodeType


class BatchIndividualItems(ConnectorV2):
    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        multi_agent: bool = False,
        **kwargs,
    ):
        """Initializes a BatchIndividualItems instance.

        Args:
            multi_agent: Whether this is a connector operating on a multi-agent
                observation space mapping AgentIDs to individual agents' observations.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )
        self._multi_agent = multi_agent

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
        is_marl_module = isinstance(rl_module, MultiAgentRLModule)

        # Convert lists of individual items into properly batched data.
        for column, column_data in data.copy().items():
            # Multi-agent case: This connector piece should only be used after(!)
            # the AgentToModuleMapping connector has already been applied, leading
            # to a batch structure of:
            # [module_id] -> [col0] -> [list of items]
            if is_marl_module and column in rl_module:
                # Case, in which a column has already been properly batched before this
                # connector piece is called.
                if not self._multi_agent:
                    continue
                # If MA Off-Policy and independent sampling we need to overcome
                # this check.
                module_data = column_data
                for col, col_data in module_data.copy().items():
                    if isinstance(col_data, list) and col != Columns.INFOS:
                        module_data[col] = batch(
                            col_data,
                            individual_items_already_have_batch_dim="auto",
                        )

            # Simple case: There is a list directly under `column`:
            # Batch the list.
            elif isinstance(column_data, list):
                data[column] = batch(
                    column_data,
                    individual_items_already_have_batch_dim="auto",
                )

            # Single-agent case: There is a dict under `column` mapping
            # `eps_id` to lists of items:
            # Sort by eps_id, concat all these lists, then batch.
            elif not self._multi_agent:
                # TODO: only really need this in non-Learner connector pipeline
                memorized_map_structure = []
                list_to_be_batched = []
                for (eps_id,) in column_data.keys():
                    for item in column_data[(eps_id,)]:
                        # Only record structure for OBS column.
                        if column == Columns.OBS:
                            memorized_map_structure.append(eps_id)
                        list_to_be_batched.append(item)
                # INFOS should not be batched (remain a list).
                data[column] = (
                    list_to_be_batched
                    if column == Columns.INFOS
                    else batch(
                        list_to_be_batched,
                        individual_items_already_have_batch_dim="auto",
                    )
                )
                if is_marl_module:
                    if DEFAULT_MODULE_ID not in data:
                        data[DEFAULT_MODULE_ID] = {}
                    data[DEFAULT_MODULE_ID][column] = data.pop(column)

                # Only record structure for OBS column.
                if column == Columns.OBS:
                    shared_data["memorized_map_structure"] = memorized_map_structure
            # Multi-agent case: This should already be covered above.
            # This connector piece should only be used after(!)
            # the AgentToModuleMapping connector has already been applied, leading
            # to a batch structure of:
            # [module_id] -> [col0] -> [list of items]
            else:
                raise NotImplementedError

        return data
