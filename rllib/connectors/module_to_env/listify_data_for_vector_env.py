from typing import Any, Dict, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch as batch_fn
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ListifyDataForVectorEnv(ConnectorV2):
    """Performs conversion from ConnectorV2-style format to env/episode insertion.

    Note: This is one of the default module-to-env ConnectorV2 pieces that
    are added automatically by RLlib into every module-to-env connector pipeline,
    unless `config.add_default_connectors_to_module_to_env_pipeline` is set to
    False.

    The default module-to-env connector pipeline is:
    [
        GetActions,
        TensorToNumpy,
        UnBatchToIndividualItems,
        ModuleToAgentUnmapping,  # only in multi-agent setups!
        RemoveSingleTsTimeRankFromBatch,

        [0 or more user defined ConnectorV2 pieces],

        NormalizeAndClipActions,
        ListifyDataForVectorEnv,
    ]

    Single agent case:
    Convert from:
    [col] -> [(episode_id,)] -> [list of items].
    To:
    [col] -> [list of items].

    Multi-agent case:
    Convert from:
    [col] -> [(episode_id, agent_id, module_id)] -> list of items.
    To:
    [col] -> [list of multi-agent dicts].
    """

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        for column, column_data in batch.copy().items():
            # Multi-agent case: Create lists of multi-agent dicts under each column.
            if isinstance(episodes[0], MultiAgentEpisode):
                # TODO (sven): Support vectorized MultiAgentEnv
                assert len(episodes) == 1
                new_column_data = [{}]

                for key, value in batch[column].items():
                    assert len(value) == 1
                    eps_id, agent_id, module_id = key
                    new_column_data[0][agent_id] = value[0]
                batch[column] = new_column_data
            # Single-agent case: Create simple lists under each column.
            else:
                batch[column] = [
                    d for key in batch[column].keys() for d in batch[column][key]
                ]
                # Batch actions for (single-agent) gym.vector.Env.
                # All other columns, leave listify'ed.
                if column in [Columns.ACTIONS_FOR_ENV, Columns.ACTIONS]:
                    batch[column] = batch_fn(batch[column])

        return batch
