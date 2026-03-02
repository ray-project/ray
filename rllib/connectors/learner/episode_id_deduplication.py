from typing import Any, Dict, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class EpisodeIdDeduplication(ConnectorV2):
    """Ensures unique episode IDs across all episodes in the learner pipeline.

    Must be the first connector in the learner pipeline. All subsequent connectors
    write data to the batch using keys derived from episode IDs; those keys must be
    unique across the entire training batch or data from different episodes will
    collide into the same batch slot.

    Two scenarios require de-duplication:

    1. **Multi-agent setups**: Multiple chunks of the same ongoing episode may
       arrive in one training batch (e.g. in APPO, when a long episode spans more
       than one rollout window, or when sub-games within a single
       `MultiAgentEpisode` rollout are collected). All such chunks share the same
       `MultiAgentEpisode.id_`, so their child
       `SingleAgentEpisode.multi_agent_episode_id` values collide. This connector
       appends a running index to `ma_episode.id_` and propagates it to each child
       SA episode's `multi_agent_episode_id`.

    2. **Single-agent setups**: Same scenario for plain `SingleAgentEpisode`
       objects: two chunks from the same episode share `id_`. This connector
       appends a running index to make them unique.

    This was previously handled as a side-effect inside
    `AddOneTsToEpisodesAndTruncate`. Extracting it into a dedicated connector
    satisfies the TODOs in `AddOneTsToEpisodesAndTruncate` and
    `AddObservationsFromEpisodesToBatch`.
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
        for i, episode in enumerate(episodes):
            if isinstance(episode, MultiAgentEpisode):
                # Make this MA episode's ID unique in case multiple chunks from
                # the same ongoing episode appear in the training batch.
                episode.id_ += "_" + str(i)
                # Propagate the new MA episode ID to all child SA episodes so
                # that their batch key (multi_agent_episode_id, agent_id,
                # module_id) is unique per episode.
                for sa_episode in episode.agent_episodes.values():
                    sa_episode.multi_agent_episode_id = episode.id_
            else:
                # Single-agent case: make the SA episode's own ID unique.
                episode.id_ += "_" + str(i)

        return batch
