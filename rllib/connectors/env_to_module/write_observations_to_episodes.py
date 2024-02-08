from typing import Any, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class WriteObservationsToEpisodes(ConnectorV2):
    """Writes the observations from the batch bact into the running episodes.

    - Operates on a batch that already has observations in it and a list of Episode
    objects.
    - Writes the observation(s) from the batch to all the given episodes. Thereby
    the number of observations in the batch must match the length of the list of
    episodes given.
    - Does NOT alter any observations (or other data) in the batch.
    - Can only be used in an EnvToModule pipeline (writing into Episode objects in a
    Learner pipeline does not make a lot of sense as - after the learner update - the
    list of episodes is discarded).

    .. testcode::

        TODO
    """

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
        observations = data.get(SampleBatch.OBS)

        if observations is None:
            raise ValueError(
                f"`batch` must already have a column named {SampleBatch.OBS} in it "
                f"for this connector to work!"
            )

        # Note that the following loop works with multi-agent as well as with
        # single-agent episode, as long as the following conditions are met (these
        # will be validated by `self.single_agent_episode_iterator()`):
        # - Per single agent episode, one observation item is expected to exist in
        # `data`, either in a list directly under the "obs" key OR for multi-agent:
        # in a list sitting under a key `(agent_id, module_id)` of a dict sitting
        # under the "obs" key.
        for sa_episode, obs in self.single_agent_episode_iterator(
            episodes=episodes, zip_with_batch_column=observations
        ):
            # Make sure episodes are NOT finalized yet (we are expecting to run in an
            # env-to-module pipeline).
            assert not sa_episode.is_finalized
            # Write new information into the episode.
            sa_episode.set_observations(at_indices=-1, new_data=obs)
            # Change the observation space of the sa_episode.
            sa_episode.observation_space = self.observation_space

        # Return the unchanged batch data.
        return data
