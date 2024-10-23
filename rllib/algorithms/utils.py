import platform
from typing import List

import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import DeveloperAPI


@DeveloperAPI(stability="alpha")
@ray.remote(num_cpus=0, max_restarts=-1)
class AggregatorActor(FaultAwareApply):
    """Runs episode lists through ConnectorV2 pipeline and creates train batches.

    The actor should be co-located with a Learner worker. Ideally, there should be one
    or two aggregator actors per Learner worker (having even more per Learner probably
    won't help. Then the main process driving the RL algo can perform the following
    execution logic:
    - query n EnvRunners to sample the environment and return n lists of episodes as
    Ray.ObjectRefs.
    - remote call the set of aggregator actors (in round-robin fashion) with these
    list[episodes] refs in async fashion.
    - gather the results asynchronously, as each actor returns refs pointing to
    ready-to-go train batches.
    - as soon as we have at least one train batch per Learner, call the LearnerGroup
    with the (already sharded) refs.
    - an aggregator actor - when receiving p refs to List[EpisodeType] - does:
    -- ray.get() the actual p lists and concatenate the p lists into one
    List[EpisodeType].
    -- pass the lists of episodes through its LearnerConnector pipeline
    -- buffer the output batches of this pipeline until enough batches have been
    collected for creating one train batch (matching the config's
    `train_batch_size_per_learner`).
    -- concatenate q batches into a train batch and return that train batch.
    - the algo main process then passes the ray.ObjectRef to the ready-to-go train batch
    to the LearnerGroup for calling each Learner with one train batch.
    """

    def __init__(self, config: AlgorithmConfig):
        self.config = config
        self._learner_connector = self.config.build_learner_connector(
            input_observation_space=None,
            input_action_space=None,
        )
        self._rl_module = None
        self._batches = []
        self._len = 0

    def add_episode_refs(self, episode_refs: List[ray.ObjectRef]):
        episodes: List[EpisodeType] = tree.flatten(ray.get(episode_refs))

        batch = self._learner_connector(
            episodes=episodes,
            rl_module=self._rl_module,
        )

        self._batches.append(batch)
        # TODO (sven): fix counting
        self._len += len(batch["default_policy"]["rewards"])
        if self._len >= self.config.train_batch_size_per_learner:

            self._batches.clear()
            self._len = 0

    def get_host(self) -> str:
        return platform.node()
