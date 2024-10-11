import platform

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.util.annotations import DeveloperAPI


@DeveloperAPI(stability="alpha")
@ray.remote(num_cpus=0, max_restarts=-1)
class AggregatorActor(FaultAwareApply):
    """Runs episode lists through ConnectorV2 pipeline and concats to a train batch.

    The actor should be co-located with a Learner worker. Ideally, there should be one
    or two aggregator actors per Learner worker (having even more per Learner probably
    won't help. Then the main process driving the RL algo can perform the following
    execution logic:
    - query n EnvRunners to sample the environment and return n lists of episodes, but
    as Ray.ObjectRefs.
    - pass these refs (in round-robin fashion) to the set of aggregator actors. Each
    actor returns a new ref pointing to a ready-to-go train batch.
    - the aggregator actor - when receiving p refs to List[EpisodeType] - does:
    -- ray.get() the actual p lists
    -- pass the lists of episodes through its LearnerConnector pipeline
    -- buffering the output batches of this pipeline until enough batches have been
    collected to create one proper train batch (matching the config's
    `train_batch_size_per_learner`).
    -- concatenating q batches into a train batch and returning that train batch.
    - the algo main process then passes the ray.ObjectRef to the ready-to-go train batch
    to the m Learner workers for updating the model.
    """

    def __init__(self, config: AlgorithmConfig):
        self.config = config
        self._learner_connector = self.config.build_learner_connector(
            input_observation_space=None,
            input_action_space=None,
        )
        self._rl_module = None

    def process_episodes(self, episode_refs: List[ray.ObjectRef]):
        episodes = tree.flatten(ray.get(episode_refs))

        batch = self._learner_connector(
            episodes=episodes,
            rl_module=self._rl_module,
        )

        return batch

    def get_host(self) -> str:
        return platform.node()
