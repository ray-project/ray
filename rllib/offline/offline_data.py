import logging
import numpy as np
from pathlib import Path
import ray
from typing import Dict, List

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner import Learner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.utils.typing import EpisodeType

logger = logging.getLogger(__name__)

# TODO (simon): Implement schema mapping for users, i.e. user define
# which row name to map to which default schema name below.
SCHEMA = [
    Columns.EPS_ID,
    Columns.AGENT_ID,
    Columns.MODULE_ID,
    Columns.OBS,
    Columns.ACTIONS,
    Columns.REWARDS,
    Columns.INFOS,
    Columns.NEXT_OBS,
    Columns.TERMINATEDS,
    Columns.TRUNCATEDS,
    Columns.T,
    # TODO (simon): Add remove as soon as we are new stack only.
    "agent_index",
    "dones",
    "unroll_id",
]


class OfflineData:
    def __init__(self, config: AlgorithmConfig):

        self.config = config
        self.is_multi_agent = config.is_multi_agent()
        self.path = (
            config.get("input_")
            if isinstance(config.get("input_"), list)
            else Path(config.get("input_"))
        )
        # Use `read_json` as default data read method.
        self.data_read_method = config.get("input_read_method", "read_json")
        # If the observation data is compressed. Note, if compressed, the
        # data must have been compressed with the `pack_if_needed` function.
        self.compressed = config.get("compressed", False)
        try:
            # TODO (simon): Add support for `kwargs`.
            self.data = getattr(ray.data, self.data_read_method)(self.path)
            logger.info("Reading data from {}".format(self.path))
            logger.info(self.data.schema())
        except Exception as e:
            logger.error(e)
        # Avoids reinstantiating the batch iterator each time we sample.
        self.batch_iterator = None
        self.locality_hints = None
        self.learner_handles = None

    def sample(
        self,
        num_samples: int,
        return_iterator: bool = False,
        num_shards: int = 1,
    ):
        if (
            not return_iterator
            or return_iterator
            and num_shards <= 1
            and not self.batch_iterator
        ):
            # If no iterator should be returned, or if we want to return a single
            # batch iterator, we instantiate the batch iterator once, here.

            # self.batch_iterator = self.data.map_batches(
            #     functools.partial(self._map_to_episodes, self.is_multi_agent)
            # ).iter_batches(
            #     batch_size=num_samples,
            #     prefetch_batches=1,
            #     local_shuffle_buffer_size=num_samples * 10,
            # )
            self.batch_iterator = self.data.map_batches(
                PreprocessEpisodes,
                fn_constructor_kwargs={
                    "config": self.config,
                    "learner": self.learner_handles[0],
                },
                concurrency=1,
            ).iter_batches(
                batch_size=num_samples,
                prefetch_batches=1,
                local_shuffle_buffer_size=num_samples * 10,
            )

        # Do we want to return an iterator or a single batch?
        if return_iterator:
            # In case of multiple shards, we return multiple
            # `StreamingSplitIterator` instances.
            if num_shards > 1:
                # return self.data.map_batches(
                #     functools.partial(self._map_to_episodes, self.is_multi_agent)
                # ).streaming_split(
                #     n=num_shards, equal=False, locality_hints=self.locality_hints
                # )
                return self.data.map_batches(
                    # TODO (cheng su): At best the learner handle passed in here should
                    # be the one from the learner that is nearest, but here we cannot
                    # provide locality hints.
                    PreprocessEpisodes,
                    config=self.config,
                    learner=self.learner_handles[0],
                    concurrency=num_shards,
                ).streaming_split(
                    n=num_shards, equal=False, locality_hints=self.locality_hints
                )

            # Otherwise, we return a simple batch `DataIterator`.
            else:
                return self.batch_iterator
        else:
            # Return a single batch from the iterator.
            return next(iter(self.batch_iterator))["batch"][0]  # ["episodes"]tolist()

    @staticmethod
    def _map_to_episodes(
        is_multi_agent: bool, batch: Dict[str, np.ndarray]
    ) -> Dict[str, List[EpisodeType]]:
        """Maps a batch of data to episodes."""

        episodes = []
        logger.warning(f"batch_size before: {batch['obs'].shape}")
        # TODO (simon): Give users possibility to provide a custom schema.
        for i, obs in enumerate(batch["obs"]):

            # If multi-agent we need to extract the agent ID.
            # TODO (simon): Check, what happens with the module ID.
            if is_multi_agent:
                agent_id = (
                    batch[Columns.AGENT_ID][i][0]
                    if Columns.AGENT_ID in batch
                    # The old stack uses "agent_index" instead of "agent_id".
                    # TODO (simon): Remove this as soon as we are new stack only.
                    else (
                        batch["agent_index"][i][0] if "agent_index" in batch else None
                    )
                )
            else:
                agent_id = None

            if is_multi_agent:
                # TODO (simon): Add support for multi-agent episodes.
                pass
            else:
                # TODO (simon): Check, if observations need to be packed into numpy
                # arrays.

                # Build a single-agent episode with a single row of the batch.
                episode = SingleAgentEpisode(
                    id_=batch[Columns.EPS_ID][i],
                    agent_id=agent_id,
                    observations=[
                        unpack_if_needed(obs),
                        unpack_if_needed(batch[Columns.NEXT_OBS][i]),
                    ],
                    infos=[
                        {},
                        batch[Columns.INFOS][i] if Columns.INFOS in batch else {},
                    ],
                    actions=[batch[Columns.ACTIONS][i]],
                    rewards=[batch[Columns.REWARDS][i]],
                    terminated=batch[
                        Columns.TERMINATEDS if Columns.TERMINATEDS in batch else "dones"
                    ][i],
                    truncated=batch[Columns.TRUNCATEDS][i]
                    if Columns.TRUNCATEDS in batch
                    else False,
                    # TODO (simon): Results in zero-length episodes in connector.
                    # t_started=batch[Columns.T if Columns.T in batch else
                    # "unroll_id"][i][0],
                    # TODO (simon): Single-dimensional columns are not supported.
                    extra_model_outputs={
                        k: [v[i]] for k, v in batch.items() if k not in SCHEMA
                    },
                    len_lookback_buffer=0,
                )
            episodes.append(episode)
        # Note, `map_batches` expects a `Dict` as return value.
        return {"episodes": episodes}


class PreprocessEpisodes:
    def __init__(self, config, learner):

        self.config = config
        # We need this learner to run the learner connector pipeline.
        self._learner = learner
        self._learner_connector = self.config.build_learner_connector(
            input_observation_space=None,
            input_action_space=None,
        )
        self._policies_to_train = self.config.policies_to_train
        self._is_multi_agent = config.is_multi_agent()
        if isinstance(self._learner, Learner):
            self.learner_is_remote = False
            self._module = self._learner._module
        else:
            self.learner_is_remote = True
            self._module = self.config.get_multi_agent_module_spec().build()

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, List[EpisodeType]]:
        # Map the batch to episodes.

        logger.warning("shape(batch): {batch.shape}")
        episodes = self._map_to_episodes(self._is_multi_agent, batch)
        logger.warning(f"len(episodes): {len(episodes['episodes'])}")
        # Synch the learner module.
        if self.learner_is_remote:
            result = self._learner.get_module_state.remote()
            weights = result.get()

            self._module.load_state_dict(weights)

        batch = self._learner_connector(
            rl_module=self._module,
            data={},
            episodes=episodes["episodes"],
            shared_data={},
        )
        print(f"batch: {batch}")
        batch = MultiAgentBatch(
            {
                module_id: SampleBatch(module_data)
                for module_id, module_data in batch.items()
            },
            # TODO (simon): This can be run once for the batch and the
            # metrics, but we run it twice: here and later in the learner.
            env_steps=sum(len(e) for e in episodes["episodes"]),
        )

        for module_id in list(batch.policy_batches.keys()):
            if not self._should_module_be_updated(module_id, batch):
                del batch.policy_batches[module_id]

        # TODO (simon): Log steps trained for metrics (how?). At best in learner
        # and not here. But we could precompute metrics here and pass it to the learner
        # for logging. Like this we do not have to pass around episode lists.

        # TODO (simon): episodes are only needed for logging here.
        return {"batch": [batch]}

    def _should_module_be_updated(self, module_id, multi_agent_batch=None):

        if not self._policies_to_train:
            return True
        elif not callable(self._policies_to_train):
            return module_id in set(self._policies_to_train)
        else:
            return self._policies_to_train(module_id, multi_agent_batch)

    @staticmethod
    def _map_to_episodes(
        is_multi_agent: bool, batch: Dict[str, np.ndarray]
    ) -> Dict[str, List[EpisodeType]]:
        """Maps a batch of data to episodes."""

        episodes = []
        # TODO (simon): Give users possibility to provide a custom schema.
        for i, obs in enumerate(batch["obs"]):

            # If multi-agent we need to extract the agent ID.
            # TODO (simon): Check, what happens with the module ID.
            if is_multi_agent:
                agent_id = (
                    batch[Columns.AGENT_ID][i]
                    if Columns.AGENT_ID in batch
                    # The old stack uses "agent_index" instead of "agent_id".
                    # TODO (simon): Remove this as soon as we are new stack only.
                    else (batch["agent_index"][i] if "agent_index" in batch else None)
                )
            else:
                agent_id = None

            if is_multi_agent:
                # TODO (simon): Add support for multi-agent episodes.
                pass
            else:
                # Build a single-agent episode with a single row of the batch.
                episode = SingleAgentEpisode(
                    id_=batch[Columns.EPS_ID][i],
                    agent_id=agent_id,
                    observations=[
                        unpack_if_needed(obs),
                        unpack_if_needed(batch[Columns.NEXT_OBS][i]),
                    ],
                    infos=[
                        {},
                        batch[Columns.INFOS][i] if Columns.INFOS in batch else {},
                    ],
                    actions=[batch[Columns.ACTIONS][i]],
                    rewards=[batch[Columns.REWARDS][i]],
                    terminated=batch[
                        Columns.TERMINATEDS if Columns.TERMINATEDS in batch else "dones"
                    ][i],
                    truncated=batch[Columns.TRUNCATEDS][i]
                    if Columns.TRUNCATEDS in batch
                    else False,
                    # TODO (simon): Results in zero-length episodes in connector.
                    # t_started=batch[Columns.T if Columns.T in batch else
                    # "unroll_id"][i][0],
                    # TODO (simon): Single-dimensional columns are not supported.
                    extra_model_outputs={
                        k: [v[i]] for k, v in batch.items() if k not in SCHEMA
                    },
                    len_lookback_buffer=0,
                )
            episodes.append(episode)
        # Note, `map_batches` expects a `Dict` as return value.
        return {"episodes": episodes}
