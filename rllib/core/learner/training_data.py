from collections import defaultdict
import dataclasses
from typing import List, Optional

import tree  # pip install dm_tree

import ray
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.minibatch_utils import (
    ShardBatchIterator,
    ShardEpisodesIterator,
    ShardObjectRefIterator,
)
from ray.rllib.utils.typing import EpisodeType


# TODO (sven): Switch to dataclass(slots=True) once on py >= 3.10.
@dataclasses.dataclass
class TrainingData:
    batch: Optional[MultiAgentBatch] = None
    batches: Optional[List[MultiAgentBatch]] = None
    batch_refs: Optional[List[ray.ObjectRef]] = None

    episodes: Optional[List[EpisodeType]] = None
    episodes_refs: Optional[List[ray.ObjectRef]] = None

    data_iterators: Optional[List[ray.data.iterator.DataIterator]] = None

    def validate(self):
        # Exactly one training data type must be provided.
        if (
            sum(
                td is not None
                for td in [
                    self.batch,
                    self.batches,
                    self.batch_refs,
                    self.episodes,
                    self.episodes_refs,
                    self.data_iterators,
                ]
            )
            != 1
        ):
            raise ValueError("Exactly one training data type must be provided!")

    def shard(
        self,
        num_shards: int,
        len_lookback_buffer: Optional[int] = None,
        **kwargs,
    ):
        # Single batch -> Split into n smaller batches.
        if self.batch is not None:
            return [
                (TrainingData(batch=b), {})
                for b in ShardBatchIterator(self.batch, num_shards=num_shards)
            ]

        # TODO (sven): Do we need a more sohpisticated shard mechanism for this case?
        elif self.batches is not None:
            assert num_shards == len(self.batches)
            return [(TrainingData(batch=b), {}) for b in self.batches]

        # List of batch refs.
        elif self.batch_refs is not None:
            return [
                (TrainingData(batch_refs=b), {})
                for b in ShardObjectRefIterator(self.batch_refs, num_shards=num_shards)
            ]

        # List of episodes -> Split into n equally sized shards (based on the lengths
        # of the episodes).
        elif self.episodes is not None:
            num_total_minibatches = 0
            if "minibatch_size" in kwargs and num_shards > 1:
                num_total_minibatches = self._compute_num_total_minibatches(
                    self.episodes,
                    num_shards,
                    kwargs["minibatch_size"],
                    kwargs.get("num_epochs", 1),
                )
            return [
                (
                    TrainingData(episodes=e),
                    {"num_total_minibatches": num_total_minibatches},
                )
                for e in ShardEpisodesIterator(
                    self.episodes,
                    num_shards=num_shards,
                    len_lookback_buffer=len_lookback_buffer,
                )
            ]
        # List of episodes refs.
        elif self.episodes_refs is not None:
            return [
                (TrainingData(episodes_refs=e), {})
                for e in ShardObjectRefIterator(self.episodes_refs, num_shards)
            ]
        # List of data iterators.
        else:
            assert self.data_iterators and len(self.data_iterators) == num_shards
            return [
                (TrainingData(data_iterators=[di]), {}) for di in self.data_iterators
            ]

    def solve_refs(self):
        # Batch references.
        if self.batch_refs is not None:
            # Solve the ray.ObjRefs.
            batches = tree.flatten(ray.get(self.batch_refs))
            # If only a single batch, set `self.batch`.
            if len(batches) == 1:
                self.batch = batches[0]
            # Otherwise, set `self.batches`.
            else:
                self.batches = batches
            # Empty `self.batch_refs`.
            self.batch_refs = None

        # Episode references.
        elif self.episodes_refs is not None:
            # It's possible that individual refs are invalid due to the EnvRunner
            # that produced the ref has crashed or had its entire node go down.
            # In this case, try each ref individually and collect only valid results.
            try:
                episodes = tree.flatten(ray.get(self.episodes_refs))
            except ray.exceptions.OwnerDiedError:
                episode_refs = self.episodes_refs
                episodes = []
                for ref in episode_refs:
                    try:
                        episodes.extend(ray.get(ref))
                    except ray.exceptions.OwnerDiedError:
                        pass
            self.episodes = episodes
            self.episodes_refs = None

    @staticmethod
    def _compute_num_total_minibatches(
        episodes,
        num_shards,
        minibatch_size,
        num_epochs,
    ):
        # Count total number of timesteps per module ID.
        if isinstance(episodes[0], MultiAgentEpisode):
            per_mod_ts = defaultdict(int)
            for ma_episode in episodes:
                for sa_episode in ma_episode.agent_episodes.values():
                    per_mod_ts[sa_episode.module_id] += len(sa_episode)
            max_ts = max(per_mod_ts.values())
        else:
            max_ts = sum(map(len, episodes))

        return int((num_epochs * max_ts) / (num_shards * minibatch_size))
