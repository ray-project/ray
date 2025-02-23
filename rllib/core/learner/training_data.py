import dataclasses
from typing import List, Optional

import tree  # pip install dm_tree

import ray
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.minibatch_utils import (
    ShardBatchIterator,
    ShardEpisodesIterator,
    ShardObjectRefIterator,
)
from ray.rllib.utils.typing import EpisodeType


@dataclasses.dataclass
class TrainingData:
    batch: Optional[MultiAgentBatch] = None
    batches: Optional[List[MultiAgentBatch]] = None
    batch_refs: Optional[List[ray.ObjectRef]] = None

    episodes: Optional[List[EpisodeType]] = None
    episodes_refs: Optional[List[ray.ObjectRef]] = None

    data_iterator: Optional[ray.data.iterator.DataIterator] = None

    def validate(self):
        # Exactly one training data type must be provided.
        if sum(
            td is not None
            for td in [
                self.batch,
                self.batches,
                self.batch_refs,
                self.episodes,
                self.episodes_refs,
                self.data_iterator,
            ]
        ) != 1:
            raise ValueError("Exactly one training data type must be provided!")

    def shard(self, num_shards: int):
        # Single batch -> Split into n smaller batches.
        if self.batch is not None:
            return [TrainingData(batch=b) for b in ShardBatchIterator()]
        # TODO (sven): Do we need a more sohpisticated shard mechanism for this case?
        # List of batches -> Return as iterator.
        elif self.batches is not None:
            assert num_shards == len(self.batches)
            return iter(self.batches)
        # List of batch refs.
        elif self.batch_refs is not None:
            return [
                TrainingData(batch_refs=b)
                for b in ShardObjectRefIterator(self.batch_refs, num_shards)
            ]
        # List of episodes -> Split into n equally sized shards (based on the lengths
        # of the episodes).
        elif self.episodes is not None:
            return [
                TrainingData(episodes=e)
                for e in ShardEpisodesIterator(self.episodes, num_shards)
            ]
        # List of episodes refs.
        elif self.episodes_refs is not None:
            return [
                TrainingData(episodes_refs=e)
                for e in ShardObjectRefIterator(self.episodes_refs, num_shards)
            ]
        else:
            raise NotImplementedError()

    def solve_refs(self):
        if self.batch_refs is not None:
            batches = tree.flatten(ray.get(self.batch_refs))
            if len(batches) == 1:
                self.batch = batches[0]
            else:
                self.batches = batches
            self.batch_refs = None
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
