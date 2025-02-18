import dataclasses

import tree  # pip install dm_tree

import ray
from ray.rllib.utils.minibatch_utils import (
    ShardBatchIterator,
    ShardEpisodesIterator,
    ShardObjectRefIterator,
)


@dataclasses.dataclass
class TrainingData:
    batch = None
    batches = None
    batch_refs = None

    episodes = None
    episodes_refs = None

    data_iterator = None

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
            return ShardBatchIterator()
        # TODO (sven): Do we need a more sohpisticated shard mechanism for this case?
        # List of batches -> Return as iterator.
        elif self.batches is not None:
            assert num_shards == len(self.batches)
            return iter(self.batches)
        # List of batch refs.
        elif self.batch_refs is not None:
            return ShardObjectRefIterator(self.batch_refs, num_shards)
        # List of episodes -> Split into n equally sized shards (based on the lengths
        # of the episodes).
        elif self.episodes is not None:
            return ShardEpisodesIterator(self.episodes, num_shards)
        # List of episodes refs.
        elif self.episodes_refs is not None:
            return ShardObjectRefIterator(self.episodes_refs, num_shards)
        else:
            raise NotImplementedError()

    def solve_refs(self):
        if self.batch_refs is not None:
            self.batches = tree.flatten(ray.get(self.batch_refs))
            self.batch_refs = None
        elif self.episodes_refs is not None:
            self.episodes = tree.flatten(ray.get(self.episodes_refs))
            self.episodes_refs = None
