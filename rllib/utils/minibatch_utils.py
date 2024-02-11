import math
from typing import List

from ray.rllib.policy.sample_batch import MultiAgentBatch, concat_samples
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import EpisodeType


@DeveloperAPI
class MiniBatchIteratorBase:
    """The base class for all minibatch iterators.

    Args:
        batch: The input multi-agent batch.
        minibatch_size: The size of the minibatch for each module_id.
        num_iters: The number of epochs to cover. If the input batch is smaller than
            minibatch_size, then the iterator will cycle through the batch until it
            has covered num_iters epochs.
    """

    def __init__(
        self, batch: MultiAgentBatch, minibatch_size: int, num_iters: int = 1
    ) -> None:
        pass


@DeveloperAPI
class MiniBatchCyclicIterator(MiniBatchIteratorBase):
    """This implements a simple multi-agent minibatch iterator.


    This iterator will split the input multi-agent batch into minibatches where the
    size of batch for each module_id (aka policy_id) is equal to minibatch_size. If the
    input batch is smaller than minibatch_size, then the iterator will cycle through
    the batch until it has covered num_iters epochs.

    Args:
        batch: The input multi-agent batch.
        minibatch_size: The size of the minibatch for each module_id.
        num_iters: The minimum number of epochs to cover. If the input batch is smaller
            than minibatch_size, then the iterator will cycle through the batch until
            it has covered at least num_iters epochs.
    """

    def __init__(
        self, batch: MultiAgentBatch, minibatch_size: int, num_iters: int = 1
    ) -> None:
        super().__init__(batch, minibatch_size, num_iters)
        self._batch = batch
        self._minibatch_size = minibatch_size
        self._num_iters = num_iters

        # mapping from module_id to the start index of the batch
        self._start = {mid: 0 for mid in batch.policy_batches.keys()}
        # mapping from module_id to the number of epochs covered for each module_id
        self._num_covered_epochs = {mid: 0 for mid in batch.policy_batches.keys()}

    def __iter__(self):
        while min(self._num_covered_epochs.values()) < self._num_iters:

            minibatch = {}
            for module_id, module_batch in self._batch.policy_batches.items():

                if len(module_batch) == 0:
                    raise ValueError(
                        f"The batch for module_id {module_id} is empty! "
                        "This will create an infinite loop because we need to cover "
                        "the same number of samples for each module_id."
                    )
                s = self._start[module_id]  # start
                # TODO (sven): Fix this bug for LSTMs:
                #  In an RNN-setting, the Learner connector already has zero-padded
                #  and added a timerank to the batch. Thus, n_step would still be based
                #  on the BxT dimension, rather than the new B dimension (excluding T),
                #  which then leads to minibatches way too large.
                #  However, changing this already would break APPO/IMPALA w/o LSTMs as
                #  these setups require sequencing, BUT their batches are not yet time-
                #  ranked (this is done only in their loss functions via the
                #  `make_time_major` utility).
                n_steps = self._minibatch_size

                samples_to_concat = []

                # get_len is a function that returns the length of a batch
                # if we are not slicing the batch in the batch dimension B, then
                # the length of the batch is simply the length of the batch
                # o.w the length of the batch is the length list of seq_lens.
                if module_batch._slice_seq_lens_in_B:
                    assert module_batch.get(SampleBatch.SEQ_LENS) is not None, (
                        "MiniBatchCyclicIterator requires SampleBatch.SEQ_LENS"
                        "to be present in the batch for slicing a batch in the batch "
                        "dimension B."
                    )

                    def get_len(b):
                        return len(b[SampleBatch.SEQ_LENS])

                else:

                    def get_len(b):
                        return len(b)

                # Cycle through the batch until we have enough samples.
                while n_steps >= get_len(module_batch) - s:
                    sample = module_batch[s:]
                    samples_to_concat.append(sample)
                    len_sample = get_len(sample)
                    assert len_sample > 0, "Length of a sample must be > 0!"
                    n_steps -= len_sample
                    s = 0
                    self._num_covered_epochs[module_id] += 1

                e = s + n_steps  # end
                if e > s:
                    samples_to_concat.append(module_batch[s:e])

                # concatenate all the samples, we should have minibatch_size of sample
                # after this step
                minibatch[module_id] = concat_samples(samples_to_concat)
                # roll minibatch to zero when we reach the end of the batch
                self._start[module_id] = e

            # Note (Kourosh): env_steps is the total number of env_steps that this
            # multi-agent batch is covering. It should be simply inherited from the
            # original multi-agent batch.
            minibatch = MultiAgentBatch(minibatch, len(self._batch))
            yield minibatch


class MiniBatchDummyIterator(MiniBatchIteratorBase):
    def __init__(self, batch: MultiAgentBatch, minibatch_size: int, num_iters: int = 1):
        super().__init__(batch, minibatch_size, num_iters)
        self._batch = batch

    def __iter__(self):
        yield self._batch


@DeveloperAPI
class ShardBatchIterator:
    """Iterator for sharding batch into num_shards batches.

    Args:
        batch: The input multi-agent batch.
        num_shards: The number of shards to split the batch into.

    Yields:
        A MultiAgentBatch of size len(batch) / num_shards.
    """

    def __init__(self, batch: MultiAgentBatch, num_shards: int):
        self._batch = batch
        self._num_shards = num_shards

    def __iter__(self):
        for i in range(self._num_shards):
            batch_to_send = {}
            for pid, sub_batch in self._batch.policy_batches.items():
                batch_size = math.ceil(len(sub_batch) / self._num_shards)
                start = batch_size * i
                end = min(start + batch_size, len(sub_batch))
                batch_to_send[pid] = sub_batch[int(start) : int(end)]
            # TODO (Avnish): int(batch_size) ? How should we shard MA batches really?
            new_batch = MultiAgentBatch(batch_to_send, int(batch_size))
            yield new_batch


@DeveloperAPI
class ShardEpisodesIterator:
    """Iterator for sharding a list of Episodes into num_shards sub-lists of Episodes.

    Args:
        episodes: The input list of Episodes.
        num_shards: The number of shards to split the episodes into.

    Yields:
        A sub-list of Episodes of size roughly `len(episodes) / num_shards`. The yielded
        sublists might have slightly different total sums of episode lengths, in order
        to not have to drop even a single timestep.
    """

    def __init__(self, episodes: List[EpisodeType], num_shards: int):
        self._episodes = sorted(episodes, key=len, reverse=True)
        self._num_shards = num_shards
        self._total_length = sum(len(e) for e in episodes)
        self._target_lengths = [0 for _ in range(self._num_shards)]
        remaining_length = self._total_length
        for s in range(self._num_shards):
            len_ = remaining_length // (num_shards - s)
            self._target_lengths[s] = len_
            remaining_length -= len_

    def __iter__(self):
        sublists = [[] for _ in range(self._num_shards)]
        lengths = [0 for _ in range(self._num_shards)]
        episode_index = 0

        while episode_index < len(self._episodes):
            episode = self._episodes[episode_index]
            min_index = lengths.index(min(lengths))

            if lengths[min_index] + len(episode) <= self._target_lengths[min_index]:
                # Add the whole episode if it fits within the target length
                sublists[min_index].append(episode)
                lengths[min_index] += len(episode)
                episode_index += 1
            else:
                # Otherwise, slice the episode
                remaining_length = self._target_lengths[min_index] - lengths[min_index]
                if remaining_length > 0:
                    slice_part, remaining_part = (
                        episode[:remaining_length],
                        episode[remaining_length:],
                    )
                    sublists[min_index].append(slice_part)
                    lengths[min_index] += len(slice_part)
                    self._episodes[episode_index] = remaining_part
                else:
                    assert remaining_length == 0
                    sublists[min_index].append(episode)
                    episode_index += 1

        for sublist in sublists:
            yield sublist
