import math

from ray.rllib.policy.sample_batch import MultiAgentBatch, concat_samples
from ray.rllib.utils.annotations import DeveloperAPI


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
                n_steps = self._minibatch_size

                samples_to_concat = []
                # cycle through the batch until we have enough samples
                while n_steps >= len(module_batch) - s:
                    sample = module_batch[s:]
                    samples_to_concat.append(sample)
                    n_steps -= len(sample)
                    s = 0
                    self._num_covered_epochs[module_id] += 1

                e = s + n_steps  # end
                if e > s:
                    samples_to_concat.append(module_batch[s:e])

                # concatenate all the samples, we should have minibatch_size of sample
                # after this step
                minibatch[module_id] = concat_samples(samples_to_concat)
                # roll miniback to zero when we reach the end of the batch
                self._start[module_id] = e

            # TODO (Kourosh): len(batch) is not correct here. However it's also not
            # clear what the correct value should be. Since training does not depend on
            # this it will be fine for now.
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
