from ray.rllib.policy.sample_batch import MultiAgentBatch, concat_samples
from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
class MiniBatchCyclicIterator:
    """This implements a simple multi-agent minibatch iterator.


    This iterator will split the input multi-agent batch into minibatches where the
    size of batch for each module_id (aka policy_id) is equal to minibatch_size. If the
    input batch is smaller than minibatch_size, then the iterator will cycle through
    the batch until it has covered num_iters epochs.

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
                s = self._start[module_id]  # start
                e = s + self._minibatch_size  # end

                samples_to_concat = []
                # cycle through the batch until we have enough samples
                while e >= len(module_batch):
                    samples_to_concat.append(module_batch[s:])
                    e = self._minibatch_size - len(module_batch[s:])
                    s = 0
                    self._num_covered_epochs[module_id] += 1

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
