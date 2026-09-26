import math
from typing import List, Optional

from ray.data import DataIterator
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch, concat_samples
from ray.rllib.utils import unflatten_dict
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import DeviceType, EpisodeType


@DeveloperAPI
class MiniBatchIteratorBase:
    """The base class for all minibatch iterators."""

    def __init__(
        self,
        batch: MultiAgentBatch,
        *,
        num_epochs: int = 1,
        shuffle_batch_per_epoch: bool = True,
        minibatch_size: int,
        num_total_minibatches: int = 0,
    ) -> None:
        """Initializes a MiniBatchIteratorBase instance.

        Args:
            batch: The input multi-agent batch.
            num_epochs: The number of complete passes over the entire train batch. Each
                pass might be further split into n minibatches (if `minibatch_size`
                provided). The train batch is generated from the given `episodes`
                through the Learner connector pipeline.
            minibatch_size: The size of minibatches to use to further split the train
                batch into per epoch. The train batch is generated from the given
                `episodes` through the Learner connector pipeline.
            num_total_minibatches: The total number of minibatches to loop through
                (over all `num_epochs` epochs). 0 (the default) means: as many as it
                takes to cover every module's data `num_epochs` times, see
                `MiniBatchCyclicIterator.num_minibatches`. Multi-Learner setups agree
                on one value and pass it explicitly, so that Learners holding
                differently sized shards still step the same number of times.
        """
        pass


@DeveloperAPI
class MiniBatchCyclicIterator(MiniBatchIteratorBase):
    """This implements a simple multi-agent minibatch iterator.

    This iterator will split the input multi-agent batch into minibatches where the
    size of batch for each module_id is equal to minibatch_size,
    cycling through the batch as needed. It yields exactly `num_total_minibatches`
    minibatches; when that is not given, it yields as many as it takes to cover
    every module's data `num_epochs` times.
    """

    def __init__(
        self,
        batch: MultiAgentBatch,
        *,
        num_epochs: int = 1,
        minibatch_size: int,
        shuffle_batch_per_epoch: bool = True,
        num_total_minibatches: int = 0,
    ) -> None:
        """Initializes a MiniBatchCyclicIterator instance."""
        super().__init__(
            batch,
            num_epochs=num_epochs,
            minibatch_size=minibatch_size,
            shuffle_batch_per_epoch=shuffle_batch_per_epoch,
        )

        self._batch = batch
        self._minibatch_size = minibatch_size
        self._shuffle_batch_per_epoch = shuffle_batch_per_epoch

        # mapping from module_id to the start index of the batch
        self._start = {mid: 0 for mid in batch.policy_batches.keys()}

        self._minibatch_count = 0
        self._num_total_minibatches = num_total_minibatches or self.num_minibatches(
            batch, minibatch_size=minibatch_size, num_epochs=num_epochs
        )

    @classmethod
    def num_minibatches(
        cls, batch: MultiAgentBatch, *, minibatch_size: int, num_epochs: int
    ) -> int:
        """Returns how many minibatches cover every module's data `num_epochs` times.

        Each minibatch consumes `minibatch_size` rows per module (for batches sliced
        in the sequence dimension: the corresponding number of sequences), so the
        module with the most data governs: ceil(num_epochs * n / step), maxed over
        the modules. Returns 0 for a batch without any module.
        """
        return max(
            (
                math.ceil(num_epochs * n / step)
                for n, step in (
                    cls._len_and_step(module_batch, minibatch_size)
                    for module_batch in batch.policy_batches.values()
                )
            ),
            default=0,
        )

    @staticmethod
    def _len_and_step(module_batch: SampleBatch, minibatch_size: int):
        """Returns `module_batch`'s length and the amount one minibatch consumes of it.

        Both are in the unit the batch is sliced in: timesteps, or -- for batches
        flagged to be sliced in the sequence dimension B -- sequences, in which case a
        minibatch of `minibatch_size` timesteps corresponds to proportionally many
        sequences.
        """
        if module_batch._slice_seq_lens_in_B:
            assert module_batch.get(SampleBatch.SEQ_LENS) is not None, (
                "MiniBatchCyclicIterator requires SampleBatch.SEQ_LENS"
                "to be present in the batch for slicing a batch in the batch "
                "dimension B."
            )
            n = len(module_batch[SampleBatch.SEQ_LENS])
            step = int(n * (minibatch_size / len(module_batch)))
            if step <= 0:
                raise ValueError(
                    f"`minibatch_size` ({minibatch_size}) is smaller than the average "
                    f"sequence length ({len(module_batch) / n:.1f}), so a minibatch "
                    "would hold less than one sequence. Increase `minibatch_size`."
                )
            return n, step
        return len(module_batch), minibatch_size

    def __iter__(self):
        while self._minibatch_count < self._num_total_minibatches:
            minibatch = {}
            for module_id, module_batch in self._batch.policy_batches.items():

                if len(module_batch) == 0:
                    raise ValueError(
                        f"The batch for module_id {module_id} is empty! Minibatches "
                        "need `minibatch_size` samples from every module_id."
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
                # One rule for how much a minibatch consumes (timesteps, or sequences
                # for batches sliced in the sequence dimension B); `_len_and_step` also
                # checks that SEQ_LENS is present in that case.
                _, n_steps = self._len_and_step(module_batch, self._minibatch_size)

                samples_to_concat = []

                # Length of a (sub-)batch in the unit we slice in.
                if module_batch._slice_seq_lens_in_B:

                    def get_len(b):
                        return len(b[SampleBatch.SEQ_LENS])

                else:

                    def get_len(b):
                        return len(b)

                # Cycle through the batch until we have enough samples.
                while s + n_steps >= get_len(module_batch):
                    sample = module_batch[s:]
                    samples_to_concat.append(sample)
                    len_sample = get_len(sample)
                    assert len_sample > 0, "Length of a sample must be > 0!"
                    n_steps -= len_sample
                    s = 0
                    # Shuffle the individual single-agent batch, if required.
                    # This should happen once per minibatch iteration in order to make
                    # each iteration go through a different set of minibatches.
                    if self._shuffle_batch_per_epoch:
                        module_batch.shuffle()

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

            self._minibatch_count += 1


class MiniBatchDummyIterator(MiniBatchIteratorBase):
    def __init__(self, batch: MultiAgentBatch, **kwargs):
        super().__init__(batch, **kwargs)
        self._batch = batch

    def __iter__(self):
        yield self._batch


@DeveloperAPI
class MiniBatchRayDataIterator:
    def __init__(
        self,
        *,
        iterator: DataIterator,
        device: DeviceType,
        minibatch_size: int,
        num_iters: Optional[int],
        **kwargs,
    ):
        # A `ray.data.DataIterator` that can iterate in different ways over the data.
        self._iterator = iterator
        # Note, in multi-learner settings the `return_state` is in `kwargs`.
        self._kwargs = {k: v for k, v in kwargs.items() if k != "return_state"}

        # Holds a batched_iterable over the dataset.
        self._batched_iterable = self._iterator.iter_torch_batches(
            batch_size=minibatch_size,
            device=device,
            **self._kwargs,
        )
        # Create an iterator that can be stopped and resumed during an epoch.
        self._epoch_iterator = iter(self._batched_iterable)
        self._num_iters = num_iters

    def __iter__(self) -> MultiAgentBatch:
        iteration = 0
        while self._num_iters is None or iteration < self._num_iters:
            for batch in self._epoch_iterator:
                # Update the iteration counter.
                iteration += 1

                batch = unflatten_dict(batch)
                batch = MultiAgentBatch(
                    {
                        module_id: SampleBatch(module_data)
                        for module_id, module_data in batch.items()
                    },
                    env_steps=sum(
                        len(next(iter(module_data.values())))
                        for module_data in batch.values()
                    ),
                )

                yield (batch)

                # If `num_iters` is reached break and return.
                if self._num_iters and iteration == self._num_iters:
                    break
            else:
                # Reinstantiate a new epoch iterator.
                self._epoch_iterator = iter(self._batched_iterable)
                # If a full epoch on the data should be run, stop.
                if not self._num_iters:
                    # Exit the loop.
                    break


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
            # TODO (sven): The following way of sharding a multi-agent batch destroys
            #  the relationship of the different agents' timesteps to each other.
            #  Thus, in case the algorithm requires agent-synchronized data (aka.
            #  "lockstep"), the `ShardBatchIterator` cannot be used.
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
    """Iterator for sharding a list of Episodes into `num_shards` lists of Episodes."""

    def __init__(
        self,
        episodes: List[EpisodeType],
        num_shards: int,
        len_lookback_buffer: Optional[int] = None,
    ):
        """Initializes a ShardEpisodesIterator instance.

        Args:
            episodes: The input list of Episodes.
            num_shards: The number of shards to split the episodes into.
            len_lookback_buffer: An optional length of a lookback buffer to enforce
                on the returned shards. When spitting an episode, the second piece
                might need a lookback buffer (into the first piece) depending on the
                user's settings.
        """
        self._episodes = sorted(episodes, key=len, reverse=True)
        self._num_shards = num_shards
        self._len_lookback_buffer = len_lookback_buffer
        self._total_length = sum(len(e) for e in episodes)
        self._target_lengths = [0 for _ in range(self._num_shards)]
        remaining_length = self._total_length
        for s in range(self._num_shards):
            len_ = remaining_length // (num_shards - s)
            self._target_lengths[s] = len_
            remaining_length -= len_

    def __iter__(self) -> List[EpisodeType]:
        """Runs one iteration through this sharder.

        Yields:
            A sub-list of Episodes of size roughly `len(episodes) / num_shards`. The
            yielded sublists might have slightly different total sums of episode
            lengths, in order to not have to drop even a single timestep.
        """
        sublists = [[] for _ in range(self._num_shards)]
        lengths = [0 for _ in range(self._num_shards)]
        episode_index = 0

        while episode_index < len(self._episodes):
            episode = self._episodes[episode_index]
            min_index = lengths.index(min(lengths))

            # Add the whole episode if it fits within the target length
            if lengths[min_index] + len(episode) <= self._target_lengths[min_index]:
                sublists[min_index].append(episode)
                lengths[min_index] += len(episode)
                episode_index += 1
            # Otherwise, slice the episode
            else:
                remaining_length = self._target_lengths[min_index] - lengths[min_index]
                if remaining_length > 0:
                    slice_part, remaining_part = (
                        # Note that the first slice will automatically "inherit" the
                        # lookback buffer size of the episode.
                        episode[:remaining_length],
                        # However, the second slice might need a user defined lookback
                        # buffer (into the first slice).
                        episode.slice(
                            slice(remaining_length, None),
                            len_lookback_buffer=self._len_lookback_buffer,
                        ),
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


@DeveloperAPI
class ShardObjectRefIterator:
    """Iterator for sharding a list of ray ObjectRefs into num_shards sub-lists.

    Args:
        object_refs: The input list of ray ObjectRefs.
        num_shards: The number of shards to split the references into.

    Yields:
        A sub-list of ray ObjectRefs with lengths as equal as possible.
    """

    def __init__(self, object_refs, num_shards: int):
        self._object_refs = object_refs
        self._num_shards = num_shards

    def __iter__(self):
        # Calculate the size of each sublist
        n = len(self._object_refs)
        sublist_size = n // self._num_shards
        remaining_elements = n % self._num_shards

        start = 0
        for i in range(self._num_shards):
            # Determine the end index for the current sublist
            end = start + sublist_size + (1 if i < remaining_elements else 0)
            # Append the sublist to the result
            yield self._object_refs[start:end]
            # Update the start index for the next sublist
            start = end
