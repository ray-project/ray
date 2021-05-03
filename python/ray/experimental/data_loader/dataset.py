import functools
import itertools
from typing import TypeVar, List, Tuple, Iterable, Iterator

import pandas as pd
import ray
from ray._private.utils import get_num_cpus
from ray.experimental.data_loader.shuffle import shuffle_from_memory
from ray.experimental.data_loader.multiqueue import MultiQueue

MULTIQUEUE_ACTOR_NAME = "MultiQueue"


class ShufflingDataset:
    """
    A shuffling dataset that yields batches upon iteration.

    This dataset will kick off shuffling for max_concurrent_epochs epochs at
    construction time in the master process (rank 0).

    Args:
        filenames (str): Paths to input Parquet files.
        num_epochs (int): Number of training epochs.
        num_trainers (int): Number of trainer workers.
        batch_size (int): Size of the batches that the iterator should yield.
        rank (int): The worker rank of the current process.
        num_reducers (optional, int): The number of shuffler reducers. Default
            is the number of trainers x the number of cores on the master
            (rank 0) worker.
        max_concurrent_epochs (optional, int): The maximum number of epochs
            whose shuffling stages should execute concurrently. Default is 2.
    """

    def __init__(self,
                 filenames: List[str],
                 num_epochs: int,
                 num_trainers: int,
                 batch_size: int,
                 rank: int,
                 num_reducers: int = None,
                 max_concurrent_epochs: int = 2,
                 max_batch_queue_size: int = 100):
        if num_reducers is None:
            num_reducers = num_trainers * get_num_cpus()

        self._batch_size = batch_size

        if rank == 0:
            # rank == 0 --> master process
            # Create the batch queue. Trainers will consume GPU batches
            # through this batch queue.
            # TODO(Clark): If the shuffle API was changed to take num_trainers
            # batch consumers instead of a single batch consumer, instead of
            # having a single batch multi-queue, we could create num_trainers
            # batch queues, have each batch consumer close over their
            # corresponding batch queue, and have each trainer reference their
            # single batch queue.
            self._batch_queue = MultiQueue(
                num_epochs * num_trainers,
                max_batch_queue_size,
                name=MULTIQUEUE_ACTOR_NAME,
                connect=False)
            # Kick off shuffle.
            # TODO(Clark): Move the shuffle kickoff to an init() method so the
            # user can better control when the shuffling starts?
            self._shuffle_result = ray.remote(shuffle_from_memory).remote(
                filenames,
                functools.partial(batch_consumer, self._batch_queue,
                                  batch_size, num_trainers),
                num_epochs,
                num_reducers,
                num_trainers,
                max_concurrent_epochs,
                collect_stats=False)
        else:
            # rank != 0 --> worker process
            # Connect to the batch queue.
            self._batch_queue = MultiQueue(
                num_epochs * num_trainers,
                max_batch_queue_size,
                name=MULTIQUEUE_ACTOR_NAME,
                connect=True)
            self._shuffle_result = None

        self._num_epochs = num_epochs
        self._num_trainers = num_trainers
        self._rank = rank
        self._epoch = None

    def set_epoch(self, epoch):
        """
        Set the current training epoch. This should be called before
        constructing the iterator on this dataset (e.g. before the
        enumerate(train_loader) call).

        Args:
            epoch (int) The epoch number for the training epoch that is about
                to start.
        """
        self._epoch = epoch

    def __iter__(self):
        """
        This iterator yields GPU batches from the shuffling queue.
        """
        if self._epoch is None:
            raise ValueError(
                "You must set the epoch on this dataset via set_epoch()"
                "before constructing the iterator.")

        leftover_batches = None
        while True:
            # TODO(Clark): Add get_up_to queue method that can fetch up to
            # some number of batches in a single RPC.
            queue_idx = self._epoch * self._num_trainers + self._rank
            batches = self._batch_queue.get(queue_idx, block=True)
            if batches is None:
                break
            batches = ray.get(batches)
            # Handle leftover batches.
            leftover = len(batches) % self._batch_size
            if leftover != 0:
                leftover_batch = batches.tail(leftover)
                # Slice off the leftover.
                batches = batches.head(-leftover)
                leftover_batches = pd.concat(
                    [leftover_batches, leftover_batch])
                # If leftover_batches contains a full batch, yield it.
                if len(leftover_batches) > self._batch_size:
                    batch = leftover_batches.head(self._batch_size)
                    # Slice off the to-be-yielded batch.
                    leftover_batches = leftover_batches.tail(-self._batch_size)
                    yield batch
            # At this point, we're guaranteed to have that
            # len(batches) % self._batch_size == 0
            for batch in chunk_df(batches, self._batch_size):
                yield batch
        # Consume leftover batch.
        if leftover_batches is not None:
            yield leftover_batches
        if (self._epoch == self._num_epochs - 1
                and self._shuffle_result is not None):
            ray.get(self._shuffle_result)


def batch_consumer(queue: MultiQueue, batch_size: int, num_trainers: int,
                   rank: int, epoch: int, batches: Iterable[ray.ObjectRef]):
    """
    Batch consumer that will be provided to the shuffler.
    """
    queue_idx = epoch * num_trainers + rank
    print(
        f"Sending batch to queue for epoch {epoch} and rank {rank} at index: "
        f"{queue_idx}")
    if batches is None:
        queue.put(queue_idx, None)
    else:
        queue.put_batch(queue_idx, batches)


def debug_batch_consumer(rank: int, epoch: int,
                         batches: Iterable[pd.DataFrame]):
    num_batches = len(batches) if batches is not None else 0
    print(f"Received {num_batches} batches in consumer {rank}.")


T = TypeVar("T")


def chunk_df(df: pd.DataFrame, n: int) -> Tuple[pd.DataFrame, ...]:
    for pos in range(0, len(df), n):
        yield df[pos:pos + n]


def chunk(iterable: Iterable[T], n: int) -> Iterator[Tuple[T, ...]]:
    it = iter(iterable)
    return iter(lambda: tuple(itertools.islice(it, n)), ())


if __name__ == "__main__":
    from ray.experimental.data_loader.data_generation import generate_data
    from ray.experimental.data_loader.stats import human_readable_size
    print("Starting Ray...")
    # FIXME(Clark): We're setting the idle worker killing time very high in
    # order to work around an idle working killing bug that's causing objects
    # to be lost. We should fix this.
    ray.init(_system_config={"idle_worker_killing_time_threshold_ms": 10**6})
    num_rows = 10**6
    num_files = 10
    num_row_groups_per_file = 1
    max_row_group_skew = 0.0
    data_dir = "data"
    print(f"Generating {num_rows} rows over {num_files} files, with "
          f"{num_row_groups_per_file} row groups per file and at most "
          f"{100 * max_row_group_skew:.1f}% row group skew.")
    filenames, num_bytes = generate_data(num_rows, num_files,
                                         num_row_groups_per_file,
                                         max_row_group_skew, data_dir)
    print(f"Generated {len(filenames)} files containing {num_rows} rows "
          f"with {num_row_groups_per_file} row groups per file, totalling "
          f"{human_readable_size(num_bytes)}.")
    num_epochs = 4
    num_trainers = 1
    batch_size = 20000
    rank = 0
    num_reducers = 8
    print(f"Creating shuffling dataset with {batch_size} batch size, "
          f"{num_epochs} epochs, {num_reducers} reducers, and {num_trainers} "
          "trainers.")
    print(f"Should consume {num_rows // batch_size} batches.")
    ds = ShufflingDataset(
        filenames,
        num_epochs,
        num_trainers,
        batch_size,
        rank,
        num_reducers=num_reducers)

    for epoch in range(num_epochs):
        ds.set_epoch(epoch)
        for batch_idx, batch in enumerate(ds):
            print(f"Consuming batch {batch_idx}!")
    print("Done consuming batches.")
