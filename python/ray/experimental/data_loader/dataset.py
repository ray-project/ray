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
        num_rounds (optional, int): The number of shuffle rounds that the
            shuffler should perform. Default is 2.
        num_mappers (optional, int): The number of shuffler mappers. Default is
            the number of input files.
        num_reducers (optional, int): The number of shuffler reducers. Default
            is the number of trainers x the number of cores on the master
            (rank 0) worker.
        max_concurrent_rounds (optional, int): The maximum number of shuffle
            rounds that should be executed concurrently. Default is the number
            of rounds (no round throttling).
        max_concurrent_epochs (optional, int): The maximum number of epochs
            whose shuffling stages should execute concurrently. Default is 2.
    """
    def __init__(
            self,
            filenames: List[str],
            num_epochs: int,
            num_trainers: int,
            batch_size: int,
            rank: int,
            num_rounds: int = 2,
            num_mappers: int = None,
            num_reducers: int = None,
            max_concurrent_rounds: int = None,
            max_concurrent_epochs: int = 2,
            max_batch_queue_size: int = 100):
        if num_mappers is None:
            num_mappers = len(filenames)
        if num_reducers is None:
            num_reducers = num_trainers * get_num_cpus()
        if max_concurrent_rounds is None:
            max_concurrent_rounds = num_rounds

        self._batch_size = batch_size

        # TODO(Clark): Find way to do this without batch consumer proxy queue.
        if rank == 0:
            # rank == 0 --> master process
            # Create the batch queue. Trainers will consume GPU batches
            # through this batch queue.
            # TODO(Clark): If the shuffle API was changed to take num_trainers
            # batch consumers instead of a single batch consumer, instead of
            # having a single batch multi-queue, we could create num_trainers
            # batch queues, have each batch consumer close over heir
            # corresponding batch queue, and have each trainer reference their
            # single batch queue.
            self._batch_queue = MultiQueue(
                num_trainers, max_batch_queue_size,
                name=MULTIQUEUE_ACTOR_NAME, connect=False)
            # Kick off shuffle.
            # TODO(Clark): Move the shuffle kickoff to an init() method so the
            # user can better control when the shuffling starts?
            self._shuffle_result = shuffle_from_memory.remote(
                filenames,
                functools.partial(
                    batch_consumer, self._batch_queue, batch_size),
                num_epochs,
                num_rounds,
                num_mappers,
                num_reducers,
                num_trainers,
                max_concurrent_rounds,
                max_concurrent_epochs,
                collect_stats=False)
        else:
            # rank != 0 --> worker process
            # Connect to the batch queue.
            self._batch_queue = MultiQueue(
                num_trainers, max_batch_queue_size,
                name=MULTIQUEUE_ACTOR_NAME, connect=True)
            self._shuffle_result = None

        self._rank = rank

    def __iter__(self):
        """
        This iterator yields GPU batches from the shuffling queue.
        """
        while True:
            # TODO(Clark): Add get_up_to queue method that can fetch up to
            # some number of batches in a single RPC.
            batches = self._batch_queue.get(self._rank, block=True)
            if batches is None:
                break
            batches = ray.get(batches)
            for batch in chunk_df(batches, self._batch_size):
                yield batch
        if self._shuffle_result is not None:
            ray.get(self._shuffle_result)


def batch_consumer(
        queue: MultiQueue,
        batch_size: int,
        rank: int,
        batches: Iterable[ray.ObjectRef]):
    """
    Batch consumer that will be provided to the shuffler.
    """
    if batches is None:
        queue.put(rank, None)
    else:
        queue.put_batch(rank, batches)


def debug_batch_consumer(
        rank: int,
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
    num_rows = 10 ** 6
    num_files = 10
    num_row_groups_per_file = 1
    max_row_group_skew = 0.0
    data_dir = "data"
    print(
        f"Generating {num_rows} rows over {num_files} files, with "
        f"{num_row_groups_per_file} row groups per file and at most "
        f"{100 * max_row_group_skew:.1f}% row group skew.")
    filenames, num_bytes = generate_data(
        num_rows, num_files, num_row_groups_per_file, max_row_group_skew,
        data_dir)
    print(
        f"Generated {len(filenames)} files containing {num_rows} rows "
        f"with {num_row_groups_per_file} row groups per file, totalling "
        f"{human_readable_size(num_bytes)}.")
    num_epochs = 4
    num_trainers = 1
    batch_size = 20000
    rank = 0
    num_mappers = 4
    num_reducers = 4
    print(f"Creating shuffling dataset with {batch_size} batch size, "
          f"{num_epochs} epochs, {num_mappers} mappers, {num_reducers} "
          f"reducers, and {num_trainers} trainers.")
    print(f"Should consume {num_rows // batch_size} batches.")
    ds = ShufflingDataset(
        filenames,
        num_epochs,
        num_trainers,
        batch_size,
        rank,
        num_mappers=num_mappers,
        num_reducers=num_reducers)

    for batch_idx, batch in enumerate(ds):
        print(f"Consuming batch {batch_idx}!")
    print("Done consuming batches.")
