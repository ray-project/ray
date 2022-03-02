import bisect
import logging
import random
from collections import defaultdict
from typing import List, Any, Optional, TYPE_CHECKING

import ray
from ray.types import ObjectRef
from ray.data.block import T, BlockAccessor
from ray.data.impl.remote_fn import cached_remote_fn

if TYPE_CHECKING:
    from ray.data import Dataset

logger = logging.getLogger(__name__)


class RandomAccessMap(object):
    """A class that provides distributed, random access to a Dataset.

    See: ``Dataset.to_random_access_map()``.
    """

    def __init__(
        self,
        dataset: "Dataset[T]",
        key: str,
        num_workers: int,
        threads_per_worker: int,
    ):
        """Construct a RandomAccessMap (internal API).

        The constructor is not part of the Datasets API. Use
        ``dataset.to_random_access_map()`` to construct a RandomAccessMap.
        """
        if dataset._dataset_format() != "arrow":
            raise ValueError(
                "RandomAccessMap can only be constructed from Arrow-format datasets."
            )

        logger.info("[setup] Indexing dataset by sort key.")
        sorted_ds = dataset.sort(key)
        get_bounds = cached_remote_fn(_get_bounds)
        blocks = sorted_ds.get_internal_block_refs()

        logger.info("[setup] Computing block range bounds.")
        bounds = ray.get([get_bounds.remote(b, key) for b in blocks])
        self._valid_blocks = []
        self._lower_bound = None
        self._upper_bounds = []
        for i, b in enumerate(bounds):
            if b:
                self._valid_blocks.append(blocks[i])
                if self._lower_bound is None:
                    self._lower_bound = b[0]
                self._upper_bounds.append(b[1])

        logger.info("[setup] Creating {} random access workers.".format(num_workers))
        self._workers = [
            _RandomAccessWorker.options(
                scheduling_strategy="SPREAD", max_concurrency=threads_per_worker
            ).remote(key)
            for _ in range(num_workers)
        ]
        (
            self._block_to_workers_map,
            self._worker_to_blocks_map,
        ) = self._compute_block_to_worker_assignments()

        logger.info(
            "[setup] Worker to blocks assignment: {}".format(self._worker_to_blocks_map)
        )
        ray.get(
            [
                w.assign_blocks.remote(
                    {i: self._valid_blocks[i] for i in self._worker_to_blocks_map[w]}
                )
                for w in self._workers
            ]
        )

        logger.info("[setup] Finished assigning blocks to workers.")

    def _compute_block_to_worker_assignments(self):
        # Return values.
        blocks_to_workers: dict[int, List["ray.ActorHandle"]] = defaultdict(list)
        workers_to_blocks: dict["ray.ActorHandle", List[int]] = defaultdict(list)

        # Aux data structures.
        loc_to_workers: dict[str, List["ray.ActorHandle"]] = defaultdict(list)
        locs = ray.get([w.ping.remote() for w in self._workers])
        for i, loc in enumerate(locs):
            loc_to_workers[loc].append(self._workers[i])
        block_locs = ray.experimental.get_object_locations(self._valid_blocks)

        # First, try to assign all blocks to all workers at its location.
        for block_idx, block in enumerate(self._valid_blocks):
            block_info = block_locs[block]
            locs = block_info.get("node_ids", [])
            for loc in locs:
                for worker in loc_to_workers[loc]:
                    blocks_to_workers[block_idx].append(worker)
                    workers_to_blocks[worker].append(block_idx)

        # Randomly assign any leftover blocks to at least one worker.
        for block_idx, block in enumerate(self._valid_blocks):
            if len(blocks_to_workers[block_idx]) == 0:
                worker = random.choice(self._workers)
                blocks_to_workers[block_idx].append(worker)
                workers_to_blocks[worker].append(block_idx)

        return blocks_to_workers, workers_to_blocks

    def get_async(self, key: Any) -> ObjectRef[Optional[T]]:
        """Asynchronously finds the record for a single key.

        Args:
            key: The key of the record to find.

        Returns:
            ObjectRef containing the record (in pydict form), or None if not found.
        """
        block_index = self._find_le(key)
        if block_index is None:
            return ray.put(None)
        return self._worker_for(block_index).get.remote(block_index, key)

    def multiget(self, keys: List[Any]) -> List[Optional[T]]:
        """Synchronously find the records for a list of keys.

        Args:
            keys: List of keys to find the records for.

        Returns:
            List of found records (in pydict form), or None for missing records.
        """
        batches = defaultdict(list)
        for k in keys:
            batches[self._find_le(k)].append(k)
        futures = {}
        for index, keybatch in batches.items():
            if index is None:
                continue
            fut = self._worker_for(index).multiget.remote(
                [index] * len(keybatch), keybatch
            )
            futures[index] = fut
        results = {}
        for i, fut in futures.items():
            keybatch = batches[i]
            values = ray.get(fut)
            for k, v in zip(keybatch, values):
                results[k] = v
        return [results.get(k) for k in keys]

    def _worker_for(self, block_index: int):
        return random.choice(self._block_to_workers_map[block_index])

    def _find_le(self, x: Any) -> int:
        i = bisect.bisect_right(self._upper_bounds, x)
        if i >= len(self._upper_bounds) or x < self._lower_bound:
            return None
        return i


@ray.remote(num_cpus=0, placement_group=None)
class _RandomAccessWorker:
    def __init__(self, key_field):
        self.blocks = None
        self.key_field = key_field

    def assign_blocks(self, block_ref_dict):
        self.blocks = {k: ray.get(ref) for k, ref in block_ref_dict.items()}

    def get(self, block_index, key):
        if block_index is None:
            return None
        block = self.blocks[block_index]
        i = _binary_search_find(block[self.key_field], key)
        if i is None:
            return None
        else:
            acc = BlockAccessor.for_block(block)
            return acc._create_table_row(acc.slice(i, i + 1, copy=True))

    def multiget(self, block_indices, keys):
        return [self.get(i, k) for i, k in zip(block_indices, keys)]

    def ping(self):
        return ray.get_runtime_context().node_id.hex()


def _binary_search_find(column, x):
    column = _ArrowListWrapper(column)
    i = bisect.bisect_left(column, x)
    if i != len(column) and column[i] == x:
        return i
    return None


class _ArrowListWrapper:
    def __init__(self, arrow_col):
        self.arrow_col = arrow_col

    def __getitem__(self, i):
        return self.arrow_col[i].as_py()

    def __len__(self):
        return len(self.arrow_col)


def _get_bounds(block, key):
    if len(block) == 0:
        return None
    return (block[key][0].as_py(), block[key][-1].as_py())


if __name__ == "__main__":
    import time

    ds = ray.data.range_arrow(100000000, parallelism=10)
    rmap = ds.to_random_access_map("value", num_workers=1, threads_per_worker=4)

    print("Demo:")
    print(ray.get(rmap.get_async(1)))
    print(ray.get(rmap.get_async(-1)))
    print(rmap.multiget([1, 2]))
    print(rmap.multiget([1, 2, 200000, -1]))

    print("Multiget throughput: ", end="")
    start = time.time()
    total = 0
    while time.time() - start < 3:
        rmap.multiget([90000] * 1000)
        total += 1000
    print(total / (time.time() - start), "keys / second / worker")

    print("Single get throughput: ", end="")
    start = time.time()
    total = 0
    while time.time() - start < 3:
        ray.get([rmap.get_async(90000) for _ in range(1000)])
        total += 1000
    print(total / (time.time() - start), "keys / second / worker")
