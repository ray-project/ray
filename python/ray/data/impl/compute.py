from typing import TypeVar, Any, Union, Callable, List

import ray
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.impl.block_list import BlockList
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.remote_fn import cached_remote_fn

T = TypeVar("T")
U = TypeVar("U")

# A class type that implements __call__.
CallableClass = type


class ComputeStrategy:
    def apply(self, fn: Any, blocks: BlockList) -> BlockList:
        raise NotImplementedError


def _map_block(block: Block, fn: Any,
               input_files: List[str]) -> (Block, BlockMetadata):
    new_block = fn(block)
    accessor = BlockAccessor.for_block(new_block)
    new_meta = BlockMetadata(
        num_rows=accessor.num_rows(),
        size_bytes=accessor.size_bytes(),
        schema=accessor.schema(),
        input_files=input_files)
    return new_block, new_meta


class TaskPool(ComputeStrategy):
    def apply(self, fn: Any, remote_args: dict,
              blocks: BlockList) -> BlockList:
        # Handle empty datasets.
        if blocks.initial_num_blocks() == 0:
            return blocks

        blocks = list(blocks.iter_blocks_with_metadata())
        map_bar = ProgressBar("Map Progress", total=len(blocks))

        kwargs = remote_args.copy()
        kwargs["num_returns"] = 2

        map_block = cached_remote_fn(_map_block)
        refs = [
            map_block.options(**kwargs).remote(b, fn, m.input_files)
            for b, m in blocks
        ]
        new_blocks, new_metadata = zip(*refs)

        new_metadata = list(new_metadata)
        try:
            new_metadata = map_bar.fetch_until_complete(new_metadata)
        except (ray.exceptions.RayTaskError, KeyboardInterrupt) as e:
            # One or more mapper tasks failed, or we received a SIGINT signal
            # while waiting; either way, we cancel all map tasks.
            for ref in new_metadata:
                ray.cancel(ref)
            # Wait until all tasks have failed or been cancelled.
            for ref in new_metadata:
                try:
                    ray.get(ref)
                except (ray.exceptions.RayTaskError,
                        ray.exceptions.TaskCancelledError):
                    pass
            # Reraise the original task failure exception.
            raise e from None
        return BlockList(list(new_blocks), list(new_metadata))


class ActorPool(ComputeStrategy):
    def __init__(self):
        self.workers = []

    def __del__(self):
        for w in self.workers:
            w.__ray_terminate__.remote()

    def apply(self, fn: Any, remote_args: dict,
              blocks: BlockList) -> BlockList:

        blocks_in = list(blocks.iter_blocks_with_metadata())
        orig_num_blocks = len(blocks_in)
        blocks_out = []
        map_bar = ProgressBar("Map Progress", total=orig_num_blocks)

        class BlockWorker:
            def ready(self):
                return "ok"

            @ray.method(num_returns=2)
            def process_block(self, block: Block, input_files: List[str]
                              ) -> (Block, BlockMetadata):
                new_block = fn(block)
                accessor = BlockAccessor.for_block(new_block)
                new_metadata = BlockMetadata(
                    num_rows=accessor.num_rows(),
                    size_bytes=accessor.size_bytes(),
                    schema=accessor.schema(),
                    input_files=input_files)
                return new_block, new_metadata

        if not remote_args:
            remote_args["num_cpus"] = 1

        BlockWorker = ray.remote(**remote_args)(BlockWorker)

        self.workers = [BlockWorker.remote()]
        metadata_mapping = {}
        tasks = {w.ready.remote(): w for w in self.workers}
        ready_workers = set()

        while len(blocks_out) < orig_num_blocks:
            ready, _ = ray.wait(
                list(tasks), timeout=0.01, num_returns=1, fetch_local=False)
            if not ready:
                if len(ready_workers) / len(self.workers) > 0.8:
                    w = BlockWorker.remote()
                    self.workers.append(w)
                    tasks[w.ready.remote()] = w
                    map_bar.set_description(
                        "Map Progress ({} actors {} pending)".format(
                            len(ready_workers),
                            len(self.workers) - len(ready_workers)))
                continue

            [obj_id] = ready
            worker = tasks[obj_id]
            del tasks[obj_id]

            # Process task result.
            if worker in ready_workers:
                blocks_out.append(obj_id)
                map_bar.update(1)
            else:
                ready_workers.add(worker)

            # Schedule a new task.
            if blocks_in:
                block, meta = blocks_in.pop()
                block_ref, meta_ref = worker.process_block.remote(
                    block, meta.input_files)
                metadata_mapping[block_ref] = meta_ref
                tasks[block_ref] = worker

        new_metadata = ray.get([metadata_mapping[b] for b in blocks_out])
        map_bar.close()
        return BlockList(blocks_out, new_metadata)


def cache_wrapper(fn: Union[CallableClass, Callable[[Any], Any]]
                  ) -> Callable[[Any], Any]:
    """Implements caching of stateful callables.

    Args:
        fn: Either a plain function or class of a stateful callable.

    Returns:
        A plain function with per-process initialization cached as needed.
    """
    if isinstance(fn, CallableClass):

        def _fn(item: Any) -> Any:
            if ray.data._cached_fn is None or ray.data._cached_cls != fn:
                ray.data._cached_cls = fn
                ray.data._cached_fn = fn()
            return ray.data._cached_fn(item)

        return _fn
    else:
        return fn


def get_compute(compute_spec: Union[str, ComputeStrategy]) -> ComputeStrategy:
    if not compute_spec or compute_spec == "tasks":
        return TaskPool()
    elif compute_spec == "actors":
        return ActorPool()
    elif isinstance(compute_spec, ComputeStrategy):
        return compute_spec
    else:
        raise ValueError("compute must be one of [`tasks`, `actors`]")
