from typing import TypeVar, Iterable, Any, Union, Callable

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.impl.block_list import BlockList
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.remote_fn import cached_remote_fn

T = TypeVar("T")
U = TypeVar("U")

# A class type that implements __call__.
CallableClass = type


class ComputeStrategy:
    def apply(self, fn: Any,
              blocks: Iterable[Block]) -> Iterable[ObjectRef[Block]]:
        raise NotImplementedError


def _map_block(block: Block, meta: BlockMetadata,
               fn: Any) -> (Block, BlockMetadata):
    new_block = fn(block)
    accessor = BlockAccessor.for_block(new_block)
    new_meta = BlockMetadata(
        num_rows=accessor.num_rows(),
        size_bytes=accessor.size_bytes(),
        schema=accessor.schema(),
        input_files=meta.input_files)
    return new_block, new_meta


class TaskPool(ComputeStrategy):
    def apply(self, fn: Any, remote_args: dict,
              blocks: BlockList[Any]) -> BlockList[Any]:
        map_bar = ProgressBar("Map Progress", total=len(blocks))

        kwargs = remote_args.copy()
        kwargs["num_returns"] = 2

        map_block = cached_remote_fn(_map_block)
        refs = [
            map_block.options(**kwargs).remote(b, m, fn)
            for b, m in zip(blocks, blocks.get_metadata())
        ]
        new_blocks, new_metadata = zip(*refs)

        map_bar.block_until_complete(list(new_blocks))
        new_metadata = ray.get(list(new_metadata))
        return BlockList(list(new_blocks), list(new_metadata))


class ActorPool(ComputeStrategy):
    def __init__(self):
        self.workers = []

    def __del__(self):
        for w in self.workers:
            w.__ray_terminate__.remote()

    def apply(self, fn: Any, remote_args: dict,
              blocks: Iterable[Block]) -> Iterable[ObjectRef[Block]]:

        map_bar = ProgressBar("Map Progress", total=len(blocks))

        class BlockWorker:
            def ready(self):
                return "ok"

            @ray.method(num_returns=2)
            def process_block(self, block: Block,
                              meta: BlockMetadata) -> (Block, BlockMetadata):
                new_block = fn(block)
                accessor = BlockAccessor.for_block(new_block)
                new_metadata = BlockMetadata(
                    num_rows=accessor.num_rows(),
                    size_bytes=accessor.size_bytes(),
                    schema=accessor.schema(),
                    input_files=meta.input_files)
                return new_block, new_metadata

        if not remote_args:
            remote_args["num_cpus"] = 1
        BlockWorker = ray.remote(**remote_args)(BlockWorker)

        self.workers = [BlockWorker.remote()]
        metadata_mapping = {}
        tasks = {w.ready.remote(): w for w in self.workers}
        ready_workers = set()
        blocks_in = [(b, m) for (b, m) in zip(blocks, blocks.get_metadata())]
        blocks_out = []

        while len(blocks_out) < len(blocks):
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
                block_ref, meta_ref = worker.process_block.remote(
                    *blocks_in.pop())
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
