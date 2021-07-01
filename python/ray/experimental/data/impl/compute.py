from typing import TypeVar, List, Any

import ray
from ray.experimental.data.impl.block import Block, ObjectRef
from ray.experimental.data.impl.progress_bar import ProgressBar

T = TypeVar("T")
U = TypeVar("U")


class ComputePool:
    def apply(self, fn: Any, blocks: List[Block[T]]) -> List[ObjectRef[Block]]:
        raise NotImplementedError


class TaskPool(ComputePool):
    def apply(self, fn: Any, remote_args: dict,
              blocks: List[Block[T]]) -> List[ObjectRef[Block]]:
        map_bar = ProgressBar("Map Progress", total=len(blocks))

        if remote_args:
            fn = ray.remote(**remote_args)(fn)
        else:
            fn = ray.remote(fn)
        blocks = [fn.remote(b) for b in blocks]

        map_bar.block_until_complete(blocks)
        return blocks


class ActorPool(ComputePool):
    def apply(self, fn: Any, remote_args: dict,
              blocks: List[Block[T]]) -> List[ObjectRef[Block]]:

        map_bar = ProgressBar("Map Progress", total=len(blocks))

        class Worker:
            def ready(self):
                return "ok"

            def process_block(self, block: Block[T]) -> Block[U]:
                return fn(block)

        if "num_cpus" not in remote_args:
            remote_args["num_cpus"] = 1
        Worker = ray.remote(**remote_args)(Worker)

        workers = [Worker.remote()]
        tasks = {w.ready.remote(): w for w in workers}
        ready_workers = set()
        blocks_in = blocks.copy()
        blocks_out = []

        while len(blocks_out) < len(blocks):
            ready, _ = ray.wait(
                list(tasks), timeout=0.01, num_returns=1, fetch_local=False)
            if not ready:
                if len(ready_workers) / len(workers) > 0.8:
                    w = Worker.remote()
                    workers.append(w)
                    tasks[w.ready.remote()] = w
                    map_bar.set_description(
                        "Map Progress ({} actors {} pending)".format(
                            len(ready_workers),
                            len(workers) - len(ready_workers)))
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
                tasks[worker.process_block.remote(blocks_in.pop())] = worker

        map_bar.close()
        return blocks_out


def get_compute(compute_spec: str) -> ComputePool:
    if compute_spec == "tasks":
        return TaskPool()
    elif compute_spec == "actors":
        return ActorPool()
    else:
        raise ValueError("compute must be one of [`tasks`, `actors`]")
