from typing import TypeVar, List, Any

import ray
from ray.experimental.data.impl.block import Block, BlockRef

T = TypeVar("T")
U = TypeVar("U")


class ComputePool:
    def apply(self, fn: Any, blocks: List[Block[T]]) -> List[BlockRef]:
        raise NotImplementedError


class TaskPool(ComputePool):
    def apply(self, fn: Any, remote_args: dict,
              blocks: List[Block[T]]) -> List[BlockRef]:
        if remote_args:
            fn = ray.remote(**remote_args)(fn)
        else:
            fn = ray.remote(fn)
        return [fn.remote(b) for b in blocks]


class ActorPool(ComputePool):
    def apply(self, fn: Any, remote_args: dict,
              blocks: List[Block[T]]) -> List[BlockRef]:
        class Worker:
            def ready(self):
                print("Worker created")
                return "ok"

            def process_block(self, block: Block[T]) -> Block[U]:
                return fn(block)

        if remote_args:
            Worker = ray.remote(**remote_args)(Worker)
        else:
            Worker = ray.remote(Worker)

        workers = [Worker.remote()]
        tasks = {w.ready.remote(): w for w in workers}
        ready_workers = set()
        blocks_in = [b for b in blocks]
        blocks_out = []

        while len(blocks_out) < len(blocks):
            ready, _ = ray.wait(list(tasks), timeout=0.01, num_returns=1)
            if not ready:
                if len(ready_workers) / len(workers) > 0.75:
                    w = Worker.remote()
                    workers.append(w)
                    tasks[w.ready.remote()] = w
                    print("Creating new worker, {} pending, {} total".format(
                        len(workers) - len(ready_workers), len(workers)))
                continue

            [obj_id] = ready
            worker = tasks[obj_id]
            del tasks[obj_id]

            # Process task result.
            if worker in ready_workers:
                blocks_out.append(obj_id)
            else:
                ready_workers.add(worker)

            # Schedule a new task.
            if blocks_in:
                tasks[worker.process_block.remote(blocks_in.pop())] = worker

        return blocks_out


def get_compute(compute_spec: str) -> ComputePool:
    if compute_spec == "tasks":
        return TaskPool()
    elif compute_spec == "actors":
        return ActorPool()
    else:
        raise ValueError("compute must be one of [`tasks`, `actors`]")
