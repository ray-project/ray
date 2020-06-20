import logging
import platform
import ray
from collections import deque

logger = logging.getLogger(__name__)


class TaskPool:
    """Helper class for tracking the status of many in-flight actor tasks."""

    def __init__(self):
        self._tasks = {}
        self._objects = {}
        self._fetching = deque()

    def add(self, worker, all_obj_ids):
        if isinstance(all_obj_ids, list):
            obj_id = all_obj_ids[0]
        else:
            obj_id = all_obj_ids
        self._tasks[obj_id] = worker
        self._objects[obj_id] = all_obj_ids

    def completed(self, blocking_wait=False):
        pending = list(self._tasks)
        if pending:
            ready, _ = ray.wait(pending, num_returns=len(pending), timeout=0)
            if not ready and blocking_wait:
                ready, _ = ray.wait(pending, num_returns=1, timeout=10.0)
            for obj_id in ready:
                yield (self._tasks.pop(obj_id), self._objects.pop(obj_id))

    def completed_prefetch(self, blocking_wait=False, max_yield=999):
        """Similar to completed but only returns once the object is local.

        Assumes obj_id only is one id."""

        for worker, obj_id in self.completed(blocking_wait=blocking_wait):
            self._fetching.append((worker, obj_id))

        for _ in range(max_yield):
            if not self._fetching:
                break

            yield self._fetching.popleft()

    def reset_workers(self, workers):
        """Notify that some workers may be removed."""
        for obj_id, ev in self._tasks.copy().items():
            if ev not in workers:
                del self._tasks[obj_id]
                del self._objects[obj_id]

        # We want to keep the same deque reference so that we don't suffer from
        # stale references in generators that are still in flight
        for _ in range(len(self._fetching)):
            ev, obj_id = self._fetching.popleft()
            if ev in workers:
                # Re-queue items that are still valid
                self._fetching.append((ev, obj_id))

    @property
    def count(self):
        return len(self._tasks)


def drop_colocated(actors):
    colocated, non_colocated = split_colocated(actors)
    for a in colocated:
        a.__ray_terminate__.remote()
    return non_colocated


def split_colocated(actors):
    localhost = platform.node()
    hosts = ray.get([a.get_host.remote() for a in actors])
    local = []
    non_local = []
    for host, a in zip(hosts, actors):
        if host == localhost:
            local.append(a)
        else:
            non_local.append(a)
    return local, non_local


def try_create_colocated(cls, args, count):
    actors = [cls.remote(*args) for _ in range(count)]
    local, rest = split_colocated(actors)
    logger.info("Got {} colocated actors of {}".format(len(local), count))
    for a in rest:
        a.__ray_terminate__.remote()
    return local


def create_colocated(cls, args, count):
    logger.info("Trying to create {} colocated actors".format(count))
    ok = []
    i = 1
    while len(ok) < count and i < 10:
        attempt = try_create_colocated(cls, args, count * i)
        ok.extend(attempt)
        i += 1
    if len(ok) < count:
        raise Exception("Unable to create enough colocated actors, abort.")
    for a in ok[count:]:
        a.__ray_terminate__.remote()
    return ok[:count]
