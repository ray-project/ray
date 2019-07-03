from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import ray

logger = logging.getLogger(__name__)


class TaskPool(object):
    """Helper class for tracking the status of many in-flight actor tasks."""

    def __init__(self):
        self._tasks = {}
        self._objects = {}
        self._fetching = []

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
            plasma_id = ray.pyarrow.plasma.ObjectID(obj_id.binary())
            (ray.worker.global_worker.raylet_client.fetch_or_reconstruct(
                [obj_id], True))
            self._fetching.append((worker, obj_id))

        remaining = []
        num_yielded = 0
        for worker, obj_id in self._fetching:
            plasma_id = ray.pyarrow.plasma.ObjectID(obj_id.binary())
            if (num_yielded < max_yield
                    and ray.worker.global_worker.plasma_client.contains(
                        plasma_id)):
                yield (worker, obj_id)
                num_yielded += 1
            else:
                remaining.append((worker, obj_id))
        self._fetching = remaining

    def reset_workers(self, workers):
        """Notify that some workers may be removed."""
        for obj_id, ev in self._tasks.copy().items():
            if ev not in workers:
                del self._tasks[obj_id]
                del self._objects[obj_id]
        ok = []
        for ev, obj_id in self._fetching:
            if ev in workers:
                ok.append((ev, obj_id))
        self._fetching = ok

    @property
    def count(self):
        return len(self._tasks)


def drop_colocated(actors):
    colocated, non_colocated = split_colocated(actors)
    for a in colocated:
        a.__ray_terminate__.remote()
    return non_colocated


def split_colocated(actors):
    localhost = os.uname()[1]
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
