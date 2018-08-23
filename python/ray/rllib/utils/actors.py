from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import ray


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

    def completed(self):
        pending = list(self._tasks)
        if pending:
            ready, _ = ray.wait(pending, num_returns=len(pending), timeout=10)
            for obj_id in ready:
                yield (self._tasks.pop(obj_id), self._objects.pop(obj_id))

    def completed_prefetch(self):
        """Similar to completed but only returns once the object is local.

        Assumes obj_id only is one id."""

        for worker, obj_id in self.completed():
            plasma_id = ray.pyarrow.plasma.ObjectID(obj_id.id())
            if not ray.global_state.use_raylet:
                ray.worker.global_worker.plasma_client.fetch([plasma_id])
            else:
                (ray.worker.global_worker.local_scheduler_client.
                 reconstruct_objects([obj_id], True))
            self._fetching.append((worker, obj_id))

        remaining = []
        for worker, obj_id in self._fetching:
            plasma_id = ray.pyarrow.plasma.ObjectID(obj_id.id())
            if ray.worker.global_worker.plasma_client.contains(plasma_id):
                yield (worker, obj_id)
            else:
                remaining.append((worker, obj_id))
        self._fetching = remaining

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
    local, _ = split_colocated(actors)
    print("Got {} colocated actors of {}".format(len(local), count))
    return local


def create_colocated(cls, args, count):
    ok = []
    i = 1
    while len(ok) < count and i < 10:
        attempt = try_create_colocated(cls, args, count * i)
        ok.extend(attempt)
        i += 1
    if len(ok) < count:
        raise Exception("Unable to create enough colocated actors, abort.")
    return ok[:count]
