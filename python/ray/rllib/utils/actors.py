from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import ray


class TaskPool(object):
    def __init__(self):
        self._tasks = {}

    def add(self, worker, obj_id):
        self._tasks[obj_id] = worker

    def completed(self):
        pending = list(self._tasks)
        if pending:
            ready, _ = ray.wait(pending, num_returns=len(pending), timeout=10)
            for obj_id in ready:
                yield (self._tasks.pop(obj_id), obj_id)

    @property
    def count(self):
        return len(self._tasks)


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
