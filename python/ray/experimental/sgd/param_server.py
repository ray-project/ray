from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import numpy as np

import ray
from ray.experimental.sgd.util import Timeline, fetch, warmup

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0)
class ParameterServer(object):
    """Helper class for ray.experimental.sgd.DistributedSGD."""

    def __init__(self, num_workers, tid):
        self.num_sgd_workers = num_workers
        self.acc_counter = 0
        self.timeline = Timeline(tid)
        # TODO(ekl) get this to work again so we get ray events
        # self.timeline.patch_ray()

    def initialize(self, shard_shape):
        """Resets the gradient buffer to zeros."""
        self.accumulated = np.zeros(shard_shape, dtype=np.float32)

    def prefetch(self, oids):
        """Tell plasma to prefetch the given object ids over the network."""
        self.timeline.reset()
        self.timeline.start("prefetch")
        fetch(oids)
        self.timeline.end("prefetch")

    def add_spinwait(self, grad_shard_ids):
        """Optimized version of add() that operates on multiple grads."""
        self.timeline.start("add_spinwait")
        plasma_ids = [ray.pyarrow.plasma.ObjectID(x) for x in grad_shard_ids]
        while plasma_ids:
            for p in plasma_ids:
                if ray.worker.global_worker.plasma_client.contains(p):
                    self.timeline.start("get_buffers")
                    grads = ray.worker.global_worker.plasma_client.get(p)
                    self.accumulated += grads
                    self.acc_counter += 1
                    self.timeline.end("get_buffers")
                    plasma_ids.remove(p)
                    break
        self.timeline.end("add_spinwait")

    def add(self, grad_shard_id):
        """Add the given gradient value to the accumulated gradients."""
        self.timeline.start("add")
        self.timeline.start("get_buffers")
        oid = ray.pyarrow.plasma.ObjectID(grad_shard_id)
        grads = ray.worker.global_worker.plasma_client.get(oid)
        self.timeline.end("get_buffers")
        self.accumulated += grads
        self.acc_counter += 1
        self.timeline.end("add")

    def get(self, object_id):
        """Put the accumulated gradients to the given object id."""
        self.timeline.start("get")
        client = ray.worker.global_worker.plasma_client
        assert self.acc_counter == self.num_sgd_workers, self.acc_counter
        oid = ray.pyarrow.plasma.ObjectID(object_id)
        self.accumulated /= self.acc_counter
        client.put(self.accumulated.flatten(), object_id=oid)
        self.accumulated = np.zeros_like(self.accumulated)
        self.acc_counter = 0
        self.timeline.end("get")

    def get_timeline(self):
        return self.timeline

    def ip(self):
        return ray.services.get_node_ip_address()

    def warmup(self):
        warmup()
