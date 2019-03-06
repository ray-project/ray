from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.experimental.serve import RayServeMixin, single_input


@ray.remote
class Counter(RayServeMixin):
    """Return the query id. Used for testing router."""

    def __init__(self):
        self.counter = 0

    @single_input
    def __call__(self, single_input):
        self.counter += 1
        return self.counter


@ray.remote
class CustomCounter(RayServeMixin):
    """Return the query id. Used for testing `serve_method` signature."""

    serve_method = "count"

    def count(self, input_batch):
        return [1 for _ in range(len(input_batch))]
