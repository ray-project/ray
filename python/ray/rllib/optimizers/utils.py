from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


def as_remote(evaluator_cls):
    evaluator_cls.sample = ray.method(num_return_vals=2)(
        evaluator_cls.sample)
    evaluator_cls.compute_gradients = ray.method(num_return_vals=2)(
        evaluator_cls.compute_gradients)
    return ray.remote(evaluator_cls)
