#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

ray.init(redis_address="localhost:6379")

@ray.remote
def f():
    return 1

for _ in range(10):
    ray.get([f.remote() for _ in range(100000)])
