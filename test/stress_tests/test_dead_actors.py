#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import sys

import ray

logger = logging.getLogger(__name__)

ray.init(redis_address="localhost:6379")


@ray.remote
class Child(object):
    def __init__(self, death_probability):
        self.death_probability = death_probability

    def ping(self):
        # Exit process with some probability.
        exit_chance = np.random.rand()
        if exit_chance > self.death_probability:
            sys.exit(-1)


@ray.remote
class Parent(object):
    def __init__(self, num_children, death_probability):
        self.death_probability = death_probability
        self.children = [
            Child.remote(death_probability) for _ in range(num_children)
        ]

    def ping(self, num_pings):
        children_outputs = []
        for _ in range(num_pings):
            children_outputs += [
                child.ping.remote() for child in self.children
            ]
        try:
            ray.get(children_outputs)
        except Exception:
            # Replace the children if one of them died.
            self.__init__(len(self.children), self.death_probability)

    def kill(self):
        # Clean up children.
        ray.get([child.__ray_terminate__.remote() for child in self.children])


num_parents = 10
num_children = 10
death_probability = 0.95

parents = [
    Parent.remote(num_children, death_probability) for _ in range(num_parents)
]

for i in range(100):
    ray.get([parent.ping.remote(10) for parent in parents])

    # Kill a parent actor with some probability.
    exit_chance = np.random.rand()
    if exit_chance > death_probability:
        parent_index = np.random.randint(len(parents))
        parents[parent_index].kill.remote()
        parents[parent_index] = Parent.remote(num_children, death_probability)

    logger.info("Finished trial", i)
