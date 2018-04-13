#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys

import ray
from ray.tune import register_trainable, run_experiments


def f(config, reporter):
    reporter(timesteps_total=1)


if __name__ == "__main__":
    ray.init()
    register_trainable("my_class", f)
    run_experiments({
        "test": {
            "run": "my_class",
            "stop": {
                "training_iteration": 1
            }
        }
    })
    assert 'ray.rllib' not in sys.modules, "RLlib should not be imported"
