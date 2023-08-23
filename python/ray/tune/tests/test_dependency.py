#!/usr/bin/env python

import sys

import pytest

import ray
from ray import train
from ray.tune import register_trainable, run_experiments


def f(config):
    train.report(dict(timesteps_total=1))


def test_dependency():
    ray.init(num_cpus=2)

    register_trainable("my_class", f)
    run_experiments({"test": {"run": "my_class", "stop": {"training_iteration": 1}}})
    assert "ray.rllib" not in sys.modules, "RLlib should not be imported"
    assert "mlflow" not in sys.modules, "MLflow should not be imported"
    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
