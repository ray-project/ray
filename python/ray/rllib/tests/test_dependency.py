#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys

import ray
from ray.rllib.agents.a3c import A2CTrainer


if __name__ == "__main__":
    assert "tensorflow" not in sys.modules, "TF initially present"

    # note: no ray.init(), to test it works without Ray
    trainer = A2CTrainer(
        env="CartPole-v0",
        config={"use_pytorch": True, "num_workers": 0})
    trainer.train()

    assert "tensorflow" not in sys.modules, "TF should not be imported"
