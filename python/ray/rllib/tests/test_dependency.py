#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys

os.environ["RLLIB_TEST_NO_TF_IMPORT"] = "1"

if __name__ == "__main__":
    from ray.rllib.agents.a3c import A2CTrainer
    assert "tensorflow" not in sys.modules, "TF initially present"

    # note: no ray.init(), to test it works without Ray
    trainer = A2CTrainer(
        env="CartPole-v0", config={
            "use_pytorch": True,
            "num_workers": 0
        })
    trainer.train()

    assert "tensorflow" not in sys.modules, "TF should not be imported"
