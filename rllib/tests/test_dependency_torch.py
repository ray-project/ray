#!/usr/bin/env python

import os
import sys

if __name__ == "__main__":
    # Do not import torch for testing purposes.
    os.environ["RLLIB_TEST_NO_TORCH_IMPORT"] = "1"

    from ray.rllib.agents.a3c import A2CTrainer

    assert "torch" not in sys.modules, "`torch` initially present, when it shouldn't!"

    # Note: No ray.init(), to test it works without Ray
    trainer = A2CTrainer(
        env="CartPole-v0",
        config={
            "framework": "tf",
            "num_workers": 0,
            # Disable the logger due to a sort-import attempt of torch
            # inside the tensorboardX.SummaryWriter class.
            "logger_config": {
                "type": "ray.tune.logger.NoopLogger",
            },
        },
    )
    trainer.train()

    assert (
        "torch" not in sys.modules
    ), "`torch` should not be imported after creating and training A3CTrainer!"

    # Clean up.
    del os.environ["RLLIB_TEST_NO_TORCH_IMPORT"]

    print("ok")
