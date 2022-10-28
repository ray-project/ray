#!/usr/bin/env python

import os
import sys

if __name__ == "__main__":
    # Do not import torch for testing purposes.
    os.environ["RLLIB_TEST_NO_TORCH_IMPORT"] = "1"

    # Test registering (includes importing) all Algorithms.
    from ray.rllib import _register_all

    # This should surface any dependency on torch, e.g. inside function
    # signatures/typehints.
    _register_all()

    from ray.rllib.algorithms.a2c import A2C

    assert "torch" not in sys.modules, "`torch` initially present, when it shouldn't!"

    # Note: No ray.init(), to test it works without Ray
    algo = A2C(
        env="CartPole-v1",
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
    algo.train()

    assert (
        "torch" not in sys.modules
    ), "`torch` should not be imported after creating and training A3C!"

    # Clean up.
    del os.environ["RLLIB_TEST_NO_TORCH_IMPORT"]

    print("ok")
