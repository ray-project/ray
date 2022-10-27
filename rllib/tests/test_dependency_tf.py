#!/usr/bin/env python

import os
import sys

if __name__ == "__main__":
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TF_IMPORT"] = "1"

    # Test registering (includes importing) all Trainers.
    from ray.rllib import _register_all

    # This should surface any dependency on tf, e.g. inside function
    # signatures/typehints.
    _register_all()

    from ray.rllib.algorithms.a2c import A2C

    assert (
        "tensorflow" not in sys.modules
    ), "`tensorflow` initially present, when it shouldn't!"

    # Note: No ray.init(), to test it works without Ray
    trainer = A2C(env="CartPole-v1", config={"framework": "torch", "num_workers": 0})
    trainer.train()

    assert (
        "tensorflow" not in sys.modules
    ), "`tensorflow` should not be imported after creating and training A3C!"

    # Clean up.
    del os.environ["RLLIB_TEST_NO_TF_IMPORT"]

    print("ok")
