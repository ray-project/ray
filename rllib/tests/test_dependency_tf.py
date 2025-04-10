#!/usr/bin/env python

import os
import sys

if __name__ == "__main__":
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TF_IMPORT"] = "1"

    # Test registering (includes importing) all Algorithms.
    from ray.rllib import _register_all

    # This should surface any dependency on tf, e.g. inside function
    # signatures/typehints.
    _register_all()

    from ray.rllib.algorithms.ppo import PPOConfig

    assert (
        "tensorflow" not in sys.modules
    ), "`tensorflow` initially present, when it shouldn't!"

    config = (
        PPOConfig()
        .api_stack(
            enable_env_runner_and_connector_v2=True,
            enable_rl_module_and_learner=True,
        )
        .environment("CartPole-v1")
        .framework("torch")
        .env_runners(num_env_runners=0)
    )
    # Note: No ray.init(), to test it works without Ray
    algo = config.build()
    algo.train()

    assert (
        "tensorflow" not in sys.modules
    ), "`tensorflow` should not be imported after creating and training A3C!"

    # Clean up.
    del os.environ["RLLIB_TEST_NO_TF_IMPORT"]

    algo.stop()

    print("ok")
