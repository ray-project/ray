#!/usr/bin/env python

import os
import sys

if __name__ == "__main__":
    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TF_IMPORT"] = "1"

    from ray.rllib.agents.a3c import A2CTrainer

    assert (
        "tensorflow" not in sys.modules
    ), "`tensorflow` initially present, when it shouldn't!"

    # Note: No ray.init(), to test it works without Ray
    trainer = A2CTrainer(
        env="CartPole-v0", config={"framework": "torch", "num_workers": 0}
    )
    trainer.train()

    assert "tensorflow" not in sys.modules, (
        "`tensorflow` should not be imported after creating and " "training A3CTrainer!"
    )

    # Clean up.
    del os.environ["RLLIB_TEST_NO_TF_IMPORT"]

    print("ok")
