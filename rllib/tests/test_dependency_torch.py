#!/usr/bin/env python

import os
import sys

from ray.tune.logger import Logger

class MyPrintLogger(Logger):
    """Logs results by simply printing out everything.
    """

    def _init(self):
        # Custom init function.
        print("Initializing ...")
        # Setting up our log-line prefix.
        self.prefix = self.config.get("prefix")

    def on_result(self, result: dict):
        # Define, what should happen on receiving a `result` (dict).
        print(f"{self.prefix}: {result}")

    def close(self):
        # Releases all resources used by this logger.
        print("Closing")

    def flush(self):
        # Flushing all possible disk writes to permanent storage.
        print("Flushing ;)", flush=True)


if __name__ == "__main__":
    # Do not import torch for testing purposes.
    os.environ["RLLIB_TEST_NO_TORCH_IMPORT"] = "1"

    #from ray.tune.logger import NoopLogger

    #from ray.rllib.utils.framework import try_import_tf
    #tf1, tf, tfv = try_import_tf()
    #import re
    #tf_file = tf.__file__
    #torch_file = re.sub("tensorflow", "torch", tf_file)
    #print(torch_file)
    #assert os.path.isfile(torch_file)
    #with open(torch_file, "w") as f:
    #    print("""
#import traceback
#print('someone is trying to import torch!')
#for line in traceback.format_stack():
#    print(line.strip())
#""", file=f)

    from ray.rllib.agents.a3c import A2CTrainer
    assert "torch" not in sys.modules, \
        "`torch` initially present, when it shouldn't!"

    # Note: No ray.init(), to test it works without Ray
    trainer = A2CTrainer(
        env="CartPole-v0", config={
            "framework": "tf",
            "num_workers": 0,
            # Disable the logger due to a sort-import attempt of torch
            # inside the tensorboardX.SummaryWriter class.
            "logger_config": {
                "type": MyPrintLogger, #"ray.tune.logger.NoopLogger",
            },
        })
    trainer.train()

    assert "torch" not in sys.modules, \
        "`torch` should not be imported after creating and " \
        "training A3CTrainer!"

    # Clean up.
    del os.environ["RLLIB_TEST_NO_TORCH_IMPORT"]

    print("ok")
