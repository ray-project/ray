import os
import time

from ray import serve

# Simulates a heavyweight application build (large model import).
time.sleep(float(os.environ.get("SERVE_TEST_SLOW_IMPORT_S", "8")))


@serve.deployment
class Slow:
    def __init__(self):
        self.name = "slow"

    def reconfigure(self, config: dict):
        self.name = config.get("name", self.name)

    def __call__(self):
        return os.getpid(), self.name


node = Slow.bind()
