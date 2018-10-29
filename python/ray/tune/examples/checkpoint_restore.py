# Test for runner.save and runner.restore
import logging
logging.basicConfig(level="DEBUG")
import ray
import time
import os
import shutil
import argparse

from ray.tune import Trainable
from ray.tune.error import TuneError
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.trial_runner import TrialRunner
from ray.tune.schedulers import (HyperBandScheduler, AsyncHyperBandScheduler,
                                 FIFOScheduler, MedianStoppingRule)

logger = logging.getLogger(__name__)
ray.init()
logdir = os.path.join(os.environ["TMPDIR"], "checkpoint_testing1")
if os.path.exists(logdir):
    shutil.rmtree(logdir)

os.makedirs(logdir)


class TestTrain(Trainable):
    def _setup(self, config):
        self.state = {"hi": 1}

    def _train(self):
        time.sleep(0.01)
        self.state["hi"] += 1
        return {
            "timesteps_this_iter": 1,
            "mean_accuracy": self.state["hi"]}

    def _save(self, path):
        return self.state

    def _restore(self, state):
        self.state = state


scheduler = FIFOScheduler()

search_alg = BasicVariantGenerator()
search_alg.add_configurations(
    {"test":
        {"run": TestTrain,
         "stop": {"training_iteration": 20},
         "num_samples": 10,
         "local_dir": logdir}})
runner = TrialRunner(
    search_alg,
    scheduler=scheduler,
    trial_executor=None)



last_debug = 0
for i in range(20):
    runner.step()
    logger.info(runner.debug_string(max_debug=99999))

runner.save(logdir, force=True)

for i in range(20):
    runner.step()
    logger.info(runner.debug_string(max_debug=99999))

runner.restore(logdir)

for i in range(50):
    runner.step()
    logger.info(runner.debug_string(max_debug=99999))

print(logdir)
