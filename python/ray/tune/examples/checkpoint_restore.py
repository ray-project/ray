# Test for runner.save and runner.restore
import ray
import time
import logging

from ray.tune import Trainable
from ray.tune.error import TuneError
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.trial import Trial, DEBUG_PRINT_INTERVAL
from ray.tune.trial_runner import TrialRunner
from ray.tune.schedulers import (HyperBandScheduler, AsyncHyperBandScheduler,
                                 FIFOScheduler, MedianStoppingRule)

logger = logging.getLogger(__name__)
ray.init()

class TestTrain(Trainable):
    def _setup(self, config):
        self.state = {"hi": 1}

    def _train(self):
        time.sleep(1)
        self.state["hi"] += 1
        return {"timesteps_this_iter": 1}

    def _save(self, path):
        return self.state

    def _restore(self, state):
        self.state = state


scheduler = FIFOScheduler()

search_alg = BasicVariantGenerator()
search_alg.add_configurations(
    {"test":
        {"run": TestTrain,
         "stop": {"training_iteration": 20}}})
runner = TrialRunner(
    search_alg,
    scheduler=scheduler,
    trial_executor=None)

logger.info(runner.debug_string(max_debug=99999))

last_debug = 0
while not runner.is_finished():
    runner.step()
    if time.time() - last_debug > DEBUG_PRINT_INTERVAL:
        logger.info(runner.debug_string())
        last_debug = time.time()
