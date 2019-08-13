from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import ray
from ray.tune import Trainable, run, Experiment
from ray.tune.schedulers import HyperBandScheduler
import urllib
import time
import json
from nas_benchmarks.tabular_benchmarks import NASCifar10A, NASCifar10B, \
                                                            NASCifar10C
import urllib.request


class NASCifar10Trainable(Trainable):
    """ Trainable using NASCifar Benchmark from Tabular Benchmarks for
        Hyperparameter Optimization and Neural Architecture Search """

    def _setup(self, config=None, data_dir=None, logger_creator=None):
        # download dataset
        urllib.urlretrieve(
            "https://storage.googleapis.com/nasbench" +
            "/nasbench_full.tfrecord", "./nasbench")

        self._global_start = config.get("start", time.time())
        self._trial_start = time.time()
        cwd = os.getcwd()
        self.config = config
        if ray.worker._mode() == ray.worker.LOCAL_MODE:
            os.chdir(cwd)
        self.iteration = 0

    def _train(self):
        acc, time = self.net.objective_function(self.config, self.iteration)
        self.iteration += 1
        return {"validation_accuracy": acc}

    def _save(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "w") as f:
            f.write(json.dumps({"iteration": self.iteration}))
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path) as f:
            self.iteration = json.loads(f.read())["iteration"]

    def get_configuration_space(self):
        cs = self.net.get_configuration_space()
        return cs.sample_configuration().get_dictionary()


class NASCifar10ATrainable(NASCifar10Trainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = NASCifar10A("./nasbench")
        super(NASCifar10ATrainable, self).__init__(config, logger_creator)


class NASCifar10BTrainable(NASCifar10Trainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = NASCifar10B("./nasbench")
        super(NASCifar10BTrainable, self).__init__(config, logger_creator)


class NASCifar10CTrainable(NASCifar10Trainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = NASCifar10C("./nasbench")
        super(NASCifar10CTrainable, self).__init__(config, logger_creator)


if __name__ == "__main__":
    """Example with NASCifar10A."""
    cs = NASCifar10A().get_configuration_space().get_dictionary()

    hyperband = HyperBandScheduler(
        time_attr="training_iteration",
        metric="episode_reward_mean",
        mode="max",
        max_t=100)

    exp = Experiment(
        name="hyperband_nas10a_test",
        run=NASCifar10ATrainable,
        num_samples=20,
        stop={"training_iteration": 1},
        config=NASCifar10ATrainable().get_configuration_space())

    run(exp, scheduler=hyperband)
