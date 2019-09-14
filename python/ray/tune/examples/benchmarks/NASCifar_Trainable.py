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
from tabular_benchmarks import NASCifar10A, NASCifar10B, NASCifar10C
import urllib.request

DATA_PATH = "./nasbench"


class NASCifar10Trainable(Trainable):
    BENCHMARK_PATH = None
    """ Trainable using NASCifar Benchmark from Tabular Benchmarks for
        Hyperparameter Optimization and Neural Architecture Search """

    def _setup(self, config):
        self.net = NASCifar10A(DATA_PATH)

    def _train(self):
        acc, time = self.net.objective_function(self.config, self.iteration)
        return {"validation_accuracy": acc}

    def _save(self, checkpoint_dir):
        return checkpoint_dir

    def _restore(self, checkpoint_path):
        pass

    def get_configuration_space(self):
        cs = self.net.get_configuration_space()
        return cs.sample_configuration().get_dictionary()


class NASCifar10ATrainable(NASCifar10Trainable):
    BENCHMARK_PATH = NASCifar10A


class NASCifar10BTrainable(NASCifar10Trainable):
    BENCHMARK_PATH = NASCifar10B

class NASCifar10CTrainable(NASCifar10Trainable):
    BENCHMARK_PATH = NASCifar10C


if __name__ == "__main__":
    """Example with NASCifar10A."""
    # download dataset
    urllib.request.urlretrieve(
        "https://storage.googleapis.com/nasbench" +
        "/nasbench_full.tfrecord", DATA_PATH)
    cs = NASCifar10A(DATA_PATH).get_configuration_space().get_dictionary()

    hyperband = HyperBandScheduler(
        time_attr="training_iteration",
        metric="episode_reward_mean",
        mode="max",
        max_t=100)

    run(run=NASCifar10ATrainable,
        name="hyperband_nas10a_test",
        num_samples=20,
        stop={"training_iteration": 1},
        config=NASCifar10ATrainable().get_configuration_space(),
        scheduler=hyperband)
