from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import ray
from ray.tune import Trainable, run, Experiment
from ray.tune.schedulers import HyperBandScheduler
import tarfile
import urllib
import time
import json
from tabular_benchmarks import (
    FCNetProteinStructureBenchmark, FCNetSliceLocalizationBenchmark,
    FCNetNavalPropulsionBenchmark, FCNetParkinsonsTelemonitoringBenchmark
)

def download_fcnet():
    # download dataset
    file_tmp = urllib.request.urlretrieve(
        "http://ml4aad.org/wp-content/uploads/2019/01" +
        "/fcnet_tabular_benchmarks.tar.gz",
        "./fcnet_tabular_benchmarks")[0]
    base_name = os.path.basename(
        "http://ml4aad.org/wp-content/uploads/2019/01" +
        "fcnet_tabular_benchmarks.tar.gz")

    file_name, file_extension = os.path.splitext(base_name)
    tar = tarfile.open(file_tmp)
    tar.extractall(file_name)


class AbstractFCNetTrainable(Trainable):
    """ Trainable using FCNet Benchmark from Tabular Benchmarks for Hyperparameter
        Optimization and Neural Architecture Search """
    BENCHMARK_CLASS = None

    def _setup(self, config):
        self.net = self.BENCHMARK_CLASS(data_dir="./fcnet_tabular_benchmarks/")


    def _train(self):
        acc, time = self.net.objective_function(self.config, self.iteration)
        return {"validation_accuracy": acc, "runtime": time}

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


class FCNetProteinStructureTrainable(AbstractFCNetTrainable):
    BENCHMARK_CLASS = FCNetProteinStructureBenchmark


class FCNetSliceLocalizationTrainable(AbstractFCNetTrainable):
    BENCHMARK_CLASS = FCNetSliceLocalizationBenchmark


class FCNetNavalPropulsionTrainable(AbstractFCNetTrainable):
    BENCHMARK_CLASS = FCNetNavalPropulsionBenchmark


class FCNetParkinsonsTelemonitoringTrainable(AbstractFCNetTrainable):
    BENCHMARK_CLASS = FCNetParkinsonsTelemonitoringBenchmark


if __name__ == "__main__":
    """Example with FCNetProteinStructure and Hyperband."""
    download_fcnet()
    hyperband = HyperBandScheduler(
        time_attr="training_iteration",
        metric="episode_reward_mean",
        mode="max",
        max_t=100)

    exp = Experiment(
        name="hyperband_fcnet_protein_test",
        run=FCNetProteinStructureTrainable,
        num_samples=20,
        stop={"training_iteration": 1},
        config=FCNetProteinStructureTrainable().get_configuration_space())

    run(exp, scheduler=hyperband)
