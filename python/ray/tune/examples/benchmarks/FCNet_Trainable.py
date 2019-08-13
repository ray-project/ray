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
from nas_benchmarks.tabular_benchmarks import FCNetProteinStructureBenchmark, \
    FCNetSliceLocalizationBenchmark, FCNetNavalPropulsionBenchmark, \
    FCNetParkinsonsTelemonitoringBenchmark


class FCNetTrainable(Trainable):
    """ Trainable using FCNet Benchmark from Tabular Benchmarks for Hyperparameter
        Optimization and Neural Architecture Search """

    def _setup(self, config):

        # download dataset
        file_tmp = urllib.urlretrieve(
            "http://ml4aad.org/wp-content/uploads/2019/01" +
            "/fcnet_tabular_benchmarks.tar.gz",
            "./fcnet_tabular_benchmarks")[0]
        base_name = os.path.basename(
            "http://ml4aad.org/wp-content/uploads/2019/01" +
            "fcnet_tabular_benchmarks.tar.gz")

        file_name, file_extension = os.path.splitext(base_name)
        tar = tarfile.open(file_tmp)
        tar.extractall(file_name)

        self._global_start = config.get("start", time.time())
        self._trial_start = time.time()
        self.config = config
        cwd = os.getcwd()
        if ray.worker._mode() == ray.worker.LOCAL_MODE:
            os.chdir(cwd)
        self.iteration = 0

    def _train(self):
        acc, time = self.net.objective_function(self.config, self.iteration)
        self.iteration += 1
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
        return self.net.get_configuration_space().get_dictionary()


class FCNetProteinStructureTrainable(FCNetTrainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = FCNetProteinStructureBenchmark(
            data_dir="./fcnet_tabular_benchmarks/")
        super(FCNetProteinStructureTrainable, self).__init__(
            config, logger_creator)


class FCNetSliceLocalizationTrainable(FCNetTrainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = FCNetSliceLocalizationBenchmark(
            data_dir="./fcnet_tabular_benchmarks/")
        super(FCNetSliceLocalizationTrainable, self).__init__(
            config, logger_creator)


class FCNetNavalPropulsionTrainable(FCNetTrainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = FCNetNavalPropulsionBenchmark(
            data_dir="./fcnet_tabular_benchmarks/")
        super(FCNetNavalPropulsionTrainable, self).__init__(
            config, logger_creator)


class FCNetParkinsonsTelemonitoringTrainable(FCNetTrainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = FCNetParkinsonsTelemonitoringBenchmark(
            data_dir="./fcnet_tabular_benchmarks/")
        super(FCNetParkinsonsTelemonitoringTrainable, self).__init__(
            config, logger_creator)


if __name__ == "__main__":
    """Example with FCNetProteinStructure and Hyperband."""

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
