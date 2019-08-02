""" Trainable using FCNet Benchmark from Tabular Benchmarks for Hyperparameter Optimization and Neural Architecture Search """
from __future__ import print_function

import os

from ray.tune import Trainable
import tarfile
import urllib
from FCNet import FCNetBenchmark, FCNetProteinStructureBenchmark, FCNetSliceLocalizationBenchmark, FCNetNavalPropulsionBenchmark, FCNetParkinsonsTelemonitoringBenchmark


class FCNetProteinStructureTrainable(Trainable):
    def __init__(self, config=None, logger_creator=None):

        # download dataset
        file_tmp = urllib.urlretrieve('http://ml4aad.org/wp-content/uploads/2019/01/fcnet_tabular_benchmarks.tar.gz', filename=None)[0]
        base_name = os.path.basename('http://ml4aad.org/wp-content/uploads/2019/01/fcnet_tabular_benchmarks.tar.gz')

        file_name, file_extension = os.path.splitext(base_name)
        tar = tarfile.open(file_tmp)
        tar.extractall(file_name)

        self._global_start = config.get("start", time.time())
        self._trial_start = time.time()
        self.config = config
        cwd = os.getcwd()
        super(FCNetProteinStructureTrainable, self).__init__(config, logger_creator)
        if ray.worker._mode() == ray.worker.LOCAL_MODE:
            os.chdir(cwd)
        self.net =FCNetProteinStructureBenchmark()
        self.iteration = 0

    def _train(self):
        acc, time = self.net.objective_function(self.config, self.iteration)
        self.iteration += 1
        return {"validation_accuracy": acc, 'runtime': time}

    def _save(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "w") as f:
            f.write(json.dumps({"iteration": self.iteration}))
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path) as f:
            self.iteration = json.loads(f.read())["iteration"]


class FCNetSliceLocalizationTrainable(Trainable):
    def __init__(self, config=None, logger_creator=None):

        # download dataset
        file_tmp = urllib.urlretrieve('http://ml4aad.org/wp-content/uploads/2019/01/fcnet_tabular_benchmarks.tar.gz', filename=None)[0]
        base_name = os.path.basename('http://ml4aad.org/wp-content/uploads/2019/01/fcnet_tabular_benchmarks.tar.gz')

        file_name, file_extension = os.path.splitext(base_name)
        tar = tarfile.open(file_tmp)
        tar.extractall(file_name)

        self._global_start = config.get("start", time.time())
        self._trial_start = time.time()
        self.config = config
        cwd = os.getcwd()
        super(FCNetSliceLocalizationTrainable, self).__init__(config, logger_creator)
        if ray.worker._mode() == ray.worker.LOCAL_MODE:
            os.chdir(cwd)
        self.net =FCNetProteinStructureBenchmark()
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


class FCNetNavalPropulsionTrainable(Trainable):
    def __init__(self, config=None, logger_creator=None):

        # download dataset
        file_tmp = urllib.urlretrieve('http://ml4aad.org/wp-content/uploads/2019/01/fcnet_tabular_benchmarks.tar.gz', filename=None)[0]
        base_name = os.path.basename('http://ml4aad.org/wp-content/uploads/2019/01/fcnet_tabular_benchmarks.tar.gz')

        file_name, file_extension = os.path.splitext(base_name)
        tar = tarfile.open(file_tmp)
        tar.extractall(file_name)

        self._global_start = config.get("start", time.time())
        self._trial_start = time.time()
        self.config = config
        cwd = os.getcwd()
        super(FCNetNavalPropulsionTrainable, self).__init__(config, logger_creator)
        if ray.worker._mode() == ray.worker.LOCAL_MODE:
            os.chdir(cwd)
        self.net =FCNetProteinStructureBenchmark()
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


class FCNetParkinsonsTelemonitoringTrainable(Trainable):
    def __init__(self, config=None, logger_creator=None):

        # download dataset
        file_tmp = urllib.urlretrieve('http://ml4aad.org/wp-content/uploads/2019/01/fcnet_tabular_benchmarks.tar.gz', filename=None)[0]
        base_name = os.path.basename('http://ml4aad.org/wp-content/uploads/2019/01/fcnet_tabular_benchmarks.tar.gz')

        file_name, file_extension = os.path.splitext(base_name)
        tar = tarfile.open(file_tmp)
        tar.extractall(file_name)

        self._global_start = config.get("start", time.time())
        self._trial_start = time.time()
        self.config = config
        cwd = os.getcwd()
        super(FCNetParkinsonsTelemonitoringTrainable, self).__init__(config, logger_creator)
        if ray.worker._mode() == ray.worker.LOCAL_MODE:
            os.chdir(cwd)
        self.net =FCNetProteinStructureBenchmark()
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