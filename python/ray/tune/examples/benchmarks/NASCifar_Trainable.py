""" Trainable using NASCifar Benchmark from Tabular Benchmarks for Hyperparameter Optimization and Neural Architecture Search """
from __future__ import print_function

import os
from ray.tune import Trainable
from nas_benchmarks.tabular_benchmarks import NASCifar10A, NASCifar10B, NASCifar10C
import urllib.request


class NASCifar10Trainable(Trainable):
    def _setup(self, config=None, logger_creator=None):
        # download dataset
        urllib.urlretrieve(
            'https://storage.googleapis.com/nasbench/nasbench_full.tfrecord',
            './nasbench')

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


class NASCifar10ATrainable(NASCifar10Trainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = NASCifar10A('./nasbench')
        super(NASCifar10ATrainable, self).__init__(config, logger_creator)


class NASCifar10BTrainable(NASCifar10Trainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = NASCifar10B('./nasbench')
        super(NASCifar10BTrainable, self).__init__(config, logger_creator)


class NASCifar10CTrainable(NASCifar10Trainable):
    def __init__(self, config=None, logger_creator=None):
        self.net = NASCifar10C('./nasbench')
        super(NASCifar10CTrainable, self).__init__(config, logger_creator)
