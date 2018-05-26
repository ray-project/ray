from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import queue

import ray
from ray.rllib.bc.experience_dataset import ExperienceDataset
from ray.rllib.bc.policy import BCPolicy
from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers import PolicyEvaluator


class BCEvaluator(PolicyEvaluator):
    def __init__(self, registry, env_creator, config, logdir):
        env = ModelCatalog.get_preprocessor_as_wrapper(registry, env_creator(
            config["env_config"]), config["model"])
        self.dataset = ExperienceDataset(config["dataset_path"])
        # TODO(rliaw): should change this to be just env.observation_space
        self.policy = BCPolicy(registry, env.observation_space.shape,
                               env.action_space, config)
        self.config = config
        self.logdir = logdir
        self.metrics_queue = queue.Queue()

    def sample(self):
        return self.dataset.sample(self.config["batch_size"])

    def compute_gradients(self, samples):
        gradient, info = self.policy.compute_gradients(samples)
        self.metrics_queue.put(
            {"num_samples": info["num_samples"], "loss": info["loss"]})
        return gradient, {}

    def apply_gradients(self, grads):
        self.policy.apply_gradients(grads)

    def get_weights(self):
        return self.policy.get_weights()

    def set_weights(self, params):
        self.policy.set_weights(params)

    def save(self):
        weights = self.get_weights()
        return pickle.dumps({
            "weights": weights})

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.set_weights(objs["weights"])

    def get_metrics(self):
        completed = []
        while True:
            try:
                completed.append(self.metrics_queue.get_nowait())
            except queue.Empty:
                break
        return completed


RemoteBCEvaluator = ray.remote(BCEvaluator)
GPURemoteBCEvaluator = ray.remote(num_gpus=1)(BCEvaluator)
