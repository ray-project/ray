import numpy as np
import os
import logging
import pickle

import ray

from ray.tune import Trainable
from ray.tune.resources import Resources
from ray.experimental.sgd.tf.tf_runner import TFRunner

logger = logging.getLogger(__name__)


class TFTrainer:
    def __init__(self,
                 model_creator,
                 data_creator,
                 init_hook=None,
                 config=None,
                 num_replicas=1,
                 use_gpu=False,
                 verbose=False):
        """Sets up the TensorFlow trainer.

        Args:
            model_creator (dict -> Model): This function takes in the `config`
                dict and returns a compiled TF model.
            data_creator (dict -> tf.Dataset, tf.Dataset): Creates
                the training and validation data sets using the config.
                `config` dict is passed into the function.
            config (dict): configuration passed to 'model_creator',
                'data_creator'. Also contains `fit_config`, which is passed
                into `model.fit(data, **fit_config)` and
                `evaluate_config` which is passed into `model.evaluate`.
            num_replicas (int): Sets number of workers used in distributed
                training. Workers will be placed arbitrarily across the
                cluster.
            use_gpu (bool): Enables all workers to use GPU.
            verbose (bool): Prints output of one model if true.
        """
        self.model_creator = model_creator
        self.data_creator = data_creator
        self.config = {} if config is None else config
        self.use_gpu = use_gpu
        self.num_replicas = num_replicas
        self.verbose = verbose

        # Generate actor class
        # todo: are these resource quotas right?
        # should they be exposed to the client codee?
        Runner = ray.remote(num_cpus=1, num_gpus=int(use_gpu))(TFRunner)

        # todo: should we warn about using
        # distributed training on one device only?
        # it's likely that whenever this happens it's a mistake
        if num_replicas == 1:
            # Start workers
            self.workers = [
                Runner.remote(
                    model_creator,
                    data_creator,
                    init_hook,
                    config=self.config,
                    verbose=self.verbose)
            ]
            # Get setup tasks in order to throw errors on failure
            ray.get(self.workers[0].setup.remote())
        else:
            # Start workers
            self.workers = [
                Runner.remote(
                    model_creator,
                    data_creator,
                    init_hook,
                    config=self.config,
                    verbose=self.verbose and i == 0)
                for i in range(num_replicas)
            ]

            # Compute URL for initializing distributed setup
            ips = ray.get(
                [worker.get_node_ip.remote() for worker in self.workers])
            ports = ray.get(
                [worker.find_free_port.remote() for worker in self.workers])

            urls = [
                "{ip}:{port}".format(ip=ips[i], port=ports[i])
                for i in range(len(self.workers))
            ]

            # Get setup tasks in order to throw errors on failure
            ray.get([
                worker.setup_distributed.remote(urls, i, len(self.workers))
                for i, worker in enumerate(self.workers)
            ])

        import tensorflow as tf
        self._Progbar = tf.keras.utils.Progbar

    def _generic_model_driver(self, request_step, steps, metrics_prefix):
        """Runs a training epoch."""
        progbar = self._Progbar(steps)

        worker_stats = [None] * len(self.workers)
        done = False
        while not done:
            reqs = [request_step(w) for w in self.workers]
            for workerN, (type, logs) in enumerate(ray.get(reqs)):
                if type == "batch":
                    if workerN == 0:
                        metrics = [(k, logs[k]) for k in ["loss", "accuracy"]]

                        # see ./tf_runner.py:setup_distributed
                        # for an explanation of only taking
                        # the first worker's data
                        progbar.update(logs["batch"], metrics)
                elif type == "end":
                    if workerN == 0:
                        metrics = [("loss", logs[metrics_prefix + "loss"]),
                                   ("accuracy",
                                    logs[metrics_prefix + "accuracy"])]

                        progbar.update(steps, metrics)

                    worker_stats[workerN] = logs

                    # all workers do the same amount of work
                    done = True

        # see ./tf_runner.py:setup_distributed
        # for an explanation of only taking the first worker's data
        stats = worker_stats[0].copy()

        return stats

    def train(self, progress_report_interval=1):
        """Runs a training epoch."""

        # todo: this will only work in an explicit steps case
        fit_config = self.config.get("fit_config", {})

        return self._generic_model_driver(
            lambda w: w.fit_step.remote(progress_report_interval),
            fit_config.get("steps_per_epoch", None), "train_")

    def validate(self, progress_report_interval=1):
        """Evaluates the model on the validation data set."""
        logger.info("Starting validation step.")

        # todo: this will only work in an explicit steps case
        evaluate_config = self.config.get("evaluate_config", {})

        return self._generic_model_driver(
            lambda w: w.validate_step.remote(progress_report_interval),
            evaluate_config.get("steps", None), "validation_")

    def get_model(self):
        """Returns the learned model."""
        state = ray.get(self.workers[0].get_state.remote())
        return self._get_model_from_state(state)

    def save(self, checkpoint):
        """Saves the model at the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        """

        state = ray.get(self.workers[0].get_state.remote())

        with open(checkpoint, "wb") as f:
            pickle.dump(state, f)

        return checkpoint

    def restore(self, checkpoint):
        """Restores the model from the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        """
        with open(checkpoint, "rb") as f:
            state = pickle.load(f)

        state_id = ray.put(state)
        ray.get([worker.set_state.remote(state_id) for worker in self.workers])

    def shutdown(self):
        """Shuts down workers and releases resources."""
        for worker in self.workers:
            worker.shutdown.remote()
            worker.__ray_terminate__.remote()

    def _get_model_from_state(self, state):
        """Creates model and load weights from state"""

        model = self.model_creator(self.config)
        model.set_weights(state["weights"])

        # This part is due to ray.get() changing scalar np.int64 object to int
        state["optimizer_weights"][0] = np.array(
            state["optimizer_weights"][0], dtype=np.int64)

        if model.optimizer.weights == []:
            model._make_train_function()
        model.optimizer.set_weights(state["optimizer_weights"])

        return model


class TFTrainable(Trainable):
    @classmethod
    def default_resource_request(cls, config):
        return Resources(
            cpu=0,
            gpu=0,
            extra_cpu=config["num_replicas"],
            extra_gpu=int(config["use_gpu"]) * config["num_replicas"])

    def _setup(self, config):
        self._trainer = TFTrainer(
            model_creator=config["model_creator"],
            data_creator=config["data_creator"],
            config=config.get("trainer_config", {}),
            num_replicas=config["num_replicas"],
            use_gpu=config["use_gpu"])

    def _train(self):

        train_stats = self._trainer.train()
        validation_stats = self._trainer.validate()

        train_stats.update(validation_stats)

        return train_stats

    def _save(self, checkpoint_dir):
        return self._trainer.save(os.path.join(checkpoint_dir, "model"))

    def _restore(self, checkpoint_path):
        return self._trainer.restore(checkpoint_path)

    def _stop(self):
        self._trainer.shutdown()
