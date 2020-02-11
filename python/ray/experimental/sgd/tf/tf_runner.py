import logging
import json
import os
import numpy as np
import threading

import ray
import ray.services
from ray.experimental.sgd import utils

logger = logging.getLogger(__name__)


def _try_import_strategy():
    """Late import for Tesnorflow"""
    import tensorflow as tf
    return tf.distribute.experimental.MultiWorkerMirroredStrategy


def _noop():
    pass


def _load_proper_LambdaCallback():
    # default TF2 LambdaCallback does not support the new callback naming

    import tensorflow as tf

    class LambdaCallback(tf.keras.callbacks.LambdaCallback):
        def __init__(self, on_batch_begin=_noop, on_batch_end=_noop):
            super().__init__()

            def noop(self, batch, logs):
                pass

            self.batch_begin = on_batch_begin
            self.batch_end = on_batch_end

        def on_predict_batch_begin(self, batch, logs):
            self.batch_begin(batch, logs)

        def on_train_batch_begin(self, batch, logs):
            self.batch_begin(batch, logs)

        def on_test_batch_begin(self, batch, logs):
            self.batch_begin(batch, logs)

        def on_predict_batch_end(self, batch, logs):
            self.batch_end(batch, logs)

        def on_train_batch_end(self, batch, logs):
            self.batch_end(batch, logs)

        def on_test_batch_end(self, batch, logs):
            self.batch_end(batch, logs)

    return LambdaCallback


class TFRunner:
    """Manages a TensorFlow model for training."""

    def __init__(self,
                 model_creator,
                 data_creator,
                 init_hook=None,
                 config=None,
                 verbose=False):
        """Initializes the runner.

        Args:
            model_creator (dict -> Model): see tf_trainer.py.
            data_creator (dict -> tf.Dataset, tf.Dataset): see tf_trainer.py.
            config (dict): see tf_trainer.py.
            verbose (bool): Outputs training data if true.
        """

        self.model_creator = model_creator
        self.data_creator = data_creator
        self.config = {} if config is None else config
        self.epoch = 0
        self.verbose = verbose
        self.init_hook = init_hook

        self._model_thread = None
        self._recorded_all_results = threading.Event()
        self._ready_to_continue = threading.Barrier(2)  # main + model
        self._step_counter = 0
        self._step_limit = 1

        self._LambdaCallback = None

    def setup(self):
        """Initializes the model."""
        if self.init_hook is not None:
            self.init_hook()

        logger.debug("Creating dataset")
        self.train_dataset, self.test_dataset = self.data_creator(self.config)

        logger.debug("Creating model")
        self.model = self.model_creator(self.config)

        self._LambdaCallback = _load_proper_LambdaCallback()

    def setup_distributed(self, urls, world_rank, world_size):
        """Sets up TensorFLow distributed environment and initializes the model.

        Args:
            urls (str): the URLs that each node uses to connect.
            world_rank (int): the index of the runner.
            world_size (int): the total number of runners.
        """
        assert len(urls) == world_size

        if self.init_hook is not None:
            self.init_hook()

        tf_config = {
            "cluster": {
                "worker": urls
            },
            "task": {
                "index": world_rank,
                "type": "worker"
            }
        }
        os.environ["TF_CONFIG"] = json.dumps(tf_config)

        MultiWorkerMirroredStrategy = _try_import_strategy()

        # MultiWorkerMirroredStrategy handles everything for us, from
        # sharding the dataset (or even sharding the data itself if the loader
        # reads files from disk) to merging the metrics and weight updates
        #
        # worker 0 is the "chief" worker and will handle the map-reduce
        # every worker ends up with the exact same metrics and model
        # after model.fit
        #
        # because of this, we only really ever need to query its state
        self.strategy = MultiWorkerMirroredStrategy()

        self.train_dataset, self.test_dataset = self.data_creator(self.config)

        logger.debug("Creating model with MultiWorkerMirroredStrategy")
        with self.strategy.scope():
            self.model = self.model_creator(self.config)

        self._LambdaCallback = _load_proper_LambdaCallback()

    # runs on another thread
    def _record_progress(self, batchN, logs):
        self._step_counter += 1
        self._logs = logs

    # runs on another thread
    def _has_next_batch(self, batchN, logs):
        if batchN != 0:
            # always gate the first batch so the model doesn't immediately run
            if self._step_counter < self._step_limit:
                return

            self._recorded_all_results.set()

        # there is a choice here of whether to run
        # the model thread while waiting for the next step call
        # solved by moving the barrier to _record_logs to stop
        # training after it ends rather than before it begins
        self._ready_to_continue.wait()
        self._step_counter = 0

    def _generic_step(self, number_of_steps, config_key, action_fn, res_fn):
        if self._model_thread is None or not self._model_thread.is_alive():
            self._recorded_all_results.clear()
            self._ready_to_continue.reset()

            self._model_thread = None

        if self._model_thread is None:
            # new epoch --- new history
            self._history = None

            config = {}
            config.update(self.config.get(config_key, {}))
            config["verbose"] = 0  # we are using our own logging
            config["callbacks"] = (
                # todo: only create this callback once
                config.get("callbacks", []) + [
                    self._LambdaCallback(
                        on_batch_begin=self._has_next_batch,
                        on_batch_end=self._record_progress)
                ])

            logger.debug("Starting a new model thread")

            def f():
                logger.debug("Model thread is running")
                self._history = action_fn(config)

                # no reason to lock here, this should only happen once
                self._recorded_all_results.set()

            self._model_thread = threading.Thread(target=f)
            self._model_thread.start()

        # allow the thread to run for n steps
        self._step_limit = number_of_steps
        self._ready_to_continue.wait()

        self._recorded_all_results.wait()
        self._recorded_all_results.clear()

        if self._history is not None:
            # wait for the thread to finish now
            logger.debug("Joining the training thread")
            self._model_thread.join()

            stats = res_fn()
            return ("end", stats)

        return ("batch", self._logs)

    def _fit_action(self, config):
        return self.model.fit(self.train_dataset, **config)

    def _fit_get_results(self):
        self.epoch += 1

        return ({
            "train_" + k: v[-1]
            for k, v in self._history.history.items()
        })

    def fit_step(self, number_of_steps=1):
        """
        Run number_of_steps training steps, then report the most recent logs.
        """
        return self._generic_step(number_of_steps, "fit_config",
                                  self._fit_action, self._fit_get_results)

    def _validate_action(self, config):
        return self.local_model.evaluate(self.test_dataset, **config)

    def _validate_get_results(self):
        if isinstance(self._history, list):
            res = {
                "validation_" + k: v
                for k, v in zip(self.model.metrics_names, self._history)
            }
        else:
            res = {"loss": self._history}

        return res

    # todo: technically you can interleave validate_step and fit_step
    # and cause trouble for yourself
    #
    # todo: tensorflow fails to validate a distributed model
    # with a weird shape mismatch error if validation is called
    # after at least 2 other calls to model functions and at least one fit call
    # we revert here to just using a local model for now, but ideally
    # this needs to be fixed somehow
    #
    # still using the threaded setup so we actually get to show progress
    def validate_step(self, number_of_steps=1):
        """Evaluates the model on the validation data set."""

        logger.debug("Running a local model to get validation score.")
        self.local_model = self.model_creator(self.config)
        self.local_model.set_weights(self.model.get_weights())

        return self._generic_step(number_of_steps,
                                  "evaluate_config",
                                  self._validate_action,
                                  self._validate_get_results)

    def get_state(self):
        """Returns the state of the runner."""
        return {
            "epoch": self.epoch,
            "weights": self.model.get_weights(),
            "optimizer_weights": self.model.optimizer.get_weights()
        }

    def set_state(self, state):
        """Sets the state of the model."""

        self.model = self.model_creator(self.config)
        self.epoch = state["epoch"]
        self.model.set_weights(state["weights"])
        # This part is due to ray.get() changing scalar np.int64 object to int
        state["optimizer_weights"][0] = np.array(
            state["optimizer_weights"][0], dtype=np.int64)

        if self.model.optimizer.weights == []:
            self.model._make_train_function()
        self.model.optimizer.set_weights(state["optimizer_weights"])

    def shutdown(self):
        """Attempts to shut down the worker."""
        del self.model
        del self.train_dataset
        del self.test_dataset

    def get_node_ip(self):
        """Returns the IP address of the current node."""
        return ray.services.get_node_ip_address()

    def find_free_port(self):
        """Finds a free port on the current node."""
        return utils.find_free_port()
