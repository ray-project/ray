import numpy as np
import os
import torch
import torch.distributed as dist
import logging
import numbers
import tempfile
import time

import ray

from ray.tune import Trainable
from ray.tune.trial import Resources
from ray.experimental.sgd.pytorch.distributed_pytorch_runner import (
    DistributedPyTorchRunner)
from ray.experimental.sgd import utils
from ray.experimental.sgd.pytorch.pytorch_runner import PyTorchRunner

logger = logging.getLogger(__name__)
RESIZE_COOLDOWN_S = 10


class PyTorchTrainer:
    """Train a PyTorch model using distributed PyTorch.

    Launches a set of actors which connect via distributed PyTorch and
    coordinate gradient updates to train the provided model.
    """

    def __init__(self,
                 model_creator,
                 data_creator,
                 optimizer_creator,
                 loss_creator,
                 train_function=None,
                 validation_function=None,
                 initialization_hook=None,
                 config=None,
                 num_replicas=1,
                 use_gpu=False,
                 batch_size=16,
                 backend="auto"):
        """Sets up the PyTorch trainer.

        Args:
            model_creator (dict -> torch.nn.Module): creates the model
                using the config.
            data_creator (int, dict -> DataLoader, DataLoader): Function that
                takes in (batch_size, config) and returns two Torch DataLoader
                objects.
            optimizer_creator (torch.nn.Module, dict -> optimizer):
                creates the loss and optimizer using the model and the config.
            loss_creator (dict -> loss): Creates the loss function/criterion
                using the config.
            train_function: Trains a model for a epoch. This takes in (
                model, train_dataloader, criterion, optimizer, config), and
                returns a dict of training stats.
            validation_function: Runs validation. This takes in (
                model, val_dataloader, criterion, config) and returns a dict of
                validation stats.
            config (dict): configuration passed to "model_creator",
                "data_creator", "optimizer_creator", and "loss_creator".
            num_replicas (int): the number of workers used in distributed
                training.
            use_gpu (bool): Sets resource allocation for workers to 1 GPU
                if true.
            batch_size (int): batch size for an update.
            backend (string): backend used by distributed PyTorch.
        """
        # TODO: add support for mixed precision
        # TODO: add support for callbacks
        if num_replicas > 1 and not dist.is_available():
            raise ValueError(
                ("Distributed PyTorch is not supported on macOS. "
                 "To run without distributed PyTorch, set 'num_replicas=1'. "
                 "For more information, see "
                 "https://github.com/pytorch/examples/issues/467."))

        self.model_creator = model_creator
        self.data_creator = data_creator
        self.train_function = train_function
        self.optimizer_creator = optimizer_creator
        self.loss_creator = loss_creator
        self.validation_function = validation_function
        self.initialization_hook = initialization_hook
        self.config = {} if config is None else config
        self.optimizer_timer = utils.TimerStat(window_size=1)

        if backend == "auto":
            backend = "nccl" if use_gpu else "gloo"

        logger.info("Using {} as backend.".format(backend))
        self.backend = backend
        self.use_gpu = use_gpu
        self.batch_size = batch_size
        self.max_replicas = num_replicas
        self.temp_dir = tempfile.mkdtemp(prefix="raysgd")
        self._num_failures = 0
        self._last_resize = float("-inf")
        self._start_workers(self.max_replicas)

    def _start_workers(self, num_replicas):
        logger.info(f"start_workers: Setting %d replicas." % num_replicas)
        if num_replicas == 1:
            # Generate actor class
            Runner = ray.remote(
                num_cpus=1, num_gpus=int(self.use_gpu))(PyTorchRunner)
            # Start workers
            self.workers = [
                Runner.remote(
                    self.model_creator,
                    self.data_creator,
                    self.optimizer_creator,
                    self.loss_creator,
                    train_function=self.train_function,
                    validation_function=self.validation_function,
                    config=self.config,
                    batch_size=self.batch_size)
            ]
            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)
            # Get setup tasks in order to throw errors on failure
            ray.get(self.workers[0].setup.remote())
        else:
            # Generate actor class
            Runner = ray.remote(
                num_cpus=1,
                num_gpus=int(self.use_gpu))(DistributedPyTorchRunner)
            # Compute batch size per replica
            batch_size_per_replica = self.batch_size // num_replicas
            if self.batch_size % num_replicas > 0:
                new_batch_size = batch_size_per_replica * num_replicas
                logger.warning(
                    ("Changing batch size from {old_batch_size} to "
                     "{new_batch_size} to evenly distribute batches across "
                     "{num_replicas} replicas.").format(
                         old_batch_size=self.batch_size,
                         new_batch_size=new_batch_size,
                         num_replicas=num_replicas))
            # Start workers
            self.workers = [
                Runner.remote(
                    self.model_creator,
                    self.data_creator,
                    self.optimizer_creator,
                    self.loss_creator,
                    backend=self.backend,
                    train_function=self.train_function,
                    validation_function=self.validation_function,
                    config=self.config,
                    batch_size=batch_size_per_replica)
                for i in range(num_replicas)
            ]
            if self.initialization_hook:
                self.apply_all_workers(self.initialization_hook)

            # Compute URL for initializing distributed PyTorch
            ip = ray.get(self.workers[0].get_node_ip.remote())
            port = ray.get(self.workers[0].find_free_port.remote())
            address = "tcp://{ip}:{port}".format(ip=ip, port=port)
            # Get setup tasks in order to throw errors on failure
            ray.get([
                worker.setup.remote(address, i, len(self.workers))
                for i, worker in enumerate(self.workers)
            ])

    def train(self, max_retries=10, checkpoint="auto"):
        """Runs a training epoch.

        Runs an average over all values returned from workers. Set
        `max_retries` to enable fault handling in case of instance preemption.

        Args:
            max_retries (int): Must be non-negative. If set to N, will
                kill all current workers, query the Ray global state for
                total available resources, and re-launch up to the
                available resources. Behavior is not well-defined
                in case of shared cluster usage.
            checkpoint (str): Path to checkpoint to restore from if retrying.
                If max_retries is set and checkpoint == "auto", PyTorchTrainer
                will save a checkpoint before starting to train.
        """
        assert max_retries >= 0, "`max_retries` must be non-negative."
        if max_retries:
            if checkpoint == "auto":
                logger.debug("Retrying detected. Automatically checkpointing.")
                checkpoint = self.save(
                    os.path.join(self.temp_dir, "tmp_checkpoint"))
            elif not checkpoint:
                raise ValueError("Cannot retry from empty checkpoint.")

        if checkpoint and self._should_resize():
            logger.info("Resize opportunity detected. Attempting to scale up.")
            self._resize_workers(checkpoint=checkpoint)

        with self.optimizer_timer:
            success, worker_stats = self._train_step()
            # Fault handling
            for i in range(max_retries):
                if success:
                    break
                else:
                    self._num_failures += 1
                self._resize_workers(checkpoint=checkpoint)
                logger.info("Retrying training step with %d workers." % len(
                    self.workers))
                success, worker_stats = self._train_step()
        if not success:
            raise RuntimeError("Training run failed.")

        worker_stats = ray.get(worker_stats)

        train_stats = {}
        for stat_key in worker_stats[0]:
            if isinstance(worker_stats[0], numbers.Number):
                train_stats[stat_key] = np.nanmean(
                    [s.get(stat_key, np.nan) for s in worker_stats])
            else:
                train_stats[stat_key] = worker_stats[0][stat_key]
        return train_stats

    def _train_step(self):
        worker_stats = [w.step.remote() for w in self.workers]
        success = utils.check_for_failure(worker_stats)
        return success, worker_stats

    def apply_all_workers(self, fn):
        return ray.get([w.apply_fn.remote(fn) for w in self.workers])

    def validate(self):
        """Evaluates the model on the validation data set."""
        if self.validation_function is False:
            return {}
        worker_stats = ray.get([w.validate.remote() for w in self.workers])

        validation_stats = {}
        for stat_key in worker_stats[0]:
            validation_stats[stat_key] = np.nanmean(
                [s.get(stat_key, np.nan) for s in worker_stats])
        return validation_stats

    def get_model(self):
        """Returns the learned model(s)."""
        models = self.model_creator(self.config)
        state = ray.get(self.workers[0].get_state.remote())
        if len(state["models"]) == 1:
            models.load_state_dict(state["models"][0])
        else:
            for model, state_dict in zip(models, state["models"]):
                model.load_state_dict(state_dict)
        return models

    def save(self, checkpoint):
        """Saves the model(s) to the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        """
        state = ray.get(self.workers[0].get_state.remote())
        torch.save(state, checkpoint)
        return checkpoint

    def restore(self, checkpoint):
        """Restores the model from the provided checkpoint.

        Args:
            checkpoint (str): Path to target checkpoint file.

        """
        state = torch.load(checkpoint)
        state_id = ray.put(state)
        ray.get([worker.set_state.remote(state_id) for worker in self.workers])

    def shutdown(self, force=False):
        """Shuts down workers and releases resources."""
        for worker in self.workers:
            if not force:
                worker.shutdown.remote()
                worker.__ray_terminate__.remote()
            else:
                logger.warning("Killing worker {}.".format(worker))
                worker.__ray_kill__()

        self.workers = []

    def _resize_workers(self, checkpoint, max_retries=10):
        # check available resources
        self.shutdown(force=True)
        assert checkpoint, "Cannot restore without checkpoint."

        time.sleep(1)
        for i in range(max_retries):
            resources = ray.available_resources()
            new_workers = min(resources.get("CPU", 0), self.max_replicas)
            if self.use_gpu:
                new_workers = min(resources.get("GPU", 0), new_workers)
            if new_workers:
                self._last_resize = time.time()
                self._start_workers(int(new_workers))
                self.restore(checkpoint)
                return
            else:
                delay = 2**i
                logger.info("Resources: {}".format(resources))
                logger.warning(
                    "No new workers found. Retrying in %d sec." % delay)
                time.sleep(delay)
        raise RuntimeError("Exceeded max_retries for relaunching workers.")

    def _should_resize(self):
        """Returns True if past cooldown and exists resources to scale up."""
        worker_gap = self.max_replicas - len(self.workers)
        past_cooldown = (time.time() - self._last_resize) > RESIZE_COOLDOWN_S
        if past_cooldown and worker_gap:
            resources = ray.available_resources()
            potential_workers = min(resources.get("CPU", 0), self.max_replicas)
            if self.use_gpu:
                potential_workers = min(
                    resources.get("GPU", 0), potential_workers)
            return potential_workers > 0
        return False


class PyTorchTrainable(Trainable):
    @classmethod
    def default_resource_request(cls, config):
        return Resources(
            cpu=0,
            gpu=0,
            extra_cpu=config["num_replicas"],
            extra_gpu=int(config["use_gpu"]) * config["num_replicas"])

    def _setup(self, config):
        self._trainer = PyTorchTrainer(**config)

    def _train(self):
        train_stats = self._trainer.train()
        validation_stats = self._trainer.validate()

        train_stats.update(validation_stats)

        # output {"mean_loss": test_loss, "mean_accuracy": accuracy}
        return train_stats

    def _save(self, checkpoint_dir):
        return self._trainer.save(os.path.join(checkpoint_dir, "model.pth"))

    def _restore(self, checkpoint_path):
        return self._trainer.restore(checkpoint_path)

    def _stop(self):
        self._trainer.shutdown()
