import collections
import json
import logging
import os
import pprint
import time
import tempfile
from typing import Dict, Optional

import ray.train
from ray.data._internal.stats import Timer
import torch
from logger_utils import ContextLoggerAdapter

from benchmark_factory import BenchmarkFactory

logger = ContextLoggerAdapter(logging.getLogger(__name__))


class TrainLoopRunner:
    """Generic runner that sets up the training loop scaffolding.

    Collects perf metrics and handles periodic checkpointing and validation.
    """

    def __init__(self, factory: BenchmarkFactory):
        self.factory = factory
        self.benchmark_config = factory.benchmark_config

        self._setup()

        # Training progress state.
        self._train_batch_idx: int = 0
        self._train_epoch_idx: int = 0
        self._global_rows_processed_this_epoch: int = 0

        # Performance metrics
        self._metrics = collections.defaultdict(lambda: Timer())

        checkpoint = ray.train.get_checkpoint()
        if checkpoint:
            self._restore_from_checkpoint(checkpoint)

    # Methods for subclasses to implement.
    def _setup(self):
        """Subclasses should override this to setup the model, optimizer, etc.
        The attributes initialized in this method should only be used in the
        other overridden methods."""
        pass

    def _cleanup(self):
        """Subclasses can override this to cleanup any resources."""
        pass

    def _train_step(self, train_dataloader):
        """Subclasses should override this to implement the training step.
        A training step represents a single forward and backward pass on a batch of data.
        """
        raise NotImplementedError

    def _validate_step(self, val_dataloader):
        """Subclasses should override this to implement the validation step.
        A validation step represents a single forward pass on a batch of data."""
        raise NotImplementedError

    def _save_training_state(self, local_dir: str):
        """Subclasses should override this to save the training state.
        This should reference the model and optimizer state initialized
        in the `_setup` method."""
        pass

    def _load_training_state(self, local_dir: str):
        """Subclasses should override this to load the training state.
        This should reference the model and optimizer state initialized
        in the `_setup` method."""
        pass

    def _restore_from_checkpoint(self, checkpoint: ray.train.Checkpoint):
        logger.info(
            f"Restoring from checkpoint: {checkpoint} for worker "
            f"{ray.train.get_context().get_world_rank()}"
        )
        with tempfile.TemporaryDirectory(
            dir="/mnt/local_storage"
        ) as temp_checkpoint_dir:
            download_start = time.perf_counter()
            checkpoint.to_directory(temp_checkpoint_dir)
            download_time = time.perf_counter() - download_start

            load_start = time.perf_counter()
            self._load_checkpoint(temp_checkpoint_dir)
            load_time = time.perf_counter() - load_start

            self._metrics["checkpoint/download"].add(download_time)
            self._metrics["checkpoint/load"].add(load_time)

    def _wrap_dataloader(self, dataloader, train: bool = True):
        dataloader_iter = iter(dataloader)

        prefix = "train" if train else "validation"

        def dataloader_with_timers():
            try:
                with self._metrics[f"{prefix}/iter_first_batch"].timer():
                    batch = next(dataloader_iter)
                    if train:
                        self._train_batch_idx += 1
            except StopIteration:
                return

            while True:
                yield batch

                try:
                    with self._metrics[f"{prefix}/iter_batch"].timer():
                        batch = next(dataloader_iter)
                        if train:
                            self._train_batch_idx += 1
                except StopIteration:
                    return

        return dataloader_with_timers()

    @property
    def _num_batches_to_skip(self) -> int:
        """Calculate the number of batches to skip based on the number of rows already processed in this epoch."""

        global_batch_size = (
            self.benchmark_config.dataloader_config.train_batch_size
            * ray.train.get_context().get_world_size()
        )

        return self._global_rows_processed_this_epoch // global_batch_size

    def _train_epoch(self):
        """Subclasses can override the entrire `_train_epoch` method for more training
        logic customization."""
        if ray.train.get_context().get_world_rank() == 0:
            logger.info(f"Training starting @ epoch={self._train_epoch_idx}")

        train_dataloader = self.factory.get_train_dataloader()
        train_dataloader = self._wrap_dataloader(train_dataloader, train=True)

        # Skip through batches if we restored to a middle of the epoch.
        # TODO: Compare this baseline to the data checkpointing approach once we have it.
        if self._num_batches_to_skip:
            if ray.train.get_context().get_world_rank() == 0:
                logger.info(f"Skipping {self._num_batches_to_skip} batches...")

            for _ in range(self._num_batches_to_skip):
                with self._metrics["train/iter_skip_batch"].timer():
                    next(train_dataloader)

        for batch in train_dataloader:
            with self._metrics["train/step"].timer():
                if not self.benchmark_config.skip_train_step:
                    self._train_step(batch)

            # TODO: This is slightly off if the last batch is a partial batch (if drop_last=False)
            global_batch_size = (
                self.benchmark_config.dataloader_config.train_batch_size
                * ray.train.get_context().get_world_size()
            )
            self._metrics["train/rows_processed"].add(global_batch_size)

            self._global_rows_processed_this_epoch += global_batch_size

            if self._should_checkpoint_during_epoch():
                self._checkpoint()

            if self._should_validate_during_epoch():
                validation_metrics = self._validate()
                self._checkpoint(validation_metrics)

            if self._should_log_metrics():
                logger.info(pprint.pformat(self.get_metrics(), indent=2))

        self._train_epoch_idx += 1
        self._train_batch_idx = 0
        self._global_rows_processed_this_epoch = 0

    def _validate_epoch(self) -> Dict[str, float]:
        if ray.train.get_context().get_world_rank() == 0:
            logger.info(
                f"Validation starting @ epoch={self._train_epoch_idx}, "
                f"batch={self._train_batch_idx}"
            )

        val_dataloader = self.factory.get_val_dataloader()
        val_dataloader = self._wrap_dataloader(val_dataloader, train=False)

        total_loss = torch.tensor(0.0).to(ray.train.torch.get_device())
        num_rows = 0

        for batch in val_dataloader:
            with self._metrics["validation/step"].timer():
                if not self.benchmark_config.skip_validation_step:
                    total_loss += self._validate_step(batch)

            num_rows += self.benchmark_config.dataloader_config.validation_batch_size
            self._metrics["validation/rows_processed"].add(
                self.benchmark_config.dataloader_config.validation_batch_size
            )
        assert num_rows > 0, "Validation dataset yielded no batches."

        return {"validation/loss": total_loss.item() / num_rows}

    def _should_checkpoint_during_epoch(self) -> bool:
        """Handles the checkpoint_every_n_steps logic."""
        return (
            self.benchmark_config.checkpoint_every_n_steps > 0
            and self._train_batch_idx % self.benchmark_config.checkpoint_every_n_steps
            == 0
        )

    def _should_validate_during_epoch(self) -> bool:
        """Handles the validate_every_n_steps logic."""
        return (
            self.benchmark_config.validate_every_n_steps > 0
            and self._train_batch_idx % self.benchmark_config.validate_every_n_steps
            == 0
        )

    def _should_log_metrics(self) -> bool:
        """Handles the log_metrics_every_n_steps logic."""
        return (
            self.benchmark_config.log_metrics_every_n_steps > 0
            and self._train_batch_idx % self.benchmark_config.log_metrics_every_n_steps
            == 0
        )

    def _validate(self) -> Dict[str, float]:
        with self._metrics["validation/epoch"].timer():
            validation_metrics = self._validate_epoch()
            return validation_metrics

    def _checkpoint(self, metrics: Optional[Dict[str, float]] = None):
        with tempfile.TemporaryDirectory(
            dir="/mnt/local_storage"
        ) as temp_checkpoint_dir:
            with self._metrics["checkpoint/save"].timer():
                self._save_checkpoint(temp_checkpoint_dir)

            with self._metrics["checkpoint/report"].timer():
                self._report_checkpoint(
                    metrics=metrics or {},
                    checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
                )

    def _load_checkpoint(self, local_dir: str):
        self._load_training_state(local_dir)

        run_state = torch.load(os.path.join(local_dir, "run_state.pt"))
        self._train_epoch_idx = run_state["epoch"]
        self._train_batch_idx = run_state["batch_idx"]
        self._global_rows_processed_this_epoch = run_state[
            "global_rows_processed_this_epoch"
        ]

        with open(os.path.join(local_dir, "metrics.json"), "r") as f:
            metrics_json = json.load(f)

        for k, v in metrics_json.items():
            self._metrics[k].__dict__.update(v)

        if ray.train.get_context().get_world_rank() == 0:
            logger.info(
                f"Restored to epoch={self._train_epoch_idx}, "
                f"train_batch_idx={self._train_batch_idx} from checkpoint: "
                f"{ray.train.get_checkpoint()}"
            )

    def _save_checkpoint(self, local_dir: str):
        logger.info(
            f"Saving checkpoint @ epoch={self._train_epoch_idx}, "
            f"train_batch_idx={self._train_batch_idx}"
        )

        self._save_training_state(local_dir)

        if ray.train.get_context().get_world_rank() == 0:
            run_state = {
                "epoch": self._train_epoch_idx,
                "batch_idx": self._train_batch_idx,
                "global_rows_processed_this_epoch": self._global_rows_processed_this_epoch,
            }
            torch.save(run_state, os.path.join(local_dir, "run_state.pt"))

            metrics_json = {k: v.__dict__.copy() for k, v in self._metrics.items()}
            with open(os.path.join(local_dir, "metrics.json"), "w") as f:
                json.dump(metrics_json, f)

    def _report_checkpoint(self, metrics, checkpoint):
        logger.info(
            f"Uploading checkpoint @ epoch={self._train_epoch_idx}, "
            f"train_batch_idx={self._train_batch_idx}"
        )

        checkpoint_dir_name = (
            f"checkpoint_epoch={self._train_epoch_idx}_batch={self._train_batch_idx}"
        )

        ray.train.report(
            metrics,
            checkpoint=checkpoint,
            checkpoint_dir_name=checkpoint_dir_name,
        )

    def run(self):
        starting_epoch = self._train_epoch_idx

        for _ in range(starting_epoch, self.benchmark_config.num_epochs):
            with self._metrics["train/epoch"].timer():
                self._train_epoch()

            if not self.benchmark_config.skip_validation_at_epoch_end:
                validation_metrics = self._validate()
                self._checkpoint(validation_metrics)

            if ray.train.get_context().get_world_rank() == 0:
                logger.info(pprint.pformat(self.get_metrics(), indent=2))

        self._cleanup()

    def get_metrics(self, dataset_creation_time: float = 0.0) -> Dict[str, float]:
        # TODO: These metrics should be aggregated across training workers.
        metrics = {}
        for key, metric in self._metrics.items():
            metrics.update(
                {
                    f"{key}-avg": metric.avg(),
                    f"{key}-min": metric.min(),
                    f"{key}-max": metric.max(),
                    f"{key}-total": metric.get(),
                }
            )

        metrics["train/dataset_creation_time"] = dataset_creation_time
        metrics["validation/dataset_creation_time"] = dataset_creation_time

        # Throughput
        # TODO: Ray Data can provide these throughput metrics automatically.
        train_time = (
            metrics["train/dataset_creation_time"]
            + self._metrics["train/step"].get()
            # Exclude the time it takes to get the first batch.
            # + self._metrics["train/iter_first_batch"].get()
            + self._metrics["train/iter_batch"].get()
        )
        if train_time > 0:
            metrics["train/global_throughput"] = (
                self._metrics["train/rows_processed"].get() / train_time
            )

        validation_time = (
            metrics["validation/dataset_creation_time"]
            + self._metrics["validation/step"].get()
            # Exclude the time it takes to get the first batch.
            # + self._metrics["validation/iter_first_batch"].get()
            + self._metrics["validation/iter_batch"].get()
        )
        if validation_time > 0:
            metrics["validation/global_throughput"] = (
                self._metrics["validation/rows_processed"].get() / validation_time
            )

        # Extra time that each worker spends to restore from checkpoint,
        # which includes downloading the checkpoint, loading the checkpoint,
        # and skipping through batches that were already processed.
        restoration_time = (
            self._metrics["checkpoint/download"].get()
            + self._metrics["checkpoint/load"].get()
            + self._metrics["train/iter_skip_batch"].get()
        )
        if restoration_time > 0:
            metrics["checkpoint/restoration_time"] = restoration_time

        # Dataloader metrics (ex: Ray Data stats)
        metrics.update(self.factory.get_dataloader_metrics())

        return metrics


class VanillaTorchRunner(TrainLoopRunner):
    """A simple runner that uses a PyTorch model, optimizer, and loss function."""

    def _setup(self):
        model = self.factory.get_model()
        self.model = ray.train.torch.prepare_model(model)
        self.loss_fn = self.factory.get_loss_fn()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-3)

    def _train_step(self, batch):
        self.model.train()

        input_batch, labels = batch

        self.model.train()
        self.optimizer.zero_grad()
        out = self.model(input_batch)
        loss = self.loss_fn(out, labels)
        loss.backward()
        self.optimizer.step()

    def _validate_step(self, batch):
        self.model.eval()

        input_batch, labels = batch

        with torch.no_grad():
            out = self.model(input_batch)
            loss = self.loss_fn(out, labels)
        return loss

    def _save_training_state(self, local_dir: str):
        # Standard DDP checkpointing.
        if ray.train.get_context().get_world_rank() == 0:
            torch.save(self.model.state_dict(), os.path.join(local_dir, "model.pt"))
            torch.save(
                self.optimizer.state_dict(), os.path.join(local_dir, "optimizer.pt")
            )

    def _load_training_state(self, local_dir: str):
        self.model.load_state_dict(
            torch.load(os.path.join(local_dir, "model.pt"), map_location="cpu")
        )
        self.optimizer.load_state_dict(
            torch.load(os.path.join(local_dir, "optimizer.pt"), map_location="cpu")
        )
