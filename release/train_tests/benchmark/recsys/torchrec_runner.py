import collections
import json
import logging
import os
import pprint
import time
import tempfile
from typing import Dict

import torch
from torchrec.distributed import TrainPipelineSparseDist
from torchrec.optim.keyed import CombinedOptimizer, KeyedOptimizerWrapper
from torchrec.optim.optimizers import in_backward_optimizer_filter

import ray.train
import ray.train.torch
from ray._private.test_utils import safe_write_to_results_json
from ray.data._internal.stats import Timer
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.util import date_str

from config import BenchmarkConfig, cli_to_config
from factory import BenchmarkFactory
from image_classification.factory import ImageClassificationFactory


logger = logging.getLogger(__name__)


# TODO: Pull out common logic into a base class, and make this a TorchTrainLoopRunner.
class TorchRecRunner:
    def __init__(self, factory: BenchmarkFactory):
        self.factory = factory
        self.benchmark_config = factory.benchmark_config

        self.setup()

        # Training progress state.
        self._train_batch_idx: int = 0
        self._train_epoch_idx: int = 0

        # Performance metrics
        self._metrics = collections.defaultdict(lambda: Timer())

        checkpoint = ray.train.get_checkpoint()
        if checkpoint:
            self.restore_from_checkpoint(checkpoint)

    def restore_from_checkpoint(self, checkpoint: ray.train.Checkpoint):
        logger.info(
            f"[Checkpoint] Restoring from checkpoint: {checkpoint} for worker "
            f"{ray.train.get_context().get_world_rank()}"
        )
        with tempfile.TemporaryDirectory(
            dir="/mnt/local_storage"
        ) as temp_checkpoint_dir:
            download_start = time.perf_counter()
            checkpoint.to_directory(temp_checkpoint_dir)
            download_time = time.perf_counter() - download_start

            load_start = time.perf_counter()
            self.load_checkpoint(temp_checkpoint_dir)
            load_time = time.perf_counter() - load_start

            self._metrics["checkpoint/download"].add(download_time)
            self._metrics["checkpoint/load"].add(load_time)

    def setup(self):
        device = ray.train.torch.get_device()

        self.model = self.factory.get_model()

        dense_optimizer = KeyedOptimizerWrapper(
            dict(in_backward_optimizer_filter(self.model.named_parameters())),
            lambda params: torch.optim.Adagrad(params, lr=15.0, eps=1e-8),
        )
        self.optimizer = CombinedOptimizer([self.model.fused_optimizer, dense_optimizer])
        # lr_scheduler = LRPolicyScheduler(
        #     optimizer, args.lr_warmup_steps, args.lr_decay_start, args.lr_decay_steps
        # )

        self.pipeline = TrainPipelineSparseDist(
            self.model, self.optimizer, device, execute_all_batches=True
        )


    def run(self):
        logger.info(
            f"[TrainLoopRunner] Starting training for {self.benchmark_config.num_epochs} "
            f"epochs for worker {ray.train.get_context().get_world_rank()}"
        )
        starting_epoch = self._train_epoch_idx

        for _ in range(starting_epoch, self.benchmark_config.num_epochs):
            self.train_epoch()

            if not self.benchmark_config.skip_validation_at_epoch_end:
                self.validate_and_checkpoint()

            if ray.train.get_context().get_world_rank() == 0:
                logger.info(pprint.pformat(self.get_metrics(), indent=2))

    def train_epoch(self):
        with self._metrics["train/epoch"].timer():
            self._train_epoch()

    def _train_epoch(self):
        if ray.train.get_context().get_world_rank() == 0:
            logger.info(f"[Train] Starting @ epoch={self._train_epoch_idx}")

        train_dataloader = self.factory.get_train_dataloader()
        batch_iterator = iter(train_dataloader)

        pipeline = self.pipeline
        pipeline._model.train()

        while True:
            try:
                with self._metrics["train/step"].timer():
                    out = pipeline.progress(batch_iterator)

                self._train_batch_idx += 1
                self._metrics["train/rows_processed"].add(len(out[-1]))

                # lr_scheduler.step()
            except StopIteration:
                break

            if self._should_log_metrics():
                logger.info(pprint.pformat(self.get_metrics(), indent=2))

        self._train_epoch_idx += 1
        self._train_batch_idx = 0

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

    def get_next_batch(self, dataloader):
        pass

    def train_step(self, input_batch, labels):
        pass

    def validate_and_checkpoint(self):
        with self._metrics["validation/epoch"].timer():
            validation_metrics = self.validate()

        with tempfile.TemporaryDirectory(
            dir="/mnt/local_storage"
        ) as temp_checkpoint_dir:
            with self._metrics["checkpoint/save"].timer():
                self.save_checkpoint(temp_checkpoint_dir)

            with self._metrics["checkpoint/report"].timer():
                self.report_checkpoint(
                    metrics=validation_metrics,
                    checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
                )

    def validate(self) -> Dict[str, float]:
        if ray.train.get_context().get_world_rank() == 0:
            logger.info(
                f"[Validation] Starting @ epoch={self._train_epoch_idx}, "
                f"batch={self._train_batch_idx}"
            )

        return {}

    def validate_step(self, input_batch, labels):
        pass

    def report_checkpoint(self, metrics, checkpoint):
        checkpoint_dir_name = (
            f"checkpoint_epoch={self._train_epoch_idx}_batch={self._train_batch_idx}"
        )

        ray.train.report(
            metrics,
            checkpoint=checkpoint,
            checkpoint_dir_name=checkpoint_dir_name,
        )

    def load_checkpoint(self, local_dir: str):
        self.model.load_state_dict(
            torch.load(os.path.join(local_dir, "model.pt"), map_location="cpu")
        )
        self.optimizer.load_state_dict(
            torch.load(os.path.join(local_dir, "optimizer.pt"), map_location="cpu")
        )

        train_state = torch.load(os.path.join(local_dir, "train_state.pt"))
        self._train_epoch_idx = train_state["epoch"]
        self._train_batch_idx = train_state["batch_idx"]

        with open(os.path.join(local_dir, "metrics.json"), "r") as f:
            metrics_json = json.load(f)

        for k, v in metrics_json.items():
            self._metrics[k].__dict__.update(v)

        if ray.train.get_context().get_world_rank() == 0:
            logger.info(
                f"[Checkpoint] Restored to epoch={self._train_epoch_idx}, "
                f"train_batch_idx={self._train_batch_idx} from checkpoint: "
                f"{ray.train.get_checkpoint()}"
            )

    def save_checkpoint(self, local_dir: str):
        train_state = {
            "epoch": self._train_epoch_idx,
            "batch_idx": self._train_batch_idx,
        }
        torch.save(self.model.state_dict(), os.path.join(local_dir, "model.pt"))
        torch.save(self.optimizer.state_dict(), os.path.join(local_dir, "optimizer.pt"))
        torch.save(train_state, os.path.join(local_dir, "train_state.pt"))

        metrics_json = {k: v.__dict__.copy() for k, v in self._metrics.items()}
        with open(os.path.join(local_dir, "metrics.json"), "w") as f:
            json.dump(metrics_json, f)

        if ray.train.get_context().get_world_rank() == 0:
            logger.info(
                f"[Checkpoint] Saved @ epoch={self._train_epoch_idx}, "
                f"train_batch_idx={self._train_batch_idx}"
            )

    def get_metrics(self) -> Dict[str, float]:
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

        # Throughput
        # TODO: Ray Data can provide these throughput metrics automatically.
        num_workers = ray.train.get_context().get_world_size()
        train_time = (
            self._metrics["train/step"].get()
            + self._metrics["train/iter_first_batch"].get()
            + self._metrics["train/iter_batch"].get()
        )
        if train_time > 0:
            metrics["train/local_throughput"] = (
                self._metrics["train/rows_processed"].get() / train_time
            )
            metrics["train/global_throughput"] = (
                metrics["train/local_throughput"] * num_workers
            )

        validation_time = (
            self._metrics["validation/step"].get()
            + self._metrics["validation/iter_first_batch"].get()
            + self._metrics["validation/iter_batch"].get()
        )
        if validation_time > 0:
            metrics["validation/local_throughput"] = (
                self._metrics["validation/rows_processed"].get() / validation_time
            )
            metrics["validation/global_throughput"] = (
                metrics["validation/local_throughput"] * num_workers
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

