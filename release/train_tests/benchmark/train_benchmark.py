import collections
import json
import logging
import os
import pprint
import time
import tempfile
from typing import Dict

import ray.train
from ray._private.test_utils import safe_write_to_results_json
from ray.data._internal.stats import Timer
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.util import date_str
import torch

from config import BenchmarkConfig, cli_to_config
from factory import BenchmarkFactory
from image_classification.image_classification_parquet.factory import (
    ImageClassificationParquetFactory,
)
from image_classification.image_classification_jpeg.factory import (
    ImageClassificationJpegFactory,
)
from logger_utils import ContextLoggerAdapter

logger = ContextLoggerAdapter(logging.getLogger(__name__))


# TODO: Pull out common logic into a base class, and make this a TorchTrainLoopRunner.
class TrainLoopRunner:
    def __init__(self, factory: BenchmarkFactory):
        self.factory = factory
        self.benchmark_config = factory.benchmark_config

        model = factory.get_model()
        self.model = ray.train.torch.prepare_model(model)

        self.loss_fn = factory.get_loss_fn()

        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-3)

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
            f"Restoring from checkpoint: {checkpoint} for worker {ray.train.get_context().get_world_rank()}"
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

    def run(self):
        logger.info(
            f"Starting training for {self.benchmark_config.num_epochs} epochs for worker {ray.train.get_context().get_world_rank()}"
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
            logger.info(f"Starting @ epoch={self._train_epoch_idx}")

        train_dataloader = self.factory.get_train_dataloader()

        # NOTE: Time the first batch separately since it includes the dataset
        # pipeline warmup time.
        with self._metrics["train/iter_first_batch"].timer():
            batch = self.get_next_batch(train_dataloader)

        # Skip through batches if we restored to a middle of the epoch.
        # TODO: Compare this baseline to the data checkpointing approach once we have it.
        if self._train_batch_idx > 0:
            if ray.train.get_context().get_world_rank() == 0:
                logger.info(f"Skipping {self._train_batch_idx} batches...")

            for _ in range(self._train_batch_idx):
                with self._metrics["train/iter_skip_batch"].timer():
                    self.get_next_batch(train_dataloader)

        while batch:
            input_batch, labels = batch

            with self._metrics["train/step"].timer():
                if not self.benchmark_config.skip_train_step:
                    self.train_step(input_batch, labels)

            self._train_batch_idx += 1
            self._metrics["train/rows_processed"].add(len(labels))

            if self._should_validate_during_epoch():
                self.validate_and_checkpoint()

            if self._should_log_metrics():
                logger.info(pprint.pformat(self.get_metrics(), indent=2))

            with self._metrics["train/iter_batch"].timer():
                batch = self.get_next_batch(train_dataloader)

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
        try:
            return next(dataloader)
        except StopIteration:
            return None

    def train_step(self, input_batch, labels):
        self.model.train()
        out = self.model(input_batch)
        loss = self.loss_fn(out, labels)
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()

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
                f"Starting @ epoch={self._train_epoch_idx}, batch={self._train_batch_idx}"
            )

        val_dataloader = self.factory.get_val_dataloader()

        self.model.eval()

        total_loss = torch.tensor(0.0).to(ray.train.torch.get_device())
        num_rows = 0

        with self._metrics["validation/iter_first_batch"].timer():
            batch = self.get_next_batch(val_dataloader)

        while batch:
            input_batch, labels = batch

            with self._metrics["validation/step"].timer():
                if not self.benchmark_config.skip_validation_step:
                    total_loss += self.validate_step(input_batch, labels)

            num_rows += len(labels)
            self._metrics["validation/rows_processed"].add(len(labels))

            with self._metrics["validation/iter_batch"].timer():
                batch = self.get_next_batch(val_dataloader)

        return {"validation/loss": total_loss.item() / num_rows}

    def validate_step(self, input_batch, labels):
        with torch.no_grad():
            out = self.model(input_batch)
            loss = self.loss_fn(out, labels)
        return loss

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
                f"Restored to epoch={self._train_epoch_idx}, train_batch_idx={self._train_batch_idx} "
                f"from checkpoint: {ray.train.get_checkpoint()}"
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
                f"Saved @ epoch={self._train_epoch_idx}, train_batch_idx={self._train_batch_idx}"
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


METRICS_OUTPUT_PATH = "/mnt/cluster_storage/train_benchmark_metrics.json"


def train_fn_per_worker(config):
    factory = config["factory"]
    runner = TrainLoopRunner(factory)
    runner.run()

    metrics = runner.get_metrics()
    if ray.train.get_context().get_world_rank() == 0:
        with open(METRICS_OUTPUT_PATH, "w") as f:
            json.dump(metrics, f)


def main():
    benchmark_config: BenchmarkConfig = cli_to_config()
    logger.info(pprint.pformat(benchmark_config.__dict__, indent=2))

    if benchmark_config.task == "image_classification_parquet":
        factory = ImageClassificationParquetFactory(benchmark_config)
    elif benchmark_config.task == "image_classification_jpeg":
        factory = ImageClassificationJpegFactory(benchmark_config)
    else:
        raise ValueError

    ray_data_execution_options = ray.train.DataConfig.default_ingest_options()
    ray_data_execution_options.locality_with_output = (
        benchmark_config.locality_with_output
    )
    ray_data_execution_options.actor_locality_enabled = (
        benchmark_config.actor_locality_enabled
    )

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config={"factory": factory},
        scaling_config=ray.train.ScalingConfig(
            num_workers=benchmark_config.num_workers,
            use_gpu=not benchmark_config.mock_gpu,
            resources_per_worker={"MOCK_GPU": 1} if benchmark_config.mock_gpu else None,
        ),
        dataset_config=ray.train.DataConfig(
            datasets_to_split="all",
            execution_options=ray_data_execution_options,
            enable_shard_locality=benchmark_config.enable_shard_locality,
        ),
        run_config=ray.train.RunConfig(
            storage_path=f"{os.environ['ANYSCALE_ARTIFACT_STORAGE']}/train_benchmark/",
            name=date_str(include_ms=True),
            failure_config=ray.train.FailureConfig(
                max_failures=benchmark_config.max_failures
            ),
        ),
        datasets=factory.get_ray_datasets(),
    )
    trainer.fit()

    with open(METRICS_OUTPUT_PATH, "r") as f:
        metrics = json.load(f)

    final_metrics_str = (
        "Final metrics:\n" + "-" * 80 + "\n" + pprint.pformat(metrics) + "\n" + "-" * 80
    )
    logger.info(final_metrics_str)

    # Write metrics as a release test result.
    safe_write_to_results_json(metrics)


if __name__ == "__main__":
    # Workers need to access the working directory module.
    ray.init(runtime_env={"working_dir": os.path.dirname(__file__)})
    main()
