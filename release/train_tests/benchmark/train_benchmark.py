import collections
import os
import pprint
import tempfile
import json
from typing import Dict

from ray._private.test_utils import safe_write_to_results_json
import ray.data
from ray.data._internal.stats import Timer
import ray.train
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.util import date_str
import torch

from config import BenchmarkConfig, cli_to_config
from factory import BenchmarkFactory
from image_classification.factory import ImageClassificationFactory


# TODO: Pull out common logic into a base class, and make this a TorchTrainLoopRunner.
class TrainLoopRunner:
    def __init__(self, factory: BenchmarkFactory):
        self.factory = factory
        self.benchmark_config = factory.benchmark_config

        model = factory.get_model()
        self.model = ray.train.torch.prepare_model(model)

        # TODO: Might want to re-initialize dataloaders at the start of every epoch.
        self.train_dataloader = factory.get_train_dataloader()
        self.val_dataloader = factory.get_val_dataloader()

        self.loss_fn = factory.get_loss_fn()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-3)

        # Training progress state.
        self._train_batch_idx: int = 0
        self._train_epoch_idx: int = 0

        # Load checkpoint if available.
        checkpoint = ray.train.get_checkpoint()
        if checkpoint:
            with checkpoint.as_directory() as temp_checkpoint_dir:
                self.load_checkpoint(temp_checkpoint_dir)

        # Performance metrics
        self._metrics = collections.defaultdict(lambda: Timer())

    def run(self):
        starting_epoch = self._train_epoch_idx

        for _ in range(starting_epoch, self.benchmark_config.num_epochs):
            self.train_epoch()

            if not self.benchmark_config.skip_validation_at_epoch_end:
                self.validate_and_checkpoint()

            if ray.train.get_context().get_world_rank() == 0:
                pprint.pprint(self.get_metrics())

    def train_epoch(self):
        with self._metrics["train/epoch"].timer():
            self._train_epoch()

    def _train_epoch(self):
        if ray.train.get_context().get_world_rank() == 0:
            print(f"Training starting @ epoch={self._train_epoch_idx}")

        # NOTE: Time the first batch separately since it includes the dataset
        # pipeline warmup time.
        with self._metrics["train/iter_first_batch"].timer():
            batch = self.get_next_batch(self.train_dataloader)

        # TODO: Handle the case where we restored to a middle of the epoch.

        while batch:
            input_batch, labels = batch

            with self._metrics["train/step"].timer():
                if not self.benchmark_config.skip_train_step:
                    self.train_step(input_batch, labels)

            self._train_batch_idx += 1
            self._metrics["train/rows_processed"].add(len(labels))

            if (
                self.benchmark_config.validate_every_n_steps > 0
                and self._train_batch_idx % self.benchmark_config.validate_every_n_steps
                == 0
            ):
                self.validate_and_checkpoint()

            if (
                self.benchmark_config.log_metrics_every_n_steps > 0
                and self._train_batch_idx
                % self.benchmark_config.log_metrics_every_n_steps
                == 0
            ):
                pprint.pprint(self.get_metrics())

            with self._metrics["train/iter_batch"].timer():
                batch = self.get_next_batch(self.train_dataloader)

        self._train_epoch_idx += 1
        self._train_batch_idx = 0

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

        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            with self._metrics["checkpoint/save"].timer():
                self.save_checkpoint(temp_checkpoint_dir)

            with self._metrics["checkpoint/report"].timer():
                self.report_checkpoint(
                    metrics=validation_metrics,
                    checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
                )

    def validate(self) -> Dict[str, float]:
        if ray.train.get_context().get_world_rank() == 0:
            print(
                f"Validation starting @ epoch={self._train_epoch_idx}, batch={self._train_batch_idx}"
            )

        self.model.eval()

        total_loss = torch.tensor(0.0).to(ray.train.torch.get_device())
        num_rows = 0

        with self._metrics["validation/iter_first_batch"].timer():
            batch = self.get_next_batch(self.val_dataloader)

        while batch:
            input_batch, labels = batch

            with self._metrics["validation/step"].timer():
                with torch.no_grad():
                    out = self.model(input_batch)
                    loss = self.loss_fn(out, labels)
                    total_loss += loss
                    num_rows += len(labels)
                    self._metrics["validation/rows_processed"].add(len(labels))

            with self._metrics["validation/iter_batch"].timer():
                batch = self.get_next_batch(self.val_dataloader)

        return {"validation/loss": total_loss.item() / num_rows}

    def report_checkpoint(self, metrics, checkpoint):
        ray.train.report(metrics, checkpoint=checkpoint)

    def load_checkpoint(self, local_dir: str):
        self.model.load_state_dict(torch.load(os.path.join(local_dir, "model.pt")))
        self.optimizer.load_state_dict(
            torch.load(os.path.join(local_dir, "optimizer.pt"))
        )

        train_state = torch.load(os.path.join(local_dir, "train_state.pt"))
        self._train_epoch_idx = train_state["epoch"]
        self._train_batch_idx = train_state["batch_idx"]
        print(
            f"[Fault Tolerance] Restored to epoch={self._train_epoch}, train_batch_idx={self._train_batch_idx}"
        )

    def save_checkpoint(self, local_dir: str):
        train_state = {
            "epoch": self._train_epoch_idx,
            "batch_idx": self._train_batch_idx,
        }
        torch.save(self.model.state_dict(), os.path.join(local_dir, "model.pt"))
        torch.save(self.optimizer.state_dict(), os.path.join(local_dir, "optimizer.pt"))
        torch.save(train_state, os.path.join(local_dir, "train_state.pt"))

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
    pprint.pprint(benchmark_config.__dict__, indent=2)

    if benchmark_config.task == "image_classification":
        factory = ImageClassificationFactory(benchmark_config)
    else:
        raise ValueError

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config={"factory": factory},
        scaling_config=ray.train.ScalingConfig(
            num_workers=benchmark_config.num_workers,
            use_gpu=True,
        ),
        run_config=ray.train.RunConfig(
            storage_path=f"{os.environ['ANYSCALE_ARTIFACT_STORAGE']}/train_benchmark/",
            name=date_str(include_ms=True),
        ),
        datasets=factory.get_ray_datasets(),
    )
    trainer.fit()

    with open(METRICS_OUTPUT_PATH, "r") as f:
        metrics = json.load(f)

    print("-" * 80)
    print("Final metrics:")
    pprint.pprint(metrics)
    print("-" * 80)

    # Write metrics as a release test result.
    safe_write_to_results_json(metrics)


if __name__ == "__main__":
    # Workers need to access the working directory module.
    ray.init(runtime_env={"working_dir": os.path.dirname(__file__)})
    main()
