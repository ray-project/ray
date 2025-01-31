# This release test should test distributed training with Torch.
# It should allow configurable:
# - # of workers
# - parallelism strategy
# - dataset and dataloader (Ray Data, native torch dataloader, webdataset, mock dataloader (for upper-bound) etc.)
# - checkpointing frequency
# - validation frequency
# - fault tolerance

# It should measure:
# - training time
# - training throughput
# - dataloading throughput
# - validation throughput
# - fault tolerance recovery time if enabled
# - checkpointing time
import argparse
import collections
import os
import tempfile
import time
from typing import Dict

import torch

import ray.data
from ray.data._internal.stats import Timer
import ray.train
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.util import date_str

from config import BenchmarkConfig
from factory import BenchmarkFactory
from image_classification_factory import ImageClassificationFactory


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
        self._timers = collections.defaultdict(lambda: Timer())
        self._train_throughput = Timer()
        self._validation_throughput = Timer()

    def run(self):
        starting_epoch = self._train_epoch_idx

        for _ in range(starting_epoch, self.benchmark_config.num_epochs):
            self.train_epoch()

            if not self.benchmark_config.skip_validation_at_epoch_end:
                self.validate_and_checkpoint()

    def train_epoch(self):
        with self._timers["train/epoch"].timer():
            self._train_epoch()

    def _train_epoch(self):
        if ray.train.get_context().get_world_rank() == 0:
            print(f"Training epoch starting @ epoch={self._train_epoch_idx}")

        step_start_s = time.perf_counter()

        # NOTE: Time the first batch separately since it includes the dataset
        # pipeline warmup time.
        with self._timers["train/iter_first_batch"].timer():
            batch = self.get_next_batch(self.train_dataloader)

        # TODO: Handle the case where we restored to a middle of the epoch.

        while batch:
            input_batch, labels = batch

            with self._timers["train/step"].timer():
                if not self.benchmark_config.skip_train_step:
                    self.train_step(input_batch, labels)

            # Compute global training throughput
            step_elapsed = time.perf_counter() - step_start_s
            batch_size = len(labels)
            local_throughput = batch_size / step_elapsed
            # TODO: This calculation assumes that all workers have the same throughput.
            global_throughput = local_throughput * ray.train.get_context().get_world_size()
            self._train_throughput.add(global_throughput)

            self._train_batch_idx += 1

            if (
                self.benchmark_config.validate_every_n_steps
                and self._train_batch_idx % self.benchmark_config.validate_every_n_steps
                == 0
            ):
                self.validate_and_checkpoint()

            if self._train_batch_idx % 50 == 0:
                import pprint
                pprint.pprint(self.get_metrics())

            step_start_s = time.perf_counter()
            with self._timers["train/iter_batch"].timer():
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
        with self._timers["validation/epoch"].timer():
            validation_metrics = self.validate()

        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            with self._timers["checkpoint/save"].timer():
                self.save_checkpoint(temp_checkpoint_dir)

            with self._timers["checkpoint/report"].timer():
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

        validation_epoch_start = time.perf_counter()

        total_loss = torch.zeros(1)
        num_rows = 0

        with self._timers["validation/iter_first_batch"].timer():
            batch = self.get_next_batch(self.val_dataloader)

        while batch:
            input_batch, labels = batch

            with self._timers["validation/step"].timer():
                with torch.no_grad():
                    out = self.model(input_batch)
                    loss = self.loss_fn(out, labels)
                    total_loss += loss
                    num_rows += len(labels)

            with self._timers["validation/iter_batch"].timer():
                batch = self.get_next_batch(self.val_dataloader)

        validation_epoch_s = time.perf_counter() - validation_epoch_start
        local_throughput = num_rows / validation_epoch_s
        global_throughput = local_throughput * ray.train.get_context().get_world_rank()
        self._validation_throughput.add(global_throughput)

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

        # Timers
        for key, timer in self._timers.items():
            metrics.update({
                f"{key}-avg": timer.avg(),
                f"{key}-min": timer.min(),
                f"{key}-max": timer.max(),
                f"{key}-total": timer.get(),
            })

        # Throughput
        metrics["train/global_throughput"] = self._train_throughput.avg()
        metrics["validation/global_throughput"] = self._validation_throughput.avg()

        # Dataloader metrics (ex: Ray Data stats)
        metrics.update(self.factory.get_dataloader_metrics())

        return metrics


def train_fn_per_worker(config):
    factory = config["factory"]
    runner = TrainLoopRunner(factory)
    runner.run()


def parse_cli_args():
    parser = argparse.ArgumentParser()
    for field, field_info in BenchmarkConfig.model_fields.items():
        field_type = field_info.annotation
        assert field_type

        if field_type is bool:
            assert not field_info.default, "Only supports bool flags that are False by default."
            parser.add_argument(
                f"--{field}", action="store_true", default=field_info.default
            )
        else:
            parser.add_argument(
                f"--{field}", type=field_type, default=field_info.default
            )
    args = parser.parse_args()
    return BenchmarkConfig(**vars(args))


def main():
    benchmark_config = parse_cli_args()
    print(benchmark_config.model_dump_json(indent=2))

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


if __name__ == "__main__":
    main()
