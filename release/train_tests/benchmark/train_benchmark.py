import json
import logging
import os
import pprint
import time

import ray.train
from ray._private.test_utils import safe_write_to_results_json
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.util import date_str
from ray.train.v2._internal.execution.callback import WorkerCallback
import torch

from config import BenchmarkConfig, cli_to_config
from factory import BenchmarkFactory


logger = logging.getLogger(__name__)


METRICS_OUTPUT_PATH = "/mnt/cluster_storage/train_benchmark_metrics.json"


class WorkerPreloadModules(WorkerCallback):
    """Preload modules on each worker."""

    def __init__(self):
        super().__init__()

    def after_init_train_context(self):
        """Called once on each worker before training starts."""
        mp = torch.multiprocessing.get_context("forkserver")
        mp.set_forkserver_preload(["torch", "torchvision"])
        logger.info("Preloaded torch/torchvision in forkserver context")


def train_fn_per_worker(config):
    factory: BenchmarkFactory = config["factory"]
    ray.train.report(
        {
            "dataset_creation_time": factory.dataset_creation_time,
        }
    )

    if factory.benchmark_config.task == "recsys":
        from recsys.torchrec_runner import TorchRecRunner

        runner = TorchRecRunner(factory)
    else:
        from runner import VanillaTorchRunner

        runner = VanillaTorchRunner(factory)

    runner.run()

    metrics = runner.get_metrics()
    if ray.train.get_context().get_world_rank() == 0:
        with open(METRICS_OUTPUT_PATH, "w") as f:
            json.dump(metrics, f)


def main():
    start_time = time.perf_counter()
    logging.basicConfig(level=logging.INFO)

    benchmark_config: BenchmarkConfig = cli_to_config()
    logger.info(
        "\nBenchmark config:\n" + pprint.pformat(benchmark_config.__dict__, indent=2)
    )

    if benchmark_config.task == "image_classification_parquet":
        from image_classification.image_classification_parquet.factory import (
            ImageClassificationParquetFactory,
        )

        factory = ImageClassificationParquetFactory(benchmark_config)
    elif benchmark_config.task == "image_classification_jpeg":
        from image_classification.image_classification_jpeg.factory import (
            ImageClassificationJpegFactory,
        )

        factory = ImageClassificationJpegFactory(benchmark_config)
    elif benchmark_config.task == "localfs_image_classification_jpeg":
        from image_classification.localfs_image_classification_jpeg.factory import (
            LocalFSImageClassificationFactory,
        )

        factory = LocalFSImageClassificationFactory(benchmark_config)
    elif benchmark_config.task == "recsys":
        from recsys.recsys_factory import RecsysFactory

        factory = RecsysFactory(benchmark_config)
    else:
        raise ValueError(f"Unknown task: {benchmark_config.task}")

    ray_data_execution_options = ray.train.DataConfig.default_ingest_options()
    ray_data_execution_options.locality_with_output = (
        benchmark_config.locality_with_output
    )
    ray_data_execution_options.actor_locality_enabled = (
        benchmark_config.actor_locality_enabled
    )

    factory.set_dataset_creation_time(time.perf_counter() - start_time)

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
            name=f"{benchmark_config.task}-{date_str(include_ms=True)}",
            failure_config=ray.train.FailureConfig(
                max_failures=benchmark_config.max_failures
            ),
            callbacks=[WorkerPreloadModules()],
        ),
        datasets=factory.get_ray_datasets(),
    )

    trainer.fit()

    end_time = time.perf_counter()

    with open(METRICS_OUTPUT_PATH, "r") as f:
        metrics = json.load(f)

    final_metrics_str = (
        f"\nTotal training time: {end_time - start_time} seconds\n"
        "Final metrics:\n" + "-" * 80 + "\n" + pprint.pformat(metrics) + "\n" + "-" * 80
    )
    logger.info(final_metrics_str)

    # Write metrics as a release test result.
    safe_write_to_results_json(metrics)


if __name__ == "__main__":
    # Workers need to access the working directory module.
    ray.init(runtime_env={"working_dir": os.path.dirname(__file__)})
    main()
