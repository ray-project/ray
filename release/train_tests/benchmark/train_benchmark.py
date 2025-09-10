import json
import logging
import os
import pprint
import time

import ray.train
from ray._private.test_utils import safe_write_to_results_json
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.util import date_str

from config import BenchmarkConfig, cli_to_config
from benchmark_factory import BenchmarkFactory
from ray_dataloader_factory import RayDataLoaderFactory


logger = logging.getLogger(__name__)


METRICS_OUTPUT_PATH = "/mnt/cluster_storage/train_benchmark_metrics.json"


def train_fn_per_worker(config):
    factory: BenchmarkFactory = config["factory"]

    if factory.benchmark_config.task == "recsys":
        from recsys.torchrec_runner import TorchRecRunner

        runner = TorchRecRunner(factory)
    else:
        from runner import VanillaTorchRunner

        runner = VanillaTorchRunner(factory)

    runner.run()

    metrics = runner.get_metrics(
        dataset_creation_time=config.get("dataset_creation_time", 0)
    )
    if ray.train.get_context().get_world_rank() == 0:
        with open(METRICS_OUTPUT_PATH, "w") as f:
            json.dump(metrics, f)


def get_datasets_and_data_config(factory: BenchmarkFactory):
    dataloader_factory = factory.get_dataloader_factory()
    if isinstance(dataloader_factory, RayDataLoaderFactory):
        datasets = dataloader_factory.get_ray_datasets()
        data_config = dataloader_factory.get_ray_data_config()
    else:
        datasets = {}
        data_config = None

    return datasets, data_config


def main():
    start_time = time.perf_counter()
    logging.basicConfig(level=logging.INFO)

    benchmark_config: BenchmarkConfig = cli_to_config()
    logger.info(
        "\nBenchmark config:\n" + pprint.pformat(benchmark_config.__dict__, indent=2)
    )

    if benchmark_config.task == "image_classification":
        from image_classification.factory import ImageClassificationFactory

        factory = ImageClassificationFactory(benchmark_config)
    elif benchmark_config.task == "recsys":
        from recsys.recsys_factory import RecsysFactory

        factory = RecsysFactory(benchmark_config)
    else:
        raise ValueError(f"Unknown task: {benchmark_config.task}")

    datasets, data_config = get_datasets_and_data_config(factory)

    dataset_creation_time = time.perf_counter() - start_time

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config={
            "factory": factory,
            "dataset_creation_time": dataset_creation_time,
        },
        scaling_config=ray.train.ScalingConfig(
            num_workers=benchmark_config.num_workers,
            use_gpu=not benchmark_config.mock_gpu,
            resources_per_worker={"MOCK_GPU": 1} if benchmark_config.mock_gpu else None,
        ),
        run_config=ray.train.RunConfig(
            storage_path=f"{os.environ['ANYSCALE_ARTIFACT_STORAGE']}/train_benchmark/",
            name=f"{benchmark_config.task}-{date_str(include_ms=True)}",
            failure_config=ray.train.FailureConfig(
                max_failures=benchmark_config.max_failures
            ),
        ),
        datasets=datasets,
        dataset_config=data_config,
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
