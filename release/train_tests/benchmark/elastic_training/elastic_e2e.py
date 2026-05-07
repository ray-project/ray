import json
import os
import pprint
import time

import ray
import ray.train
from ray._private.test_utils import safe_write_to_results_json
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.util import date_str

from config import cli_to_config
from image_classification.factory import ImageClassificationFactory
from elastic_training.resource_schedule import (
    MockResourceAvailabilityUpdater,
    ResourceAvailabilityEvent,
    generate_schedule,
)
from train_benchmark import (
    METRICS_OUTPUT_PATH,
    get_datasets_and_data_config,
    train_fn_per_worker,
)


def main():
    config = cli_to_config()
    print("\nBenchmark config:\n" + pprint.pformat(config.__dict__, indent=2))

    factory = ImageClassificationFactory(config)

    # Resolve num_workers based on min_workers and max_workers.
    if config.min_workers and config.max_workers:
        num_workers = (config.min_workers, config.max_workers)
    else:
        num_workers = config.num_workers

    updater_actor = ray.remote(num_cpus=0)(MockResourceAvailabilityUpdater).remote(
        resource_key="GPU"
    )
    ray.get(updater_actor.__ray_ready__.remote())

    interval_s = 60 * 5
    schedule = generate_schedule(
        resource_availability_options=[4, 8, 16, 32],
        duration_s=60 * 60,
        interval_s=interval_s,
        seed=777777,
    )
    # Make sure the run can finish at the end of the schedule.
    schedule.append(
        ResourceAvailabilityEvent(
            time_s=schedule[-1].time_s + interval_s, resource_units=32
        )
    )
    execute_schedule_fut = updater_actor.execute_schedule.remote(schedule)

    datasets, data_config = get_datasets_and_data_config(factory)

    start_time = time.perf_counter()
    trainer = TorchTrainer(
        train_loop_per_worker=train_fn_per_worker,
        train_loop_config={"factory": factory},
        scaling_config=ray.train.ScalingConfig(num_workers=num_workers, use_gpu=True),
        run_config=ray.train.RunConfig(
            storage_path=f"{os.environ['ANYSCALE_ARTIFACT_STORAGE']}/train_benchmark/",
            name=f"{config.task}-{date_str(include_ms=True)}",
            failure_config=ray.train.FailureConfig(max_failures=len(schedule)),
        ),
        datasets=datasets,
        dataset_config=data_config,
    )
    trainer.fit()
    end_time = time.perf_counter()
    e2e_time = end_time - start_time

    with open(METRICS_OUTPUT_PATH, "r") as f:
        metrics = json.load(f)

    # Includes recovery time across resource updates.
    total_rows_processed = metrics["train/rows_processed-total"]
    metrics["e2e_throughput"] = total_rows_processed / e2e_time
    metrics["e2e_time"] = e2e_time

    safe_write_to_results_json(metrics)

    final_metrics_str = (
        f"\nTotal training time: {e2e_time} seconds\n"
        + "Final metrics:\n"
        + "-" * 80
        + "\n"
        + pprint.pformat(metrics)
        + "\n"
        + "-" * 80
    )
    print(final_metrics_str)

    ray.get(execute_schedule_fut)
    ray.get(updater_actor.shutdown.remote())


if __name__ == "__main__":
    # Workers need to access the working directory module.
    ray.init(runtime_env={"working_dir": os.path.dirname(os.path.dirname(__file__))})
    main()
