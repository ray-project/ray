import argparse
import collections
import json
import os
import tempfile
import time
import uuid

import ray.data
import ray.train
from ray._private.test_utils import safe_write_to_results_json
from ray.data.checkpoint.data_iterator_checkpointer import RowIDBasedStateDict
from ray.data.datasource import PartitionStyle, PathPartitionFilter
from ray.train import DatasetCheckpointConfig
from ray.train.collective import barrier as ray_train_barrier
from ray.train.torch import TorchTrainer


def _max_reduce(val: float) -> float:
    import torch
    import torch.distributed as dist

    tensor = torch.tensor(val)
    dist.all_reduce(tensor, op=dist.ReduceOp.MAX)
    return tensor.item()


def train_fn_per_worker(config):
    total_dataset_rows = config["total_dataset_rows"]
    checkpoint_interval = config["checkpoint_interval"]
    batch_size = config.get("batch_size", 256)
    num_epochs = config.get("num_epochs", 1)
    error_at = config.get("error_at", [])

    num_restarts = 0
    start_epoch = 0
    global_rows_processed = 0
    global_epochs_processed = 0
    current_error_percent = float("inf")
    metrics_history = collections.defaultdict(list)

    checkpoint_load_start = time.perf_counter()
    checkpoint = ray.train.get_checkpoint()
    restored = bool(checkpoint)

    data_state_dict = None
    if restored:
        with checkpoint.as_directory() as temp_checkpoint_dir:
            with open(os.path.join(temp_checkpoint_dir, "train_state.json"), "r") as f:
                train_state = json.load(f)
                num_restarts = train_state["num_restarts"] + 1
                global_rows_processed = train_state["global_rows_processed"]
                metrics_history = train_state["history"]
                start_epoch = global_rows_processed // total_dataset_rows

            with open(os.path.join(temp_checkpoint_dir, "data_state.json"), "r") as f:
                data_state_dict = json.load(f)

            print(
                f"[Checkpoint restore] {num_restarts=} {global_rows_processed=} "
                f"{global_epochs_processed=} {start_epoch=} {data_state_dict=}"
            )

    ds_iter = ray.train.get_dataset_shard("train", state_dict=data_state_dict)

    checkpoint_load_duration = time.perf_counter() - checkpoint_load_start
    max_checkpoint_load_duration = _max_reduce(checkpoint_load_duration)
    metrics_history["checkpoint_load_duration"].append(max_checkpoint_load_duration)

    rank = ray.train.get_context().get_world_rank()
    if len(error_at) > num_restarts:
        current_error_percent = error_at[num_restarts]
        if rank == 0:
            print(
                f"[{num_restarts=}] Will inject an error at: {current_error_percent=}"
            )

    def save_checkpoint(data_state_dict):
        with tempfile.TemporaryDirectory() as tmpdir:
            if rank == 0:
                with open(os.path.join(tmpdir, "data_state.json"), "w") as f:
                    json.dump(data_state_dict, f)

                train_state = {
                    "num_restarts": num_restarts,
                    "global_rows_processed": global_rows_processed,
                    "history": metrics_history,
                }
                with open(os.path.join(tmpdir, "train_state.json"), "w") as f:
                    json.dump(train_state, f)

            ray.train.report(
                metrics={},
                checkpoint=ray.train.Checkpoint.from_directory(tmpdir),
            )
        ray_train_barrier()

    for epoch in range(start_epoch, num_epochs):
        print(f"{epoch=}")
        batch_iter = ds_iter.iter_batches(batch_size=batch_size, prefetch_batches=1)

        first_batch_start = time.perf_counter()
        for i, batch in enumerate(batch_iter):
            if i == 0:
                time_to_first_batch = time.perf_counter() - first_batch_start
                print(f"[{num_restarts=}] {time_to_first_batch=}")

                max_time_to_first_batch = _max_reduce(time_to_first_batch)
                metrics_history["time_to_first_batch"].append(max_time_to_first_batch)

            global_rows_processed += (
                len(batch["id"]) * ray.train.get_context().get_world_size()
            )
            global_epochs_processed = global_rows_processed / total_dataset_rows

            if (i + 1) % checkpoint_interval == 0:
                state_dict = ds_iter.state_dict()
                if rank == 0:
                    print(
                        f"[Mid-epoch checkpoint] {global_epochs_processed=:.4f}, {global_rows_processed=}, {state_dict=}"
                    )
                save_checkpoint(state_dict)

            # Inject an end-of-epoch error after the end of epoch checkpoint instead.
            at_epoch_end = global_rows_processed % total_dataset_rows == 0
            if global_epochs_processed >= current_error_percent and not at_epoch_end:
                raise RuntimeError(
                    f"[Mid-epoch error] {global_epochs_processed=:.4f}, {global_rows_processed=}"
                )

        state_dict = ds_iter.state_dict()
        if rank == 0:
            print(
                f"[End-of-epoch checkpoint] {global_epochs_processed=:.4f}, {global_rows_processed=}, {state_dict=}"
            )
        save_checkpoint(state_dict)

        # Each epoch should have processed all dataset rows.
        assert (
            global_rows_processed % total_dataset_rows == 0
        ), f"Should have processed all dataset rows exactly once per epoch: {global_rows_processed=}, {total_dataset_rows=}"

        if global_epochs_processed == current_error_percent:
            raise RuntimeError(
                f"[End-of-epoch error] {global_epochs_processed=:.4f}, {global_rows_processed=}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_rows", type=str, default="10M", choices=["10M", "3B"])
    parser.add_argument("--num_epochs", type=int, default=2)
    parser.add_argument("--disable_data_checkpointing", action="store_true")
    parser.add_argument("--generate_id_column", action="store_true")
    parser.add_argument(
        "--error_at", type=lambda lst: [float(s) for s in lst.split(",")], default=[]
    )
    parser.add_argument("--use_s3", action="store_true")
    args = parser.parse_args()

    name_prefix = args.num_rows

    config = {
        "batch_size": 256,
        "num_epochs": args.num_epochs,
        "error_at": args.error_at,
    }
    config.update(
        {
            "10M": {"total_dataset_rows": int(10e6), "checkpoint_interval": 100},
            "3B": {"total_dataset_rows": int(3e9), "checkpoint_interval": 10_000},
        }[args.num_rows]
    )
    id_column = "generated_id" if args.generate_id_column else "id"
    storage_path = (
        "/mnt/cluster_storage"
        if not args.use_s3
        else os.environ["ANYSCALE_ARTIFACT_STORAGE"]
        + "/train_release_tests/mid_epoch_resume_benchmark"
    )
    run_name = name_prefix + "_" + uuid.uuid4().hex[:6]

    print(f"\n[{run_name}] {config=}\n")

    data_path_root = (
        "s3://ray-benchmark-data-internal-us-west-2/ray-data/checkpoint-benchmark"
    )
    data_path = (
        f"{data_path_root}/10M-rows"
        if args.num_rows == "10M"
        else f"{data_path_root}/3B-rows"
    )

    time_start = time.perf_counter()
    train_ds = ray.data.read_parquet(
        data_path,
        # TODO: check to see if this is still necessary to prevent OOMs
        ray_remote_args={"memory": 8 * 1024**3},
    )

    trainer = TorchTrainer(
        train_fn_per_worker,
        train_loop_config=config,
        scaling_config=ray.train.ScalingConfig(
            num_workers=16, resources_per_worker={"MOCK_GPU": 1}
        ),
        run_config=ray.train.RunConfig(
            storage_path=storage_path,
            name=run_name,
            failure_config=ray.train.FailureConfig(max_failures=len(args.error_at)),
        ),
        datasets={"train": train_ds},
        dataset_config=ray.train.DataConfig(
            dataset_checkpoint_configs={
                "train": DatasetCheckpointConfig(
                    id_column=id_column,
                    generate_id_column=args.generate_id_column,
                    # Do not delete checkpoints after each epoch, since we want to
                    # inspect the checkpoint contents for correctness.
                    delete_checkpoints_after_epoch=False,
                ),
            },
        ),
    )
    result = trainer.fit()
    time_end = time.perf_counter()
    run_duration = time_end - time_start
    print(f"[{run_name}] Total training time: {run_duration}")

    print("\nChecking checkpoint contents for correctness...\n")

    # Check correctness of checkpoint contents:
    # 1. Each epoch should have checkpointed all dataset rows.
    # 2. Checkpointed row IDs should be unique.
    data_checkpoint_path = f"{storage_path}/{run_name}/ray_data_checkpoints"

    for epoch in range(args.num_epochs):
        partition_filter = PathPartitionFilter.of(
            filter_fn=lambda partitioned_path: (
                int(partitioned_path[RowIDBasedStateDict.EPOCH_PATH_KEY]) == epoch
            ),
            style=PartitionStyle.HIVE,
        )
        ds = ray.data.read_parquet(
            data_checkpoint_path, partition_filter=partition_filter
        )
        checkpoint_row_count = ds.count()
        assert (
            checkpoint_row_count == config["total_dataset_rows"]
        ), f"Expected {config['total_dataset_rows']} rows checkpointed for {epoch=}, got {checkpoint_row_count}"
        print(f"[{run_name}] {epoch=} {checkpoint_row_count=}")

    checkpoint = result.checkpoint
    with checkpoint.as_directory() as temp_checkpoint_dir:
        with open(os.path.join(temp_checkpoint_dir, "train_state.json"), "r") as f:
            train_state = json.load(f)
            history = train_state["history"]
            checkpoint_load_duration = history["checkpoint_load_duration"]
            time_to_first_batch = history["time_to_first_batch"]

    time_to_first_batch_without_checkpoint = time_to_first_batch[0]
    time_to_first_batch_with_checkpoint = -1
    if len(time_to_first_batch) >= 2:
        time_to_first_batch_with_checkpoint = time_to_first_batch[1]

    results = {
        "duration": run_duration,
        # Baseline metric, which measures time to first batch from a fresh start.
        "time_to_first_batch_without_checkpoint": time_to_first_batch_without_checkpoint,
        # Time to first batch from a checkpointed start, which includes the time
        # to load the data checkpoint.
        "time_to_first_batch_with_checkpoint": time_to_first_batch_with_checkpoint,
        "time_to_first_batch_history": time_to_first_batch,
        "checkpoint_load_duration_history": checkpoint_load_duration,
        "error_at": args.error_at,
        "num_rows": config["total_dataset_rows"],
    }
    safe_write_to_results_json(results)

    print(f"\n[{run_name}] {results=}\n")
