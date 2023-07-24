import tensorflow as tf
import numpy as np
import os
import pandas as pd
import time
import logging
import csv
import json
import torchvision
import torch

import ray
from ray.air import session
from ray.train.tensorflow import prepare_dataset_shard, TensorflowTrainer
from ray.air.config import ScalingConfig
from ray.train import DataConfig
from ray import tune
from ray.tune import Tuner
from ray.data.datasource.partitioning import Partitioning


from tf_utils import (
    DEFAULT_IMAGE_SIZE,
    NUM_CHANNELS,
    preprocess_image,
    build_tf_dataset,
)

from metric_utils import (
    determine_if_memory_monitor_is_enabled_in_latest_session,
    get_ray_spilled_and_restored_mb,
    MaxMemoryUtilizationTracker,
)

IMAGE_DIMS = (None, DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE, NUM_CHANNELS)

ONE_HOT = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data loader options.
# Use tf.data preprocessor provided by MLPerf reference implementation.
TF_DATA = "tf.data"
# Use a single empty data batch, repeated.
SYNTHETIC = "synthetic"
# Use Ray Datasets.
RAY_DATA = "ray.data"
# torch dataloader.
TORCH_DATALOADER = "torch"

# Each image is about 600KB after preprocessing.
APPROX_PREPROCESS_IMAGE_BYTES = 6 * 1e5


def build_model():
    return tf.keras.applications.resnet50.ResNet50(
        weights=None,
        # input_tensor=None,
        # input_shape=None,
        # pooling=None,
        # classes=1000,
    )


def print_dataset_stats(ds):
    print("")
    print("====Dataset stats====")
    print(ds.stats())
    print("")


def train_loop_for_worker(config):
    ray.data.DataContext.get_current().execution_options.verbose_progress = True

    epoch_times = []
    throughputs = []
    if config["train_sleep_time_ms"] >= 0:
        model = None
    else:
        strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
        with strategy.scope():
            model = build_model()
            # model.compile(optimizer="rmsprop", loss="sparse_categorical_crossentropy")
            model.compile(optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

    dataset_shard = session.get_dataset_shard("train")
    _tf_dataset = None
    synthetic_dataset = None
    if config["data_loader"] == TF_DATA:
        assert dataset_shard is None
        logger.info("Building tf.dataset...")
        filenames = get_tfrecords_filenames(
            config["data_root"],
            config["num_images_per_epoch"],
            config["num_images_per_input_file"],
        )
        _tf_dataset = build_tf_dataset(
            filenames,
            config["batch_size"],
            config["num_images_per_epoch"],
            config["num_epochs"],
            shuffle_buffer=config["shuffle_buffer_size"],
        )
    elif config["data_loader"] == SYNTHETIC:
        # Build an empty batch and repeat it.
        synthetic_dataset = build_synthetic_dataset(config["batch_size"])
    # TODO(swang): We should use the TorchTrainer and iter_torch_batches to
    # compare properly against TORCH_DATALOADER.
    # elif config["data_loader"] == TORCH_DATALOADER:
    #     assert dataset_shard is None
    #     logger.info("Building torch.DataLoader...")
    #     # TODO(swang): pass in shuffle buffer size.
    #     # NOTE(swang): There is no way to .limit() the number of images read
    #     # for torch.
    #     torch_dataset = build_torch_dataset(
    #         config["data_root"],
    #         config["batch_size"],
    #     )

    def build_synthetic_tf_dataset(dataset, batch_size, num_steps_per_epoch):
        batch = list(dataset.iter_tf_batches(batch_size=batch_size, dtypes=tf.float32))[
            0
        ]
        batch = (batch["image"], batch["label"])

        # TODO(swang): Might generate a few more records than expected if
        # batches don't divide evenly into num_images_per_epoch.
        def to_tensor_iterator():
            for _ in range(num_steps_per_epoch):
                yield batch

        output_signature = (
            tf.TensorSpec(shape=IMAGE_DIMS, dtype=tf.uint8),
            tf.TensorSpec(shape=(None,), dtype=tf.int32),
        )
        tf_dataset = tf.data.Dataset.from_generator(
            to_tensor_iterator, output_signature=output_signature
        )
        return prepare_dataset_shard(tf_dataset)

    num_steps_per_epoch = config["num_images_per_epoch"] // config["batch_size"]
    if config["num_images_per_epoch"] % config["batch_size"]:
        # Assuming batches will respect epoch boundaries.
        num_steps_per_epoch += 1

    for epoch in range(config["num_epochs"]):
        tf_dataset = None
        if config["data_loader"] == TF_DATA:
            assert _tf_dataset is not None
            tf_dataset = _tf_dataset
        elif config["data_loader"] == RAY_DATA:
            assert dataset_shard is not None
            tf_dataset = dataset_shard.to_tf(
                feature_columns="image",
                label_columns="label",
                batch_size=config["batch_size"],
            )
        elif config["data_loader"] == SYNTHETIC:
            tf_dataset = build_synthetic_tf_dataset(
                synthetic_dataset,
                batch_size=config["batch_size"],
                num_steps_per_epoch=num_steps_per_epoch,
            )

        epoch_start_time_s = time.perf_counter()

        if model:
            model.fit(tf_dataset, steps_per_epoch=num_steps_per_epoch)
        else:
            num_rows_read = 0
            for i, batch in enumerate(tf_dataset):
                num_rows_read += len(batch[0])
                if i >= num_steps_per_epoch:
                    break
                time.sleep(config["train_sleep_time_ms"] / 1000)
                if i % 10 == 0:
                    print("Step", i)

            assert num_rows_read >= config["num_images_per_epoch"], (
                num_rows_read,
                config["num_images_per_epoch"],
            )

        epoch_time_s = time.perf_counter() - epoch_start_time_s
        epoch_times.append(epoch_time_s)
        throughputs.append(config["num_images_per_epoch"] / epoch_time_s)

        total_tput = config["num_images_per_epoch"] / epoch_time_s
        # Drop the first epoch to remove warmup time.
        if len(epoch_times) > 1:
            total_tput = (epoch) * config["num_images_per_epoch"] / sum(epoch_times[1:])
        logger.info(
            "Epoch time: {epoch_time_s}s, images/s: {throughput}".format(
                epoch_time_s=epoch_time_s,
                throughput=config["num_images_per_epoch"] / epoch_time_s,
            )
        )

        session.report(
            {
                "all_epoch_times_s": epoch_times,
                "all_throughputs_imgs_s": throughputs,
                "tput_images_per_s": total_tput,
            }
        )

        if config["data_loader"] == RAY_DATA:
            print_dataset_stats(dataset_shard)
            print("epoch time", epoch, epoch_time_s)


def crop_and_flip_image_batch(image_batch):
    transform = torchvision.transforms.Compose(
        [
            torchvision.transforms.RandomResizedCrop(
                size=DEFAULT_IMAGE_SIZE,
                scale=(0.05, 1.0),
                ratio=(0.75, 1.33),
            ),
            torchvision.transforms.RandomHorizontalFlip(),
        ]
    )
    batch_size, height, width, channels = image_batch["image"].shape
    tensor_shape = (batch_size, channels, height, width)
    image_batch["image"] = transform(
        torch.Tensor(image_batch["image"].reshape(tensor_shape))
    )
    return image_batch


def decode_tf_record_batch(tf_record_batch: pd.DataFrame) -> pd.DataFrame:
    def process_images():
        for image_buffer in tf_record_batch["image/encoded"]:
            image_buffer = tf.reshape(image_buffer, shape=[])
            image_buffer = tf.io.decode_jpeg(image_buffer, channels=NUM_CHANNELS)
            yield image_buffer

    # Subtract one so that labels are in [0, 1000), and cast to float32 for
    # Keras model.
    # TODO(swang): Do we need to support one-hot encoding?
    labels = (tf_record_batch["image/class/label"] - 1).astype("float32")
    df = pd.DataFrame.from_dict({"image": process_images(), "label": labels})

    return df


def decode_crop_and_flip_tf_record_batch(tf_record_batch: pd.DataFrame) -> pd.DataFrame:
    """
    This version of the preprocessor fuses the load step with the crop and flip
    step, which should have better performance (at the cost of re-executing the
    load step on each epoch):
    - the reference tf.data implementation can use the fused decode_and_crop op
    - ray.data doesn't have to materialize the intermediate decoded batch.
    """

    def process_images():
        for image_buffer in tf_record_batch["image/encoded"]:
            # Each image output is ~600KB.
            yield preprocess_image(
                image_buffer=image_buffer,
                output_height=DEFAULT_IMAGE_SIZE,
                output_width=DEFAULT_IMAGE_SIZE,
                num_channels=NUM_CHANNELS,
                # TODO(swang): Also load validation set.
                is_training=True,
            ).numpy()

    # Subtract one so that labels are in [0, 1000), and cast to float32 for
    # Keras model.
    # TODO(swang): Do we need to support one-hot encoding?
    labels = (tf_record_batch["image/class/label"] - 1).astype("float32")
    df = pd.DataFrame.from_dict({"image": process_images(), "label": labels})

    return df


def build_synthetic_dataset(batch_size):
    image_dims = IMAGE_DIMS[1:]
    empty = np.empty(image_dims, dtype=np.uint8)
    ds = ray.data.from_items(
        [{"image": empty, "label": 1} for _ in range(int(batch_size))],
        parallelism=1,
    )
    return ds


def get_tfrecords_filenames(data_root, num_images_per_epoch, num_images_per_input_file):
    num_files = num_images_per_epoch // num_images_per_input_file
    if num_images_per_epoch % num_images_per_input_file:
        num_files += 1
    filenames = [
        os.path.join(data_root, filename) for filename in os.listdir(data_root)
    ][:num_files]
    assert (
        len(filenames) == num_files
    ), f"Need {num_files} input files, only found {len(filenames)}"
    return filenames


def build_dataset(
    data_root,
    num_images_per_epoch,
    num_images_per_input_file,
    batch_size,
    read_from_images=True,
):
    if read_from_images:
        ds = ray.data.read_images(
            data_root,
            # Use the same partitioning required by torch dataloader.
            # root_dir
            #   class_name1
            #     XXX.jpg
            #   class_name2
            #     YYY.jpg
            partitioning=Partitioning("dir", field_names=["label"], base_dir="~/data"),
        )

        classes = {label: i for i, label in enumerate(ds.unique("label"))}

        def convert_class_to_idx(df, classes):
            df["label"] = df["label"].map(classes).astype("float32")
            return df

        ds = ds.map_batches(
            convert_class_to_idx,
            fn_kwargs={"classes": classes},
        )
        ds = ds.map_batches(
            crop_and_flip_image_batch,
            zero_copy_batch=True,
        )
    else:
        filenames = get_tfrecords_filenames(
            data_root, num_images_per_epoch, num_images_per_input_file
        )
        ds = ray.data.read_tfrecords(filenames)
        ds = ds.map_batches(
            decode_crop_and_flip_tf_record_batch,
            batch_size=batch_size,
            batch_format="pandas",
        )

    ds = ds.limit(num_images_per_epoch)
    return ds


FIELDS = [
    "data_loader",
    "train_sleep_time_ms",
    "num_cpu_nodes",
    "num_epochs",
    "num_images_per_epoch",
    "num_images_per_input_file",
    "num_files",
    "batch_size",
    "shuffle_buffer_size",
    "ray_mem_monitor_enabled",
    "ray_spilled_mb",
    "ray_restored_mb",
    "min_available_mb",
    "time_total_s",
    "tput_images_per_s",
    "all_epoch_times_s",
    "all_throughputs_imgs_s",
]


def write_metrics(data_loader, command_args, metrics, output_file):
    print(metrics)
    assert "tput_images_per_s" in metrics
    row = {key: val for key, val in metrics.items() if key in FIELDS}
    row["data_loader"] = data_loader
    for field in FIELDS:
        val = getattr(command_args, field, None)
        if val is not None:
            row[field] = val

    for field in FIELDS:
        print(f"{field}: {row[field]}")

    write_header = not os.path.exists(output_file)
    with open(output_file, "a+", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    test_output_json_envvar = "TEST_OUTPUT_JSON"
    test_output_json_path = os.environ.get(test_output_json_envvar)
    if not test_output_json_path:
        print(
            "Env var {env_var} not set, will not write test output json.".format(
                env_var=test_output_json_envvar
            )
        )
    else:
        print(
            "Env var {env_var} set to '{path}'. Will write test output json.".format(
                env_var=test_output_json_envvar, path=test_output_json_path
            )
        )
        append_to_test_output_json(test_output_json_path, row)


def append_to_test_output_json(path, metrics):

    output_json = {}
    try:
        with open(path, "r") as existing_test_output_file:
            output_json = json.load(existing_test_output_file)
    except FileNotFoundError:
        pass

    # Set success to be previous_success && current_success.
    success = output_json.get("success", "1")
    success = "1" if (success == "1") and (metrics["tput_images_per_s"] != -1) else "0"
    output_json["success"] = success

    # Append all metrics to an array of runs.
    runs = output_json.get("runs", [])
    runs.append(metrics)
    output_json["runs"] = runs

    num_images_per_file = metrics["num_images_per_input_file"]
    num_files = metrics["num_files"]
    data_loader = metrics["data_loader"]
    num_cpu_nodes = metrics["num_cpu_nodes"]

    # Append select performance metrics to perf_metrics.
    perf_metrics = output_json.get("perf_metrics", [])
    perf_metrics.append(
        {
            "perf_metric_name": f"{data_loader}_{num_images_per_file}-images-per-file_{num_files}-num-files-{num_cpu_nodes}-num-cpu-nodes_throughput-img-per-second",  # noqa: E501
            "perf_metric_value": metrics["tput_images_per_s"],
            "perf_metric_type": "THROUGHPUT",
        }
    )
    output_json["perf_metrics"] = perf_metrics

    with open(path, "w") as test_output_file:
        json.dump(output_json, test_output_file)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-root",
        default=None,
        type=str,
        help='Directory path with TFRecords. Filenames should start with "train".',
    )

    data_ingest_group = parser.add_mutually_exclusive_group(required=True)
    data_ingest_group.add_argument("--use-tf-data", action="store_true")
    data_ingest_group.add_argument("--use-ray-data", action="store_true")
    data_ingest_group.add_argument("--use-torch", action="store_true")
    data_ingest_group.add_argument("--synthetic-data", action="store_true")

    parser.add_argument(
        "--num-images-per-input-file",
        default=1,
        type=int,
        help=(
            "Estimated number of images per input TFRecord file. "
            "Used to determine how many files to load."
            "If you receive an error about too few rows, lower this value."
        ),
    )
    parser.add_argument("--num-images-per-epoch", default=100, type=int)
    parser.add_argument("--num-epochs", default=2, type=int)
    parser.add_argument("--batch-size", default=1, type=int)
    parser.add_argument(
        "--train-sleep-time-ms",
        default=-1,
        type=int,
        help="If set to >= 0, use an empty trainer that sleeps this many ms per batch.",
    )
    parser.add_argument(
        "--shuffle-buffer-size",
        default=0,
        type=int,
        help=(
            "Size of each Train worker's local shuffle buffer. "
            "Default value taken from MLPerf reference implementation."
        ),
    )
    parser.add_argument(
        "--trainer-resources-cpu",
        default=1,
        type=int,
        help=("CPU resources requested per AIR trainer instance. Defaults to 1."),
    )
    parser.add_argument(
        "--tune-trials",
        default=0,
        type=int,
        help=(
            "Number of Tune trials to run. Defaults to 0, "
            "which disables Tune and executes a Trainer instance directly."
        ),
    )
    parser.add_argument("--output-file", default="out.csv", type=str)
    parser.add_argument("--use-gpu", action="store_true")
    parser.add_argument("--num-cpu-nodes", default=0, type=int)
    parser.add_argument("--from-images", action="store_true")
    args = parser.parse_args()

    ray.init(
        runtime_env={
            "working_dir": os.path.dirname(__file__),
        }
    )

    if args.use_tf_data or args.use_ray_data or args.use_torch:
        assert (
            args.data_root is not None
        ), "--use-tf-data, --use-ray-data, and --use-torch require a --data-root directory for TFRecord files"  # noqa: E501
    elif args.synthetic_data:
        assert args.data_root is None, "--synthetic-data doesn't use --data-root"

    memory_utilization_tracker = MaxMemoryUtilizationTracker(sample_interval_s=1)
    memory_utilization_tracker.start()

    # Get the available space on the current filesystem.
    # We'll use this to check whether the job should throw an OutOfDiskError.
    statvfs = os.statvfs("/home")
    available_disk_space = statvfs.f_bavail * statvfs.f_frsize
    expected_disk_usage = args.num_images_per_epoch * APPROX_PREPROCESS_IMAGE_BYTES
    print(f"Available disk space: {available_disk_space / 1e9}GB")
    print(f"Expected disk usage: {expected_disk_usage/ 1e9}GB")
    disk_error_expected = expected_disk_usage > available_disk_space * 0.8

    datasets = {}
    train_loop_config = {
        "num_epochs": args.num_epochs,
        "batch_size": args.batch_size,
        "train_sleep_time_ms": args.train_sleep_time_ms,
        "data_root": args.data_root,
        "num_images_per_epoch": args.num_images_per_epoch,
        "num_images_per_input_file": args.num_images_per_input_file,
        "shuffle_buffer_size": None
        if args.shuffle_buffer_size == 0
        else args.shuffle_buffer_size,
    }

    options = DataConfig.default_ingest_options()

    if args.synthetic_data:
        logger.info("Using synthetic data loader...")
        train_loop_config["data_loader"] = SYNTHETIC
    else:
        if args.use_tf_data:
            logger.info("Using tf.data loader")
            train_loop_config["data_loader"] = TF_DATA
        elif args.use_torch:
            logger.info("Using torch Dataloader")
            preprocessor = None
            train_loop_config["data_loader"] = TORCH_DATALOADER
        else:
            logger.info("Using Ray Datasets loader")

            # Enable block splitting to support larger file sizes w/o OOM.
            ctx = ray.data.context.DataContext.get_current()
            ctx.block_splitting_enabled = True

            options.resource_limits.object_store_memory = 10e9

            datasets["train"] = build_dataset(
                args.data_root,
                args.num_images_per_epoch,
                args.num_images_per_input_file,
                args.batch_size,
                args.from_images,
            )
            train_loop_config["data_loader"] = RAY_DATA

    trainer = TensorflowTrainer(
        train_loop_for_worker,
        scaling_config=ScalingConfig(
            num_workers=1,
            use_gpu=args.use_gpu,
            trainer_resources={"CPU": args.trainer_resources_cpu},
        ),
        datasets=datasets,
        dataset_config=ray.train.DataConfig(
            execution_options=options,
        ),
        train_loop_config=train_loop_config,
    )

    tuner = None
    if args.tune_trials > 0:
        tuner = Tuner(
            trainer,
            param_space={
                "train_loop_config": {
                    "random_var": tune.grid_search(range(1, args.tune_trials + 1))
                }
            },
            tune_config=tune.TuneConfig(
                metric="time_total_s", mode="max", num_samples=1
            ),
        )

    result = {}
    exc = None
    start_time_s = time.perf_counter()
    ray_spill_stats_start = get_ray_spilled_and_restored_mb()
    try:
        if tuner:
            result_grid = tuner.fit()
            result = result_grid.get_best_result()
        else:
            result = trainer.fit()
        result = result.metrics
    except Exception as e:
        exc = e

    if exc is not None:
        result["tput_images_per_s"] = -1
        result["time_total_s"] = time.perf_counter() - start_time_s

    result["ray_spilled_mb"], result["ray_restored_mb"] = tuple(
        end - start
        for start, end in zip(ray_spill_stats_start, get_ray_spilled_and_restored_mb())
    )
    result["min_available_mb"] = memory_utilization_tracker.stop() / (1 << 20)
    result[
        "ray_mem_monitor_enabled"
    ] = determine_if_memory_monitor_is_enabled_in_latest_session()

    if args.from_images:
        result["num_files"] = args.num_images_per_epoch
        result["num_images_per_input_file"] = 1
    else:
        result["num_files"] = len(
            get_tfrecords_filenames(
                train_loop_config["data_root"],
                train_loop_config["num_images_per_epoch"],
                train_loop_config["num_images_per_input_file"],
            )
        )

    try:
        write_metrics(train_loop_config["data_loader"], args, result, args.output_file)
    except OSError:
        if not disk_error_expected:
            raise

    if exc is not None:
        print(f"Raised exception: {exc}")
        if not disk_error_expected:
            raise exc
        else:
            # There is no way to get the error cause from the TuneError
            # returned by AIR, so it's possible that it raised an error other
            # than OutOfDiskError here.
            pass

    ray.timeline("timeline.json")
