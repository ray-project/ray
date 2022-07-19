import json
import os
import socket
import time
from contextlib import closing

import click
import numpy as np
import tensorflow as tf
from typing import List, Tuple

CONFIG = {"lr": 1e-3, "batch_size": 64}
VANILLA_RESULT_JSON = "/tmp/vanilla_out.json"


def mnist_dataset(batch_size: int) -> tf.data.Dataset:
    (x_train, y_train), _ = tf.keras.datasets.fashion_mnist.load_data()
    # The `x` arrays are in uint8 and have values in the [0, 255] range.
    # You need to convert them to float32 with values in the [0, 1] range.
    x_train = x_train / np.float32(255)
    y_train = y_train.astype(np.int64)
    train_dataset = (
        tf.data.Dataset.from_tensor_slices((x_train, y_train))
        .shuffle(60000, seed=1234)
        .batch(batch_size)
    )
    return train_dataset


def build_cnn_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.Input(shape=(28, 28)),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(512, activation="relu"),
            tf.keras.layers.Dense(512, activation="relu"),
            tf.keras.layers.Dense(10),
        ]
    )
    return model


def train_func(use_ray: bool, config: dict):
    if use_ray:
        from ray.air.callbacks.keras import Callback as TrainCheckpointReportCallback

        callbacks = [TrainCheckpointReportCallback(frequency=0)]
    else:
        callbacks = []

    per_worker_batch_size = config.get("batch_size", 64)
    epochs = config.get("epochs", 3)
    steps_per_epoch = config.get("steps_per_epoch", None)
    learning_rate = config.get("lr", 0.001)

    tf_config = json.loads(os.environ["TF_CONFIG"])
    num_workers = len(tf_config["cluster"]["worker"])
    local_rank = tf_config["task"]["index"]

    strategy = tf.distribute.MultiWorkerMirroredStrategy()

    global_batch_size = per_worker_batch_size * num_workers
    multi_worker_dataset = mnist_dataset(global_batch_size)

    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_cnn_model()
        multi_worker_model.compile(
            loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
            optimizer=tf.keras.optimizers.SGD(learning_rate=learning_rate),
            metrics=["accuracy"],
        )

    history = multi_worker_model.fit(
        multi_worker_dataset,
        epochs=epochs,
        steps_per_epoch=steps_per_epoch,
        callbacks=callbacks,
    )
    results = history.history
    loss = results["loss"][-1]

    if not use_ray:
        print(f"Reporting loss: {loss:.4f}")
        if local_rank == 0:
            with open(VANILLA_RESULT_JSON, "w") as f:
                json.dump({"loss": loss}, f)

    return results


def train_tf_ray_air(
    *,
    config: dict,
    num_workers: int = 4,
    cpus_per_worker: int = 8,
    use_gpu: bool = False,
) -> Tuple[float, float]:
    # This function is kicked off by the main() function and runs a full training
    # run using Ray AIR.
    from ray.train.tensorflow import TensorflowTrainer
    from ray.air.config import ScalingConfig

    def train_loop(config):
        train_func(use_ray=True, config=config)

    start_time = time.monotonic()
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config=config,
        scaling_config=ScalingConfig(
            trainer_resources={"CPU": 0},
            num_workers=num_workers,
            resources_per_worker={"CPU": cpus_per_worker},
            use_gpu=use_gpu,
        ),
    )
    result = trainer.fit()
    time_taken = time.monotonic() - start_time

    print(f"Last result: {result.metrics}")
    return time_taken, result.metrics["loss"]


def train_tf_vanilla_worker(
    *,
    config: dict,
    rank: int,
    world_size: int,
    worker_ip_port_list: List[str],
    use_gpu: bool = False,
):
    # This function is kicked off by the main() function and runs the vanilla
    # training script on a single worker.
    assert world_size == len(worker_ip_port_list)

    tf_config = {
        "cluster": {"worker": worker_ip_port_list},
        "task": {"type": "worker", "index": rank},
    }
    os.environ["TF_CONFIG"] = json.dumps(tf_config)

    train_func(use_ray=False, config=config)


def train_tf_vanilla(
    *,
    config: dict,
    num_workers: int = 4,
    cpus_per_worker: int = 8,
    use_gpu: bool = False,
) -> Tuple[float, float]:
    # This function is kicked off by the main() function and subsequently kicks
    # off tasks that run train_tf_vanilla_worker() on the worker nodes.
    import ray
    from benchmark_util import (
        upload_file_to_all_nodes,
        create_actors_with_resources,
        run_commands_on_actors,
        run_fn_on_actors,
    )

    path = os.path.abspath(__file__)
    upload_file_to_all_nodes(path)

    num_epochs = config["epochs"]

    actors = create_actors_with_resources(
        num_actors=num_workers,
        resources={
            "CPU": cpus_per_worker,
            "GPU": int(use_gpu),
        },
    )

    def get_ip_port():
        ip = ray.util.get_node_ip_address()
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("localhost", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            port = s.getsockname()[1]
        return ip, port

    ips_ports = run_fn_on_actors(actors=actors, fn=get_ip_port)
    ip_port_list = [f"{ip}:{port}" for ip, port in ips_ports]
    ip_port_str = ",".join(ip_port_list)

    cmds = [
        [
            "python",
            path,
            "worker",
            "--num-epochs",
            str(num_epochs),
            "--num-workers",
            str(num_workers),
            "--rank",
            str(rank),
            "--worker-ip-ports",
            ip_port_str,
        ]
        + (["--use-gpu"] if use_gpu else [])
        for rank in range(num_workers)
    ]

    start_time = time.monotonic()
    run_commands_on_actors(actors=actors, cmds=cmds)
    time_taken = time.monotonic() - start_time

    loss = 0.0
    if os.path.exists(VANILLA_RESULT_JSON):
        with open(VANILLA_RESULT_JSON, "r") as f:
            result = json.load(f)
        loss = result["loss"]

    return time_taken, loss


@click.group(help="Run Tensorflow benchmarks")
def cli():
    pass


@cli.command(help="Kick off Ray and vanilla benchmarks")
@click.option("--num-runs", type=int, default=1)
@click.option("--num-epochs", type=int, default=4)
@click.option("--num-workers", type=int, default=4)
@click.option("--cpus-per-worker", type=int, default=8)
@click.option("--use-gpu", is_flag=True, default=False)
def run(
    num_runs: int = 1,
    num_epochs: int = 4,
    num_workers: int = 4,
    cpus_per_worker: int = 8,
    use_gpu: bool = False,
):
    import ray
    from benchmark_util import upload_file_to_all_nodes, run_command_on_all_nodes

    config = CONFIG.copy()
    config["epochs"] = num_epochs

    ray.init("auto")
    print("Preparing Tensorflow benchmark: Downloading MNIST")

    path = os.path.abspath("workloads/_tensorflow_prepare.py")
    upload_file_to_all_nodes(path)
    run_command_on_all_nodes(["python", path])

    times_ray = []
    losses_ray = []
    times_vanilla = []
    losses_vanilla = []
    for run in range(1, num_runs + 1):
        print(f"[Run {run}/{num_runs}] Running Tensorflow Ray benchmark")

        time_ray, loss_ray = train_tf_ray_air(
            num_workers=num_workers,
            cpus_per_worker=cpus_per_worker,
            use_gpu=use_gpu,
            config=config,
        )

        print(
            f"[Run {run}/{num_runs}] Finished Ray training ({num_epochs} epochs) in "
            f"{time_ray:.2f} seconds. Observed loss = {loss_ray:.4f}"
        )

        time.sleep(5)

        print(f"[Run {run}/{num_runs}] Running Tensorflow vanilla benchmark")

        time_vanilla, loss_vanilla = train_tf_vanilla(
            num_workers=num_workers,
            cpus_per_worker=cpus_per_worker,
            use_gpu=use_gpu,
            config=config,
        )

        print(
            f"[Run {run}/{num_runs}] Finished vanilla training ({num_epochs} epochs) "
            f"in {time_vanilla:.2f} seconds. Observed loss = {loss_vanilla:.4f}"
        )

        print(
            f"[Run {run}/{num_runs}] Observed results: ",
            {
                "tensorflow_mnist_ray_time_s": time_ray,
                "tensorflow_mnist_ray_loss": loss_ray,
                "tensorflow_mnist_vanilla_time_s": time_vanilla,
                "tensorflow_mnist_vanilla_loss": loss_vanilla,
            },
        )

        times_ray.append(time_ray)
        losses_ray.append(loss_ray)
        times_vanilla.append(time_vanilla)
        losses_vanilla.append(loss_vanilla)

    times_ray_mean = np.mean(times_ray)
    times_ray_sd = np.std(times_ray)

    times_vanilla_mean = np.mean(times_vanilla)
    times_vanilla_sd = np.std(times_vanilla)

    result = {
        "tensorflow_mnist_ray_num_runs": num_runs,
        "tensorflow_mnist_ray_time_s_all": times_ray,
        "tensorflow_mnist_ray_time_s_mean": times_ray_mean,
        "tensorflow_mnist_ray_time_s_sd": times_ray_sd,
        "tensorflow_mnist_ray_loss_mean": np.mean(losses_ray),
        "tensorflow_mnist_ray_loss_sd": np.std(losses_ray),
        "tensorflow_mnist_vanilla_time_s_all": times_vanilla,
        "tensorflow_mnist_vanilla_time_s_mean": times_vanilla_mean,
        "tensorflow_mnist_vanilla_time_s_sd": times_vanilla_sd,
        "tensorflow_mnist_vanilla_loss_mean": np.mean(losses_vanilla),
        "tensorflow_mnist_vanilla_loss_std": np.std(losses_vanilla),
    }

    print("Results:", result)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    target_ratio = 1.15
    ratio = (times_ray_mean / times_vanilla_mean) if times_vanilla_mean != 0.0 else 1.0
    if ratio > 1.15:
        raise RuntimeError(
            f"Training on Ray took an average of {times_ray_mean:.2f} seconds, "
            f"which is more than {target_ratio:.2f}x of the average vanilla training "
            f"time of {times_vanilla_mean:.2f} seconds ({ratio:.2f}x). FAILED"
        )

    print(
        f"Training on Ray took an average of {times_ray_mean:.2f} seconds, "
        f"which is less than {target_ratio:.2f}x of the average vanilla training "
        f"time of {times_vanilla_mean:.2f} seconds ({ratio:.2f}x). PASSED"
    )


@cli.command(help="Run Tensorflow vanilla worker")
@click.option("--num-epochs", type=int, default=4)
@click.option("--num-workers", type=int, default=4)
@click.option("--rank", type=int, default=0)
@click.option("--worker-ip-ports", type=str, default="")
@click.option("--use-gpu", is_flag=True, default=False)
def worker(
    num_epochs: int = 4,
    num_workers: int = 4,
    rank: int = 0,
    worker_ip_ports: str = "",
    use_gpu: bool = False,
):
    config = CONFIG.copy()
    config["epochs"] = num_epochs

    # Parse worker ip ports
    worker_ip_port_list = worker_ip_ports.split(",")

    # Then we kick off the training function on every worker.
    return train_tf_vanilla_worker(
        config=config,
        rank=rank,
        world_size=num_workers,
        worker_ip_port_list=worker_ip_port_list,
        use_gpu=use_gpu,
    )


def main():
    return cli()


if __name__ == "__main__":
    main()
