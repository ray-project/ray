import random
import time
import numpy as np

import ray
from ray import tune
from ray.train import ScalingConfig, RunConfig
from ray.train.torch import TorchTrainer

import trackio

from trackio_ray_integration import (
    TrackioLoggerCallback,
    setup_trackio,
)


# -----------------------------------------------------------
# Configuration
# -----------------------------------------------------------

PROJECT_NAME = "trackio-ray-demo"

# Optional — set these if you want remote logging
HF_DATASET_ID = None  # e.g. "username/ray-trackio-experiments"
HF_SPACE_ID = None    # e.g. "username/ray-trackio-dashboard"


# -----------------------------------------------------------
# Ray Tune Example (Callback integration)
# -----------------------------------------------------------

def tune_trainable(config):

    for step in range(15):

        loss = (config["lr"] * 10) / (step + 1) + random.random()

        accuracy = 1 / (loss + 1e-3)

        # Example artifact: random image
        image = np.random.rand(64, 64, 3)

        tune.report(
            loss=loss,
            accuracy=accuracy,
            sample_image=image,
        )

        time.sleep(0.2)


def run_tune_example():

    tuner = tune.Tuner(
        tune_trainable,
        param_space={
            "lr": tune.grid_search([0.001, 0.01, 0.1]),
        },
        run_config=tune.RunConfig(
            name="trackio-ray-tune-demo",
            callbacks=[
                TrackioLoggerCallback(
                    project=PROJECT_NAME,

                    # Trackio advanced features
                    auto_log_gpu=True,
                    gpu_log_interval=5,

                    dataset_id=HF_DATASET_ID,
                    space_id=HF_SPACE_ID,
                )
            ],
        ),
    )

    results = tuner.fit()

    print("Tune finished:", results)


# -----------------------------------------------------------
# Ray Train Example (setup_trackio helper)
# -----------------------------------------------------------

def train_loop(config):

    run = setup_trackio(
        config=config,
        project=PROJECT_NAME,

        auto_log_gpu=True,
        gpu_log_interval=5,

        dataset_id=HF_DATASET_ID,
        space_id=HF_SPACE_ID,
    )

    for step in range(15):

        loss = 5 / (step + 1) + random.random()

        throughput = random.uniform(50, 150)

        trackio.log(
            {
                "loss": loss,
                "throughput": throughput,
            },
            step=step,
        )

        # Example artifact
        sample_image = np.random.rand(64, 64, 3)

        trackio.log(
            {
                "generated_image": sample_image
            }
        )

        time.sleep(0.2)

    if run:
        run.finish()


def run_train_example():

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config={"lr": 0.01},
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            name="trackio-ray-train-demo"
        ),
    )

    trainer.fit()


# -----------------------------------------------------------
# Dashboard Demo
# -----------------------------------------------------------

def launch_dashboard():

    print("\nLaunching Trackio dashboard...\n")

    trackio.show(
        project=PROJECT_NAME,
        open_browser=True,
    )


# -----------------------------------------------------------
# Main
# -----------------------------------------------------------

if __name__ == "__main__":

    ray.init()

    print("\nRunning Ray Tune experiment\n")
    run_tune_example()

    print("\nRunning Ray Train experiment\n")
    run_train_example()

    print("\nOpening dashboard\n")
    launch_dashboard()

    ray.shutdown()

    print("\nDemo completed\n")