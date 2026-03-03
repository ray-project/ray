"""Tune worker fault tolerance test.

This test checks if Tune worker fault tolerance works as expected. Worker fault
tolerance concerns the case where a worker node goes down (e.g. due to spot instance
preemption).
In this test, we start dummy trials that do nothing but sleep and checkpoint. We
also start a node killer actor, that has a chance to kill a random worker node
every N seconds.
The checkpoints are synced to S3.

If a trial is restored, it should restart from the last checkpointed iteration.

The test is successful if all trials finish with the expected number of iterations,
and that a checkpoint is always available when restoring.

This test only works on AWS as it uses AWS CLI to terminate nodes.

Test owner: Yard1 (Antoni)

"""

import os
import pickle
import argparse
import tempfile
import time
import random
import gc

import ray
from ray import tune
from ray.tune import Checkpoint, RunConfig, FailureConfig, CheckpointConfig
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner

from terminate_node_aws import create_instance_killer


MAX_ITERS = 40
ITER_TIME_BOUNDS = (60, 90)
WARMUP_TIME_S = 45


def objective(config):
    start_iteration = 0
    checkpoint = tune.get_checkpoint()
    # Ensure that after the node killer warmup time, we always have
    # a checkpoint to restore from.
    if (time.monotonic() - config["start_time"]) >= config["warmup_time_s"]:
        assert checkpoint
    checkpoint = tune.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "ckpt.pkl"), "rb") as f:
                checkpoint_dict = pickle.load(f)
            start_iteration = checkpoint_dict["iteration"] + 1

    for iteration in range(start_iteration, MAX_ITERS + 1):
        time.sleep(random.uniform(*ITER_TIME_BOUNDS))
        dct = {"iteration": iteration}
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "ckpt.pkl"), "wb") as f:
                pickle.dump(dct, f)
            tune.report(dct, checkpoint=Checkpoint.from_directory(tmpdir))


def main(bucket_uri: str):
    ray.init(log_to_driver=True, runtime_env={"working_dir": os.path.dirname(__file__)})
    num_samples = int(ray.cluster_resources()["CPU"])

    tuner = Tuner(
        objective,
        param_space={"start_time": time.monotonic(), "warmup_time_s": WARMUP_TIME_S},
        tune_config=TuneConfig(num_samples=num_samples, metric="iteration", mode="max"),
        run_config=RunConfig(
            verbose=2,
            failure_config=FailureConfig(max_failures=-1),
            storage_path=bucket_uri,
            checkpoint_config=CheckpointConfig(num_to_keep=2),
        ),
    )

    instance_killer = create_instance_killer(
        probability=0.03, time_between_checks_s=10, warmup_time_s=WARMUP_TIME_S
    )
    results = tuner.fit()
    print("Fitted:", results)

    del instance_killer
    print("Deleted instance killer")
    gc.collect()
    print("Collected garbage")

    for result in results:
        checkpoint = result.checkpoint
        with checkpoint.as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "ckpt.pkl"), "rb") as f:
                checkpoint_dict = pickle.load(f)

        assert checkpoint_dict["iteration"] == MAX_ITERS, (checkpoint_dict, MAX_ITERS)
        assert checkpoint_dict["iteration"] == result.metrics["iteration"], result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, help="Bucket URI")
    args, _ = parser.parse_known_args()

    main(args.bucket or "s3://tune-cloud-tests/worker_fault_tolerance")
    print("Finished test.")
