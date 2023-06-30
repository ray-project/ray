from ray import tune
import glob
import shutil
import pytest
import time
from ray.air.config import RunConfig
from ray.air import session
from ray.train.checkpoint import Checkpoint
import os

NUM_MODELS = 2
RAY_RESULTS = os.path.expanduser("~/ray_results")
FAKE_NFS_DIR = "/tmp/nfs_storage"


def train_model(config):
    time.sleep(1)
    print("Training model woohoo!")

    # Import model libraries, etc...
    # Load data and train model code here...
    with open("random_artifact.txt", "w") as f:
        f.write("artifact data hi there")

    # Return final stats. You can also return intermediate progress
    # using ray.air.session.report() if needed.
    # To return your model, you could write it to storage and return its
    # URI in this dict, or return it as a Tune Checkpoint:
    # https://docs.ray.io/en/latest/tune/tutorials/tune-checkpoints.html
    for i in range(5):
        with open("/tmp/data/checkpoint.data", "w") as f:
            f.write(f"Hello world {i}")
        session.report(
            {"score": 2.0, "epoch": i},
            checkpoint=Checkpoint.from_directory("/tmp/data"),
        )


# Test writing to a FAKE nfs path.
@pytest.mark.parametrize("storage_path", [None, FAKE_NFS_DIR])
def test_storage_path(storage_path):
    shutil.rmtree(RAY_RESULTS, ignore_errors=True)
    shutil.rmtree(FAKE_NFS_DIR, ignore_errors=True)

    # Define trial parameters as a single grid sweep.
    trial_space = {
        # This is an example parameter. You could replace it with filesystem paths,
        # model types, or even full nested Python dicts of model configurations, etc.,
        # that enumerate the set of trials to run.
        "model_id": tune.grid_search(["model_{}".format(i) for i in range(NUM_MODELS)])
    }

    # Start a Tune run and print the best result.
    rc = RunConfig(storage_path=storage_path)
    # rc = RunConfig(storage_path=None)
    tuner = tune.Tuner(train_model, param_space=trial_space, run_config=rc)
    results = tuner.fit()

    if storage_path:
        assert storage_path in results[0].checkpoint.path
    else:
        assert RAY_RESULTS_DIR in results[0].checkpoint.path

    # One experiment result dir.
    if storage_path:
        assert len(glob.glob(f"{storage_path}/train_model*")) == 1
    else:
        # Nothing should be there.
        assert len(glob.glob(f"{FAKE_NFS_DIR}/train_model*")) == 0
    assert len(glob.glob(f"{RAY_RESULTS}/train_model*")) == 1
    if storage_path:
        assert len(glob.glob(f"{storage_path}/train_model*/train_*")) == 2
    #        assert len(glob.glob(f"{storage_path}/train_model*/basic-variant-state*.json")) == 1
    #        assert len(glob.glob(f"{storage_path}/train_model*/experiment_state*.json")) == 1
    #        assert len(glob.glob(f"{storage_path}/train_model*/tuner.pkl")) == 1
    assert len(glob.glob(f"{RAY_RESULTS}/train_model*/train_*")) == 2
    assert len(glob.glob(f"{RAY_RESULTS}/train_model*/basic-variant-state*.json")) == 1
    assert len(glob.glob(f"{RAY_RESULTS}/train_model*/experiment_state*.json")) == 1
    #    assert len(glob.glob(f"{RAY_RESULTS}/train_model*/tuner.pkl")) == 1

    # Artifact and metrics sync.
    if storage_path:
        assert len(glob.glob(f"{storage_path}/train_model*/*/result.json")) == 2
        assert len(glob.glob(f"{storage_path}/train_model*/*/random_artifact.txt")) == 2
    assert len(glob.glob(f"{RAY_RESULTS}/train_model*/*/result.json")) == 2
    assert len(glob.glob(f"{RAY_RESULTS}/train_model*/*/random_artifact.txt")) == 2

    # Ten checkpoints.
    if storage_path:
        assert len(glob.glob(f"{storage_path}/train_model*/*/checkpoint_*")) == 10
        assert (
            len(
                glob.glob(f"{storage_path}/train_model*/*/checkpoint_*/checkpoint.data")
            )
            == 10
        )
        assert (
            len(glob.glob(f"{storage_path}/train_model*/*/checkpoint_*/.tune_metadata"))
            == 10
        )
        assert (
            len(glob.glob(f"{storage_path}/train_model*/*/checkpoint_*/.is_checkpoint"))
            == 10
        )
    assert len(glob.glob(f"{RAY_RESULTS}/train_model*/*/checkpoint_*")) == 10
    assert (
        len(glob.glob(f"{RAY_RESULTS}/train_model*/*/checkpoint_*/checkpoint.data"))
        == 10
    )
    assert (
        len(glob.glob(f"{RAY_RESULTS}/train_model*/*/checkpoint_*/.tune_metadata"))
        == 10
    )
    assert (
        len(glob.glob(f"{RAY_RESULTS}/train_model*/*/checkpoint_*/.is_checkpoint"))
        == 10
    )
