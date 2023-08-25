# flake8: noqa

# __ft_initial_run_start__
import pickle
from tempfile import TemporaryDirectory
from ray import train, tune
from ray.train._checkpoint import Checkpoint


def trainable(config):
    # Checkpoint loading
    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as tmpdir:
            with open(f"{tmpdir}/ckpt.pkl", "rb") as fin:
                state_dict = pickle.load(fin)
        start = state_dict["epoch"] + 1
    else:
        start = 1

    for epoch in range(start, config["num_epochs"]):
        # Do some training...

        # Checkpoint saving
        with TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/ckpt.pkl", "wb") as fout:
                pickle.dump({"epoch": epoch}, fout)

            train.report({"epoch": epoch}, checkpoint=Checkpoint.from_directory(tmpdir))


tuner = tune.Tuner(
    trainable,
    param_space={"num_epochs": 10},
    run_config=train.RunConfig(
        storage_path="~/ray_results", name="tune_fault_tolerance_guide"
    ),
)
tuner.fit()
# __ft_initial_run_end__

# __ft_restored_run_start__
tuner = tune.Tuner.restore(
    "~/ray_results/tune_fault_tolerance_guide",
    trainable=trainable,
    resume_errored=True,
)
tuner.fit()
# __ft_restored_run_end__

# __ft_restore_options_start__
tuner = tune.Tuner.restore(
    "~/ray_results/tune_fault_tolerance_guide",
    trainable=trainable,
    resume_errored=True,
    restart_errored=False,
    resume_unfinished=True,
)
# __ft_restore_options_end__

# __ft_restore_multiplexing_start__
import os
from ray import train, tune

storage_path = "~/ray_results"
exp_name = "tune_fault_tolerance_guide"
path = os.path.join(storage_path, exp_name)

if tune.Tuner.can_restore(path):
    tuner = tune.Tuner.restore(path, trainable=trainable, resume_errored=True)
else:
    tuner = tune.Tuner(
        trainable,
        param_space={"num_epochs": 10},
        run_config=train.RunConfig(storage_path=storage_path, name=exp_name),
    )
tuner.fit()
# __ft_restore_multiplexing_end__


# Run the multiplexed logic again to make sure it goes through the restore branch.
if tune.Tuner.can_restore(path):
    tuner = tune.Tuner.restore(path, trainable=trainable, resume_errored=True)
else:
    tuner = tune.Tuner(
        trainable,
        param_space={"num_epochs": 10},
        run_config=train.RunConfig(storage_path=storage_path, name=exp_name),
    )
assert tuner.get_results()


# __ft_restore_objrefs_initial_start__
import ray
from ray import train, tune


class LargeModel:
    def __init__(self, model_id):
        self.model_id = model_id
        # Load weights based on the `model_id`...


def train_fn(config):
    # Retrieve the model from the object store.
    model = ray.get(config["model_ref"])
    print(model.model_id)


# These models may be large, so `ray.put` them in the Ray Object Store
# to share the models between trials.
model_refs = [ray.put(LargeModel(1)), ray.put(LargeModel(2))]

tuner = tune.Tuner(
    train_fn,
    # Tune over the object references!
    param_space={"model_ref": tune.grid_search(model_refs)},
    run_config=train.RunConfig(
        storage_path="~/ray_results", name="restore_object_refs"
    ),
)
tuner.fit()
# __ft_restore_objrefs_initial_end__

if ray.is_initialized():
    ray.shutdown()

# __ft_restore_objrefs_restored_start__
# Re-create the objects and put them in the object store.
param_space = {
    "model_ref": tune.grid_search([ray.put(LargeModel(1)), ray.put(LargeModel(2))])
}

tuner = tune.Tuner.restore(
    "~/ray_results/restore_object_refs",
    trainable=train_fn,
    # Re-specify the `param_space` to update the object references.
    param_space=param_space,
    resume_errored=True,
)
tuner.fit()
# __ft_restore_objrefs_restored_end__

# __ft_trial_failure_start__
from ray import train, tune

tuner = tune.Tuner(
    trainable,
    param_space={"num_epochs": 10},
    run_config=train.RunConfig(
        storage_path="~/ray_results",
        name="trial_fault_tolerance",
        failure_config=train.FailureConfig(max_failures=3),
    ),
)
tuner.fit()
# __ft_trial_failure_end__
