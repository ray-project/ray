import pandas as pd
import pytest
from datasets import Dataset
from transformers import AutoConfig, AutoModelForCausalLM, Trainer, TrainingArguments

import ray.data
from ray import tune
from ray.train import Checkpoint, ScalingConfig
from ray.train.huggingface.transformers import RayTrainReportCallback, prepare_trainer
from ray.train.tests._huggingface_data import train_data, validation_data
from ray.train.torch import TorchTrainer
from ray.tune import Tuner
from ray.tune.schedulers.async_hyperband import ASHAScheduler
from ray.tune.schedulers.resource_changing_scheduler import (
    DistributeResources,
    ResourceChangingScheduler,
)


@pytest.fixture
def ray_start_6_cpus_2_gpus():
    address_info = ray.init(num_cpus=6, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


# We are only testing Causal Language Modeling here
MODEL_NAME = "hf-internal-testing/tiny-random-BloomForCausalLM"

# Training Loop Configurations
NUM_WORKERS = 2
BATCH_SIZE_PER_WORKER = 2
TRAIN_DATASET_SIZE = 16
MAX_EPOCHS = 4

STEPS_PER_EPOCH = TRAIN_DATASET_SIZE // (BATCH_SIZE_PER_WORKER * NUM_WORKERS)
MAX_STEPS = MAX_EPOCHS * STEPS_PER_EPOCH

# Transformers Traienr Configurations
CONFIGURATIONS = {
    "epoch_gpu": {
        "evaluation_strategy": "epoch",
        "save_strategy": "epoch",
        "logging_strategy": "epoch",
        "eval_steps": None,
        "save_steps": None,
        "logging_steps": None,
        "no_cuda": False,
        "use_dict_eval_datasets": False,
    },
    "steps_gpu": {
        "evaluation_strategy": "steps",
        "save_strategy": "steps",
        "logging_strategy": "steps",
        "eval_steps": STEPS_PER_EPOCH,
        "save_steps": STEPS_PER_EPOCH * 2,
        "logging_steps": 1,
        "no_cuda": False,
        "use_dict_eval_datasets": False,
    },
    "steps_cpu": {
        "evaluation_strategy": "steps",
        "save_strategy": "steps",
        "logging_strategy": "steps",
        "eval_steps": STEPS_PER_EPOCH,
        "save_steps": STEPS_PER_EPOCH,
        "logging_steps": 1,
        "no_cuda": True,
        "use_dict_eval_datasets": False,
    },
}


def train_func(config):
    # Datasets
    if config["use_ray_data"]:
        train_ds_shard = ray.train.get_dataset_shard("train")
        train_dataset = train_ds_shard.iter_torch_batches(
            batch_size=BATCH_SIZE_PER_WORKER
        )
        if config["use_dict_eval_datasets"]:
            eval_ds_shard_1 = ray.train.get_dataset_shard("eval_1")
            eval_ds_shard_2 = ray.train.get_dataset_shard("eval_2")

            eval_dataset = {
                "eval_1": eval_ds_shard_1.iter_torch_batches(
                    batch_size=BATCH_SIZE_PER_WORKER
                ),
                "eval_2": eval_ds_shard_2.iter_torch_batches(
                    batch_size=BATCH_SIZE_PER_WORKER
                ),
            }
        else:
            eval_ds_shard = ray.train.get_dataset_shard("eval")

            eval_dataset = eval_ds_shard.iter_torch_batches(
                batch_size=BATCH_SIZE_PER_WORKER
            )
    else:
        train_df = pd.read_json(train_data)
        validation_df = pd.read_json(validation_data)

        train_dataset = Dataset.from_pandas(train_df)
        eval_dataset = Dataset.from_pandas(validation_df)

    # Model
    model_config = AutoConfig.from_pretrained(MODEL_NAME)
    model = AutoModelForCausalLM.from_config(model_config)

    # HF Transformers Trainer
    training_args = TrainingArguments(
        f"{MODEL_NAME}-wikitext2",
        evaluation_strategy=config["evaluation_strategy"],
        logging_strategy=config["logging_strategy"],
        save_strategy=config["save_strategy"],
        eval_steps=config["eval_steps"],
        save_steps=config["save_steps"],
        logging_steps=config["logging_steps"],
        num_train_epochs=config.get("num_train_epochs", MAX_EPOCHS),
        max_steps=config.get("max_steps", -1),
        learning_rate=config.get("learning_rate", 2e-5),
        per_device_train_batch_size=BATCH_SIZE_PER_WORKER,
        per_device_eval_batch_size=BATCH_SIZE_PER_WORKER,
        weight_decay=0.01,
        disable_tqdm=True,
        no_cuda=config["no_cuda"],
        report_to="none",
    )
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
    )

    # Report to Ray Train
    trainer.add_callback(RayTrainReportCallback())
    trainer = prepare_trainer(trainer)

    # Start Training
    trainer.train()


@pytest.mark.parametrize("config_id", ["epoch_gpu", "steps_gpu", "steps_cpu"])
def test_e2e_hf_data(ray_start_6_cpus_2_gpus, config_id):
    train_loop_config = CONFIGURATIONS[config_id]

    # Specify `num_train_epochs` for Map-style Dataset
    train_loop_config["use_ray_data"] = False
    train_loop_config["num_train_epochs"] = MAX_EPOCHS

    # Calculate the num of Ray training iterations
    if train_loop_config["save_strategy"] == "steps":
        num_iterations = MAX_STEPS // train_loop_config["save_steps"]
    else:
        num_iterations = MAX_EPOCHS

    use_gpu = not train_loop_config["no_cuda"]

    trainer = TorchTrainer(
        train_func,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=NUM_WORKERS, use_gpu=use_gpu),
    )
    result = trainer.fit()

    assert result.metrics["epoch"] == MAX_EPOCHS
    assert result.metrics["step"] == MAX_STEPS
    assert result.metrics["training_iteration"] == num_iterations
    assert result.checkpoint
    assert isinstance(result.checkpoint, Checkpoint)
    assert len(result.best_checkpoints) == num_iterations
    assert "eval_loss" in result.metrics


@pytest.mark.parametrize("config_id", ["steps_gpu", "steps_cpu"])
def test_e2e_ray_data(ray_start_6_cpus_2_gpus, config_id):
    train_loop_config = CONFIGURATIONS[config_id]

    # Must specify `max_steps` for Iterable Dataset
    train_loop_config["use_ray_data"] = True
    train_loop_config["max_steps"] = MAX_STEPS

    # Calculate the num of Ray training iterations
    num_iterations = MAX_STEPS // train_loop_config["save_steps"]

    train_df = pd.read_json(train_data)
    validation_df = pd.read_json(validation_data)

    ray_train_ds = ray.data.from_pandas(train_df)
    ray_eval_ds = ray.data.from_pandas(validation_df)

    use_gpu = not train_loop_config["no_cuda"]

    trainer = TorchTrainer(
        train_func,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=NUM_WORKERS, use_gpu=use_gpu),
        datasets={"train": ray_train_ds, "eval": ray_eval_ds},
    )
    result = trainer.fit()

    assert result.metrics["step"] == MAX_STEPS
    assert result.metrics["training_iteration"] == num_iterations
    assert result.checkpoint
    assert isinstance(result.checkpoint, Checkpoint)
    assert len(result.best_checkpoints) == num_iterations
    assert "eval_loss" in result.metrics


@pytest.mark.parametrize("config_id", ["steps_gpu", "steps_cpu"])
def test_e2e_dict_eval_ray_data(ray_start_6_cpus_2_gpus, config_id):
    train_loop_config = CONFIGURATIONS[config_id]

    # Must specify `max_steps` for Iterable Dataset
    train_loop_config["use_ray_data"] = True
    train_loop_config["use_dict_eval_datasets"] = True
    train_loop_config["max_steps"] = MAX_STEPS

    # Calculate the num of Ray training iterations
    num_iterations = MAX_STEPS // train_loop_config["save_steps"]

    train_df = pd.read_json(train_data)
    validation_df = pd.read_json(validation_data)

    ray_train_ds = ray.data.from_pandas(train_df)
    ray_eval_ds_1 = ray.data.from_pandas(validation_df)
    ray_eval_ds_2 = ray.data.from_pandas(validation_df)

    use_gpu = not train_loop_config["no_cuda"]

    trainer = TorchTrainer(
        train_func,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=NUM_WORKERS, use_gpu=use_gpu),
        datasets={
            "train": ray_train_ds,
            "eval_1": ray_eval_ds_1,
            "eval_2": ray_eval_ds_2,
        },
    )
    result = trainer.fit()

    assert result.metrics["step"] == MAX_STEPS
    assert result.metrics["training_iteration"] == num_iterations
    assert result.checkpoint
    assert isinstance(result.checkpoint, Checkpoint)
    assert len(result.best_checkpoints) == num_iterations
    assert "eval_eval_1_loss" in result.metrics
    assert "eval_eval_2_loss" in result.metrics


# Tests if Ray Tune works correctly.
def test_tune(ray_start_8_cpus):
    train_loop_config = CONFIGURATIONS["steps_cpu"]
    train_loop_config["use_ray_data"] = False

    use_gpu = not train_loop_config["no_cuda"]
    trainer = TorchTrainer(
        train_func,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=NUM_WORKERS, use_gpu=use_gpu),
    )

    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "learning_rate": tune.loguniform(2e-6, 2e-5),
            }
        },
        tune_config=tune.TuneConfig(
            metric="eval_loss",
            mode="min",
            num_samples=3,
            scheduler=ResourceChangingScheduler(
                ASHAScheduler(
                    max_t=MAX_EPOCHS,
                ),
                resources_allocation_function=DistributeResources(
                    add_bundles=True, reserve_resources={"CPU": 1}
                ),
            ),
        ),
    )
    tune_results = tuner.fit()
    assert not tune_results.errors


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
