import pandas as pd
import pytest
from datasets import Dataset
from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    Trainer,
    TrainingArguments,
)

import ray.data
from ray.train.trainer import TrainingFailedError
from ray.train import ScalingConfig, Checkpoint
from ray.train.tests._huggingface_data import train_data, validation_data
from ray import tune
from ray.tune import Tuner
from ray.tune.schedulers.async_hyperband import ASHAScheduler
from ray.tune.schedulers.resource_changing_scheduler import (
    DistributeResources,
    ResourceChangingScheduler,
)

from ray.train.torch import TorchTrainer
from ray.train.huggingface.transformers import RayTrainReportCallback, prepare_trainer

# 16 first rows of tokenized wikitext-2-raw-v1 training & validation

prompts = pd.DataFrame(
    ["Complete me", "And me", "Please complete"], columns=["sentences"]
)

# We are only testing Causal Language Modeling here

model_checkpoint = "hf-internal-testing/tiny-random-gpt2"
tokenizer_checkpoint = "hf-internal-testing/tiny-random-gpt2"


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def train_func(config):
    if config["use_ray_data"]:
        train_ds_shard = ray.train.get_dataset_shard("train")
        eval_ds_shard = ray.train.get_dataset_shard("eval")

        train_dataset = train_ds_shard.iter_torch_batches(batch_size=8)
        eval_dataset = eval_ds_shard.iter_torch_batches(batch_size=8)
    else:
        train_df = pd.read_json(train_data)
        validation_df = pd.read_json(validation_data)

        train_dataset = Dataset.from_pandas(train_df)
        eval_dataset = Dataset.from_pandas(validation_df)

    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)

    training_args = TrainingArguments(
        f"{model_checkpoint}-wikitext2",
        evaluation_strategy=config["evaluation_strategy"],
        logging_strategy=config["logging_strategy"],
        save_strategy=config["save_strategy"],
        eval_steps=config["eval_steps"],
        save_steps=config["save_steps"],
        logging_steps=config["logging_steps"],
        num_train_epochs=3,
        max_steps=config.get("max_steps", -1),
        learning_rate=config.get("learning_rate", 2e-5),
        weight_decay=0.01,
        disable_tqdm=True,
        no_cuda=True,
    )
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
    )
    trainer.add_callback(RayTrainReportCallback())
    trainer = prepare_trainer(trainer)
    trainer.fit()


CONFIGURATIONS = {
    "1": {
        "evaluation_strategy": "epoch",
        "save_strategy": "epoch",
        "logging_strategy": "epoch",
        "eval_steps": None,
        "save_steps": None,
        "logging_steps": None,
    },
    "2": {
        "evaluation_strategy": "steps",
        "save_strategy": "steps",
        "logging_strategy": "steps",
        "eval_steps": 20,
        "save_steps": 20,
        "logging_steps": 20,
    },
    "3": {
        "evaluation_strategy": "epochs",
        "save_strategy": "epochs",
        "logging_strategy": "steps",
        "eval_steps": None,
        "save_steps": None,
        "logging_steps": 20,
    },
}


def test_e2e_hf_data(config_id):
    train_loop_config = CONFIGURATIONS[config_id]
    train_loop_config["use_ray_data"] = False

    trainer = TorchTrainer(
        train_func,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    )
    result = trainer.fit()

    assert result.metrics["epoch"] == 3
    assert result.metrics["training_iteration"] == 3
    assert result.checkpoint
    assert isinstance(result.checkpoint, Checkpoint)
    assert "eval_loss" in result.metrics


def test_e2e_ray_data(config_id):
    train_loop_config = CONFIGURATIONS[config_id]
    train_loop_config["use_ray_data"] = True
    train_loop_config["max_steps"] = 60

    train_df = pd.read_json(train_data)
    validation_df = pd.read_json(validation_data)

    ray_train_ds = ray.data.from_pandas(train_df)
    ray_eval_ds = ray.data.from_pandas(validation_df)

    trainer = TorchTrainer(
        train_func,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        datasets={"train": ray_train_ds, "eval": ray_eval_ds},
    )
    result = trainer.fit()

    assert result.metrics["step"] == 60
    assert result.metrics["training_iteration"] == 3
    assert result.checkpoint
    assert isinstance(result.checkpoint, Checkpoint)
    assert "eval_loss" in result.metrics


# Tests if checkpointing and restoring during tuning works correctly.
def test_tune(ray_start_8_cpus):
    train_loop_config = CONFIGURATIONS["1"]
    train_loop_config["use_ray_data"] = False

    trainer = TorchTrainer(
        train_func,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
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
                    max_t=3,
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

    sys.exit(pytest.main(["-v", "-x", __file__]))
