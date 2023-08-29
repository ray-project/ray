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
from ray.train.huggingface import (
    TransformersPredictor,
    TransformersTrainer,
    LegacyTransformersCheckpoint,
)
from ray.train.trainer import TrainingFailedError
from ray.train import ScalingConfig
from ray.train.tests._huggingface_data import train_data, validation_data
from ray import tune
from ray.tune import Tuner
from ray.tune.schedulers.async_hyperband import ASHAScheduler
from ray.tune.schedulers.resource_changing_scheduler import (
    DistributeResources,
    ResourceChangingScheduler,
)

# 16 first rows of tokenized wikitext-2-raw-v1 training & validation
train_df = pd.read_json(train_data)
validation_df = pd.read_json(validation_data)
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


def train_function(train_dataset, eval_dataset=None, **config):
    # Check that train_dataset has len
    assert len(train_dataset)

    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    evaluation_strategy = (
        config.pop("evaluation_strategy", "epoch") if eval_dataset else "no"
    )
    training_args = TrainingArguments(
        f"{model_checkpoint}-wikitext2",
        evaluation_strategy=evaluation_strategy,
        logging_strategy=config.pop("logging_strategy", "epoch"),
        num_train_epochs=config.pop("epochs", 3),
        learning_rate=config.pop("learning_rate", 2e-5),
        weight_decay=0.01,
        disable_tqdm=True,
        no_cuda=True,
        save_strategy=config.pop("save_strategy", "no"),
        **config,
    )
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
    )
    return trainer


def train_function_local_dataset(train_dataset, eval_dataset=None, **config):
    train_dataset = Dataset.from_pandas(train_df)
    eval_dataset = Dataset.from_pandas(validation_df)
    return train_function(train_dataset, eval_dataset, **config)


def test_deprecations(ray_start_4_cpus):
    """Tests that soft deprecations warn but still can be used"""
    from ray.train.huggingface import (
        HuggingFaceCheckpoint,
        HuggingFacePredictor,
        HuggingFaceTrainer,
    )

    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)

    with pytest.warns(DeprecationWarning):
        obj = HuggingFaceCheckpoint.from_dict({"foo": "bar"})
    assert isinstance(obj, LegacyTransformersCheckpoint)

    with pytest.warns(DeprecationWarning):
        obj = HuggingFacePredictor()
    assert isinstance(obj, TransformersPredictor)

    with pytest.warns(DeprecationWarning):
        obj = HuggingFaceTrainer(
            train_function,
            datasets={"train": ray_train, "evaluation": ray_validation},
        )
    assert isinstance(obj, TransformersTrainer)


@pytest.mark.parametrize("save_strategy", ["no", "epoch"])
def test_e2e(ray_start_4_cpus, save_strategy):
    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)
    scaling_config = ScalingConfig(num_workers=2, use_gpu=False)
    trainer = TransformersTrainer(
        trainer_init_per_worker=train_function,
        trainer_init_config={"epochs": 4, "save_strategy": save_strategy},
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
    )
    result = trainer.fit()

    assert result.metrics["epoch"] == 4
    assert result.metrics["training_iteration"] == 4
    assert result.checkpoint
    assert "eval_loss" in result.metrics

    trainer2 = TransformersTrainer(
        trainer_init_per_worker=train_function,
        trainer_init_config={
            "epochs": 5,
            "save_strategy": save_strategy,
        },  # this will train for 1 epoch: 5 - 4 = 1
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
        resume_from_checkpoint=result.checkpoint,
    )
    result2 = trainer2.fit()

    assert result2.metrics["epoch"] == 5
    assert result2.metrics["training_iteration"] == 1
    assert result2.checkpoint
    assert "eval_loss" in result2.metrics


def test_training_local_dataset(ray_start_4_cpus):
    scaling_config = ScalingConfig(num_workers=2, use_gpu=False)
    trainer = TransformersTrainer(
        trainer_init_per_worker=train_function_local_dataset,
        trainer_init_config={"epochs": 1, "save_strategy": "no"},
        scaling_config=scaling_config,
    )
    result = trainer.fit()

    assert result.metrics["epoch"] == 1
    assert result.metrics["training_iteration"] == 1
    assert result.checkpoint
    assert "eval_loss" in result.metrics


def test_validation(ray_start_4_cpus):
    def fit_and_check_for_error(trainer, error_type=ValueError):
        with pytest.raises(TrainingFailedError) as exc_info:
            trainer.fit().error
        assert isinstance(exc_info.value.__cause__, error_type)

    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)
    scaling_config = ScalingConfig(num_workers=2, use_gpu=False)
    trainer_conf = dict(
        trainer_init_per_worker=train_function,
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
    )

    # load_best_model_at_end set to True should raise an exception
    trainer = TransformersTrainer(
        trainer_init_config={
            "epochs": 1,
            "load_best_model_at_end": True,
            "save_strategy": "epoch",
        },
        **trainer_conf,
    )
    fit_and_check_for_error(trainer)

    # logging strategy set to no should raise an exception
    trainer = TransformersTrainer(
        trainer_init_config={
            "epochs": 1,
            "logging_strategy": "no",
        },
        **trainer_conf,
    )
    fit_and_check_for_error(trainer)

    # logging steps != eval steps should raise an exception
    trainer = TransformersTrainer(
        trainer_init_config={
            "epochs": 1,
            "logging_strategy": "steps",
            "evaluation_strategy": "steps",
            "logging_steps": 20,
            "eval_steps": 10,
        },
        **trainer_conf,
    )
    fit_and_check_for_error(trainer)

    # mismatched strategies should raise an exception
    for logging_strategy, evaluation_strategy, save_strategy in (
        ("steps", "steps", "epoch"),
        ("epoch", "steps", "steps"),
        ("epoch", "steps", "epoch"),
        ("steps", "epoch", "steps"),
    ):
        trainer = TransformersTrainer(
            trainer_init_config={
                "epochs": 1,
                "load_best_model_at_end": True,
                "logging_strategy": logging_strategy,
                "save_strategy": evaluation_strategy,
                "evaluation_strategy": save_strategy,
            },
            **trainer_conf,
        )
        fit_and_check_for_error(trainer)


# Tests if checkpointing and restoring during tuning works correctly.
def test_tune(ray_start_8_cpus):
    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)
    scaling_config = ScalingConfig(
        num_workers=2, use_gpu=False, trainer_resources={"CPU": 0}
    )
    trainer = TransformersTrainer(
        trainer_init_per_worker=train_function,
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
    )

    tune_epochs = 5
    tuner = Tuner(
        trainer,
        param_space={
            "trainer_init_config": {
                "learning_rate": tune.loguniform(2e-6, 2e-5),
                "epochs": tune_epochs,
                "save_strategy": "epoch",
            }
        },
        tune_config=tune.TuneConfig(
            metric="eval_loss",
            mode="min",
            num_samples=3,
            scheduler=ResourceChangingScheduler(
                ASHAScheduler(
                    max_t=tune_epochs,
                ),
                resources_allocation_function=DistributeResources(
                    add_bundles=True, reserve_resources={"CPU": 1}
                ),
            ),
        ),
    )
    tune_results = tuner.fit()
    assert not tune_results.errors


# Tests for https://github.com/ray-project/ray/issues/28084
def test_datasets_modules_import(ray_start_4_cpus):
    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)
    scaling_config = ScalingConfig(num_workers=2, use_gpu=False)

    from datasets import load_metric

    metric = load_metric("glue", "cola")

    def train_function_with_metric(train_dataset, eval_dataset=None, **config):
        print(metric)
        return train_function(train_dataset, eval_dataset=eval_dataset, **config)

    trainer = TransformersTrainer(
        trainer_init_per_worker=train_function_with_metric,
        trainer_init_config={"epochs": 1},
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
