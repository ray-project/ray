import pandas as pd
import pytest
from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoTokenizer,
    Trainer,
    TrainingArguments,
)

import ray.data
from ray.exceptions import RayTaskError
from ray.train.batch_predictor import BatchPredictor
from ray.train.huggingface import HuggingFacePredictor, HuggingFaceTrainer
from ray.air.config import ScalingConfig
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
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    training_args = TrainingArguments(
        f"{model_checkpoint}-wikitext2",
        evaluation_strategy=config.pop("evaluation_strategy", "epoch"),
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


@pytest.mark.parametrize("save_strategy", ["no", "epoch"])
def test_e2e(ray_start_4_cpus, save_strategy):
    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)
    scaling_config = ScalingConfig(num_workers=2, use_gpu=False)
    trainer = HuggingFaceTrainer(
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

    trainer2 = HuggingFaceTrainer(
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

    predictor = BatchPredictor.from_checkpoint(
        result2.checkpoint,
        HuggingFacePredictor,
        task="text-generation",
        tokenizer=AutoTokenizer.from_pretrained(tokenizer_checkpoint),
    )

    predictions = predictor.predict(ray.data.from_pandas(prompts))
    assert predictions.count() == 3


def test_validation(ray_start_4_cpus):
    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)
    scaling_config = ScalingConfig(num_workers=2, use_gpu=False)
    trainer_conf = dict(
        trainer_init_per_worker=train_function,
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
    )

    # load_best_model_at_end set to True should raise an exception
    trainer = HuggingFaceTrainer(
        trainer_init_config={
            "epochs": 1,
            "load_best_model_at_end": True,
            "save_strategy": "epoch",
        },
        **trainer_conf,
    )
    with pytest.raises(RayTaskError):
        trainer.fit().error

    # logging strategy set to no should raise an exception
    trainer = HuggingFaceTrainer(
        trainer_init_config={
            "epochs": 1,
            "logging_strategy": "no",
        },
        **trainer_conf,
    )
    with pytest.raises(RayTaskError):
        trainer.fit().error

    # logging steps != eval steps should raise an exception
    trainer = HuggingFaceTrainer(
        trainer_init_config={
            "epochs": 1,
            "logging_strategy": "steps",
            "evaluation_strategy": "steps",
            "logging_steps": 20,
            "eval_steps": 10,
        },
        **trainer_conf,
    )
    with pytest.raises(RayTaskError):
        trainer.fit().error

    # mismatched strategies should raise an exception
    for logging_strategy, evaluation_strategy, save_strategy in (
        ("steps", "steps", "epoch"),
        ("epoch", "steps", "steps"),
        ("epoch", "steps", "epoch"),
        ("steps", "epoch", "steps"),
    ):
        trainer = HuggingFaceTrainer(
            trainer_init_config={
                "epochs": 1,
                "load_best_model_at_end": True,
                "logging_strategy": logging_strategy,
                "save_strategy": evaluation_strategy,
                "evaluation_strategy": save_strategy,
            },
            **trainer_conf,
        )
        with pytest.raises(RayTaskError):
            trainer.fit().error

    with pytest.raises(RayTaskError):
        trainer.fit().error


# Tests if checkpointing and restoring during tuning works correctly.
def test_tune(ray_start_8_cpus):
    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)
    scaling_config = ScalingConfig(
        num_workers=2, use_gpu=False, trainer_resources={"CPU": 0}
    )
    trainer = HuggingFaceTrainer(
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

    trainer = HuggingFaceTrainer(
        trainer_init_per_worker=train_function_with_metric,
        trainer_init_config={"epochs": 1},
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
