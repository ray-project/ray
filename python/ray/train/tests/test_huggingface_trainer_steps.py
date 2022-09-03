import math

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
from ray.train.batch_predictor import BatchPredictor
from ray.train.huggingface import HuggingFacePredictor, HuggingFaceTrainer
from ray.air.config import ScalingConfig
from ray.train.tests._huggingface_data import train_data, validation_data

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


def train_function(train_dataset, eval_dataset=None, **config):
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    training_args = TrainingArguments(
        f"{model_checkpoint}-wikitext2",
        evaluation_strategy=config.pop("evaluation_strategy", "steps"),
        logging_strategy=config.pop("logging_strategy", "steps"),
        num_train_epochs=config.pop("epochs", 3),
        learning_rate=2e-5,
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


@pytest.mark.parametrize("save_steps", [0, 2, 3, 8, 12])
@pytest.mark.parametrize("logging_steps", [2, 3, 8, 12])
def test_e2e_steps(ray_start_4_cpus, save_steps, logging_steps):
    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)
    scaling_config = ScalingConfig(num_workers=2, use_gpu=False)

    epochs = 4
    trainer = HuggingFaceTrainer(
        trainer_init_per_worker=train_function,
        trainer_init_config={
            "epochs": epochs,
            "save_strategy": "no" if not save_steps else "steps",
            "logging_strategy": "steps",
            "evaluation_strategy": "steps",
            "save_steps": save_steps,
            "logging_steps": logging_steps,
        },
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
    )
    if save_steps and (save_steps < logging_steps or save_steps % logging_steps != 0):
        # Test validation
        with pytest.raises(ValueError):
            result = trainer.fit()
        return
    result = trainer.fit()

    assert result.metrics["epoch"] == epochs
    assert result.metrics["training_iteration"] == math.ceil(epochs / logging_steps)
    assert result.checkpoint
    assert "eval_loss" in result.metrics

    trainer2 = HuggingFaceTrainer(
        trainer_init_per_worker=train_function,
        trainer_init_config={
            "epochs": epochs + 1,
            "save_strategy": "no" if not save_steps else "steps",
            "logging_strategy": "steps",
            "evaluation_strategy": "steps",
            "save_steps": save_steps,
            "logging_steps": logging_steps,
        },
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
        resume_from_checkpoint=result.checkpoint,
    )
    result2 = trainer2.fit()

    assert result2.metrics["epoch"] == epochs + 1
    assert result2.metrics["training_iteration"] == math.ceil(1 / logging_steps)
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
