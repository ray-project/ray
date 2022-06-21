from unittest.mock import patch

import pandas as pd
import pytest
from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoTokenizer,
    Trainer,
    TrainingArguments,
)
from transformers.trainer_callback import TrainerState

import ray.data
from ray.train.batch_predictor import BatchPredictor
from ray.train.huggingface import HuggingFacePredictor, HuggingFaceTrainer
from ray.train.huggingface._huggingface_utils import TrainReportCallback
from ray.train.tests._huggingface_data import train_data, validation_data

# 16 first rows of tokenized wikitext-2-raw-v1 training & validation
train_df = pd.read_json(train_data)
validation_df = pd.read_json(validation_data)
prompts = pd.DataFrame(
    ["Complete me", "And me", "Please complete"], columns=["sentences"]
)

# We are only testing Casual Language Modelling here

model_checkpoint = "sshleifer/tiny-gpt2"
tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"


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
        evaluation_strategy="epoch",
        num_train_epochs=config.get("epochs", 3),
        learning_rate=2e-5,
        weight_decay=0.01,
        disable_tqdm=True,
        no_cuda=True,
        save_strategy=config.get("save_strategy", "no"),
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
    scaling_config = {"num_workers": 2, "use_gpu": False}
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

    trainer2 = HuggingFaceTrainer(
        trainer_init_per_worker=train_function,
        trainer_init_config={"epochs": 5},  # this will train for 1 epoch: 5 - 4 = 1
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
        resume_from_checkpoint=result.checkpoint,
    )
    result2 = trainer2.fit()

    assert result2.metrics["epoch"] == 5
    assert result2.metrics["training_iteration"] == 1
    assert result2.checkpoint

    predictor = BatchPredictor.from_checkpoint(
        result2.checkpoint,
        HuggingFacePredictor,
        task="text-generation",
        tokenizer=AutoTokenizer.from_pretrained(tokenizer_checkpoint),
    )

    predictions = predictor.predict(ray.data.from_pandas(prompts))
    assert predictions.count() == 3


def test_reporting():
    reports = []

    def _fake_report(**kwargs):
        reports.append(kwargs)

    with patch("ray.train.report", _fake_report):
        state = TrainerState()
        report_callback = TrainReportCallback()
        report_callback.on_epoch_begin(None, state, None)
        state.epoch = 0.5
        report_callback.on_log(None, state, None, logs={"log1": 1})
        state.epoch = 1
        report_callback.on_log(None, state, None, logs={"log2": 1})
        report_callback.on_epoch_end(None, state, None)
        report_callback.on_epoch_begin(None, state, None)
        state.epoch = 1.5
        report_callback.on_log(None, state, None, logs={"log1": 1})
        state.epoch = 2
        report_callback.on_log(None, state, None, logs={"log2": 1})
        report_callback.on_epoch_end(None, state, None)
        report_callback.on_train_end(None, state, None)

    assert len(reports) == 2
    assert "log1" in reports[0]
    assert "log2" in reports[0]
    assert reports[0]["epoch"] == 1
    assert "log1" in reports[1]
    assert "log2" in reports[1]
    assert reports[1]["epoch"] == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
