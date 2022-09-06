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
from ray.exceptions import RayTaskError
from ray.train.batch_predictor import BatchPredictor
from ray.train.huggingface import HuggingFacePredictor, HuggingFaceTrainer
from ray.air.config import ScalingConfig
from ray.train.huggingface._huggingface_utils import TrainReportCallback
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

# We are only testing Casual Language Modelling here

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

    with patch("ray.air.session.report", _fake_report):
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
    assert "log1" in reports[0]["metrics"]
    assert "log2" in reports[0]["metrics"]
    assert reports[0]["metrics"]["epoch"] == 1
    assert "log1" in reports[1]["metrics"]
    assert "log2" in reports[1]["metrics"]
    assert reports[1]["metrics"]["epoch"] == 2


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

    # evaluation_strategy set to "steps" should raise an exception
    trainer = HuggingFaceTrainer(
        trainer_init_config={"epochs": 1, "evaluation_strategy": "steps"},
        **trainer_conf,
    )
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
