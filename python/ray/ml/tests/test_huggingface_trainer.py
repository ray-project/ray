import pandas as pd
import pytest
import torch
from datasets.arrow_dataset import Dataset
from transformers import AutoConfig, AutoModelForCausalLM, Trainer, TrainingArguments

import ray.data
from ray.ml.train.integrations.huggingface import HuggingFaceTrainer
from ray.ml.predictors.integrations.huggingface import HuggingFacePredictor
from ray.ml.train.integrations.huggingface.huggingface_utils import process_datasets

# 16 first rows of tokenized wikitext-2-raw-v1 training & validation
train_df = pd.read_json("./huggingface_data/train.json")
validation_df = pd.read_json("./huggingface_data/validation.json")

# We are only testing Casual Language Modelling here

model_checkpoint = "sshleifer/tiny-gpt2"


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
        trainer_init_config={"epochs": 3, "save_strategy": save_strategy},
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
    )
    result = trainer.fit()

    assert result.metrics["epoch"] == 3
    assert result.checkpoint

    trainer2 = HuggingFaceTrainer(
        trainer_init_per_worker=train_function,
        trainer_init_config={"epochs": 4},  # this will train for 1 epoch: 4 - 3 = 1
        scaling_config=scaling_config,
        datasets={"train": ray_train, "evaluation": ray_validation},
        resume_from_checkpoint=result.checkpoint,
    )
    result2 = trainer2.fit()

    assert result2.metrics["epoch"] == 4
    assert result2.checkpoint

    class HuggingFaceScorer:
        def __init__(self):
            self.pred = HuggingFacePredictor.from_checkpoint(
                result2.checkpoint, AutoModelForCausalLM
            )

        def __call__(self, x):
            return self.pred.predict(x)

    predictions = ray_validation.map_batches(
        HuggingFaceScorer, batch_size=8, batch_format="pandas", compute="actors"
    )
    assert predictions.count() == 16


def test_same_data_format(ray_start_4_cpus):
    train_hf_dataset = Dataset.from_pandas(train_df)
    validation_hf_dataset = Dataset.from_pandas(validation_df)
    hf_trainer = train_function(train_hf_dataset, validation_hf_dataset)
    hf_trainer._get_train_sampler = lambda: None  # No randomness
    hf_train_dataloader = hf_trainer.get_train_dataloader()

    ray_train = ray.data.from_pandas(train_df)
    ray_validation = ray.data.from_pandas(validation_df)
    ray_train, ray_validation = process_datasets(ray_train, ray_validation)
    ray_trainer = train_function(ray_train, ray_validation)
    ray_train_dataloader = ray_trainer.get_train_dataloader()

    hf_train_dataloader_inputs = [
        hf_trainer._prepare_inputs(inputs) for inputs in hf_train_dataloader
    ]
    ray_train_dataloader_inputs = [
        ray_trainer._prepare_inputs(inputs) for inputs in ray_train_dataloader
    ]

    def equal_or_exception(a: torch.Tensor, b: torch.Tensor):
        if not torch.equal(a, b):
            raise AssertionError(
                f"Tensor A ({a.shape}) doesn't equal tensor B ({b.shape}):"
                f"\n{a}\n{b}\n"
            )

    # We squeeze to get rid of the extra dimension added by the HF
    # torch_default_data_collator. The models seem to train and predict
    # fine with that extra dimension.
    [
        [equal_or_exception(a[k], b[k].squeeze()) for k in a]
        for a, b in zip(hf_train_dataloader_inputs, ray_train_dataloader_inputs)
    ]
