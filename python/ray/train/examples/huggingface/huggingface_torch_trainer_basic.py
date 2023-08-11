# Minimal Example adapted from https://huggingface.co/docs/transformers/training
from datasets import load_dataset
from transformers import AutoTokenizer
from transformers import AutoModelForSequenceClassification
from transformers import TrainingArguments, Trainer
import numpy as np
import evaluate

from ray.train.huggingface.transformers import (
    prepare_trainer,
    RayTrainReportCallback,
)
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):
    # Datasets
    dataset = load_dataset("yelp_review_full")
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    def tokenize_function(examples):
        return tokenizer(examples["text"], padding="max_length", truncation=True)

    tokenized_datasets = dataset.map(tokenize_function, batched=True)

    small_train_dataset = (
        tokenized_datasets["train"].shuffle(seed=42).select(range(1000))
    )
    small_eval_dataset = tokenized_datasets["test"].shuffle(seed=42).select(range(1000))

    # Model
    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-cased", num_labels=5
    )

    # Evaluation Metrics
    metric = evaluate.load("accuracy")

    def compute_metrics(eval_pred):
        logits, labels = eval_pred
        predictions = np.argmax(logits, axis=-1)
        return metric.compute(predictions=predictions, references=labels)

    # HuggingFace Trainer
    training_args = TrainingArguments(
        output_dir="test_trainer", evaluation_strategy="epoch", report_to="none"
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=small_train_dataset,
        eval_dataset=small_eval_dataset,
        compute_metrics=compute_metrics,
    )

    # Ray Train Integration
    trainer.add_callback(RayTrainReportCallback())
    trainer = prepare_trainer(trainer)

    # Start Training
    trainer.train()


trainer = TorchTrainer(
    train_func, scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)

trainer.fit()
