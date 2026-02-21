import os

import numpy as np
import evaluate
from datasets import load_dataset
from transformers import (
    Trainer,
    TrainingArguments,
    AutoTokenizer,
    AutoModelForSequenceClassification,
)

import ray.train.huggingface.transformers
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


# [1] Encapsulate data preprocessing, training, and evaluation
# logic in a training function
# ============================================================
def train_func():
    # Datasets
    dataset = load_dataset("yelp_review_full")
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    def tokenize_function(examples):
        return tokenizer(examples["text"], padding="max_length", truncation=True)

    small_train_dataset = (
        dataset["train"].select(range(100)).map(tokenize_function, batched=True)
    )
    small_eval_dataset = (
        dataset["test"].select(range(100)).map(tokenize_function, batched=True)
    )

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

    # Hugging Face Trainer
    training_args = TrainingArguments(
        output_dir="test_trainer",
        evaluation_strategy="epoch",
        save_strategy="epoch",
        report_to="none",
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=small_train_dataset,
        eval_dataset=small_eval_dataset,
        compute_metrics=compute_metrics,
    )

    # [2] Report Metrics and Checkpoints to Ray Train
    # ===============================================
    callback = ray.train.huggingface.transformers.RayTrainReportCallback()
    trainer.add_callback(callback)

    # [3] Prepare Transformers Trainer
    # ================================
    trainer = ray.train.huggingface.transformers.prepare_trainer(trainer)

    # Start Training
    trainer.train()


def test_huggingface_transformers():
    # [4] Define a Ray TorchTrainer to launch `train_func` on all workers
    # ===================================================================
    ray_trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
        # [4a] For multi-node clusters, configure persistent storage that is
        # accessible across all worker nodes
        run_config=ray.train.RunConfig(
            storage_path="/mnt/cluster_storage/huggingface_run"
        ),
    )
    result: ray.train.Result = ray_trainer.fit()

    # [5] Load the trained model
    with result.checkpoint.as_directory() as checkpoint_dir:
        checkpoint_path = os.path.join(  # noqa: F841
            checkpoint_dir,
            ray.train.huggingface.transformers.RayTrainReportCallback.CHECKPOINT_NAME,
        )
        model = AutoModelForSequenceClassification.from_pretrained(  # noqa: F841
            checkpoint_path
        )


if __name__ == "__main__":
    test_huggingface_transformers()
