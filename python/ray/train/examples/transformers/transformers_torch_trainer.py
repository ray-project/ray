import os
import evaluate
import numpy as np
from datasets import load_dataset
from ray.train import RunConfig, ScalingConfig, CheckpointConfig
from ray.train.torch import TorchTrainer
from transformers import AutoTokenizer
from transformers import (
    AutoModelForSequenceClassification,
    DataCollatorWithPadding,
    TrainingArguments,
    Trainer,
)

from ray.train.huggingface.transformers import prepare_trainer

hf_ds = load_dataset("tweet_eval", "irony")
tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

def tokenize(examples):
    return tokenizer(
        examples["text"],
        max_length=256,
        truncation=True,
        padding="max_length",
    )

train_ds = hf_ds["train"].map(tokenize, batched=True)
test_ds = hf_ds["test"].map(tokenize, batched=True)

data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

def train_loop_per_worker():
    accuracy = evaluate.load("accuracy")

    def compute_metrics(eval_pred):
        predictions, labels = eval_pred
        predictions = np.argmax(predictions, axis=1)
        return accuracy.compute(predictions=predictions, references=labels)

    # Load pretrained model
    model = AutoModelForSequenceClassification.from_pretrained(
        "bert-base-cased", num_labels=2
    )

    # Define Transformers Trainer
    training_args = TrainingArguments(
        output_dir="./exp_results",
        learning_rate=1e-4,
        per_device_train_batch_size=32,
        per_device_eval_batch_size=32,
        num_train_epochs=4,
        weight_decay=0.01,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="epoch",  # It cannot be set to "steps", so rigid...
        logging_steps=10,
        push_to_hub=False,
        report_to="wandb",
        save_total_limit=2,
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=test_ds,
        tokenizer=tokenizer,
        data_collator=data_collator,
        compute_metrics=compute_metrics,
    )

    trainer = prepare_trainer(trainer)

    # Train your model
    trainer.train()

if __name__ == "__main__":
    ray_trainer = TorchTrainer(
        train_loop_per_worker,
        run_config=RunConfig(
            name="exp",
            checkpoint_config=CheckpointConfig(
                num_to_keep=2,
                checkpoint_score_attribute="eval_accuracy",
                checkpoint_score_order="max",
            )
        ),
        scaling_config=ScalingConfig(
            num_workers=4,
            use_gpu=True
        )
    )

    ray_trainer.fit()
   