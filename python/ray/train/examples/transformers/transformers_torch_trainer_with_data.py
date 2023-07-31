
import ray
import torch
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

from ray.train.huggingface.transformers import prepare_trainer, RayDataIterableDataset


tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

def tokenize(examples):
    output = tokenizer(
        list(examples["text"]),
        max_length=256,
        truncation=True,
        padding="max_length",
        return_tensors="pt",
    )
    output["labels"] = torch.LongTensor(examples["label"]).cuda()
    return output

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

    train_ds = ray.train.get_dataset_shard("train")
    eval_ds = ray.train.get_dataset_shard("test")

    train_ds = RayDataIterableDataset(train_ds, batch_size=32, collate_fn=tokenize)
    eval_ds = RayDataIterableDataset(eval_ds, batch_size=32, collate_fn=tokenize)

    # TODO(yunxuanx): This will be possible after iter_torch_batches returns an iterable
    # train_ds = ray.train.get_dataset_shard("train").iter_torch_batches(batch_size=32, collate_fn=tokenize)
    # eval_ds = ray.train.get_dataset_shard("test").iter_torch_batches(batch_size=32, collate_fn=tokenize)

    # Define Transformers Trainer
    training_args = TrainingArguments(
        output_dir="./exp_results",
        learning_rate=1e-4,
        per_device_train_batch_size=32,
        per_device_eval_batch_size=32,
        num_train_epochs=4,
        max_steps=100,
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
        eval_dataset=eval_ds,
        tokenizer=tokenizer,
        compute_metrics=compute_metrics,
    )

    trainer = prepare_trainer(trainer)

    # Train your model
    trainer.train()

if __name__ == "__main__":
    hf_ds = load_dataset("tweet_eval", "irony")

    ray_train_ds = ray.data.from_huggingface(hf_ds["train"])
    ray_test_ds = ray.data.from_huggingface(hf_ds["test"])

    ray_datasets = {"train": ray_test_ds, "test": ray_test_ds}
    
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
        ),
        datasets=ray_datasets
    )

    ray_trainer.fit()
   