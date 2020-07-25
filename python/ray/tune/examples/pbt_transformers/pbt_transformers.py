# flake8: noqa
# yapf: disable

"""
Please note that this example requires Python >= 3.7 to run.
"""

# __import_begin__
import os
from dataclasses import dataclass, field
from functools import partial
from typing import Callable, Dict, Optional

import numpy as np
from ray.tune import CLIReporter
from ray.tune.schedulers import PopulationBasedTraining

from ray import tune
from ray.tune.examples.pbt_transformers import trainer
from ray.tune.examples.pbt_transformers import load_and_cache, get_datasets, disable_tqdm

from transformers import AutoConfig, AutoModelForSequenceClassification, EvalPrediction
from transformers import (
    TrainingArguments,
    glue_compute_metrics,
    glue_output_modes,
    glue_tasks_num_labels,
)


def build_compute_metrics_fn(task_name: str) -> Callable[[EvalPrediction], Dict]:
    output_mode = glue_output_modes[task_name]

    def compute_metrics_fn(p: EvalPrediction):
        if output_mode == "classification":
            preds = np.argmax(p.predictions, axis=1)
        elif output_mode == "regression":
            preds = np.squeeze(p.predictions)
        metrics = glue_compute_metrics(task_name, preds, p.label_ids)
        tune.report(mean_accuracy=metrics["acc"])
        return metrics

    return compute_metrics_fn


def get_trainer(model_name_or_path, data_dir, task_name, training_args):
    try:
        num_labels = glue_tasks_num_labels[task_name]
        output_mode = glue_output_modes[task_name]
    except KeyError:
        raise ValueError("Task not found: %s" % (task_name))

    config = AutoConfig.from_pretrained(
        model_name_or_path,
        num_labels=num_labels,
        finetuning_task=task_name,
    )

    model = AutoModelForSequenceClassification.from_pretrained(
        model_name_or_path,
        config=config,
    )

    train_dataset = get_datasets(data_dir, task_name, "train")
    eval_dataset = get_datasets(data_dir, task_name, "val")

    tune_trainer = trainer.TuneTransformerTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        compute_metrics=build_compute_metrics_fn(task_name)
    )

    return tune_trainer


# __train_begin__
def train_transformer(config, checkpoint=None):
    training_args = TrainingArguments(
        output_dir=tune.get_trial_dir(),
        learning_rate=config["learning_rate"],
        do_train=True,
        do_eval=True,
        evaluate_during_training=True,
        eval_steps=50,
        save_steps=0,
        num_train_epochs=config["num_epochs"],
        per_device_train_batch_size=config["per_gpu_train_batch_size"],
        per_device_eval_batch_size=config["per_gpu_val_batch_size"],
        warmup_steps=0,
        weight_decay=config["weight_decay"],
        logging_dir="./logs",
    )

    tune_trainer = get_trainer(config["model_name"], config["data_dir"], config["task_name"], training_args)
    tune_trainer.train(checkpoint if checkpoint is not None and len(checkpoint) > 0 else config["model_name"])


# __train_end__


# __tune_begin__
def tune_transformer(num_samples=8, num_epochs=3, gpus_per_trial=0):
    data_dir = os.path.abspath(os.path.join(os.getcwd(), "./data"))
    if not os.path.exists(data_dir):
        os.mkdir(data_dir, 0o755)
    model_name = "bert-base-uncased"
    task_name = "rte"

    # Download and cache tokenizer, model, and features
    load_and_cache(task_name, model_name, data_dir=data_dir)

    config = {
        "model_name": model_name,
        "task_name": task_name,
        "data_dir": data_dir,
        "per_gpu_val_batch_size": 32,
        "per_gpu_train_batch_size": tune.choice([16, 32, 64]),
        "learning_rate": tune.uniform(1e-5, 5e-5),
        "weight_decay": tune.uniform(0.0, 0.3),
        "num_epochs": tune.choice([2, 3, 4, 5]),
    }

    scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        metric="mean_accuracy",
        mode="max",
        perturbation_interval=1,
        hyperparam_mutations={
            "weight_decay": lambda: tune.uniform(0.0, 0.3).func(None),
            "learning_rate": lambda: tune.uniform(1e-5, 5e-5).func(None),
            "per_gpu_train_batch_size": [16, 32, 64],

        })

    reporter = CLIReporter(
        parameter_columns=["weight_decay", "learning_rate", "per_gpu_train_batch_size", "num_epochs"],
        metric_columns=["mean_accuracy", "training_iteration"])

    tune.run(
        train_transformer,
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        name="tune_transformer_pbt")


# __tune_end__


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        tune_transformer(num_samples=1, num_epochs=1, gpus_per_trial=0)
    else:
        # You can change the number of GPUs here:
        tune_transformer(num_samples=8, num_epochs=3, gpus_per_trial=2)
