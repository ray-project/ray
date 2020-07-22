# __import_begin__
import os
from dataclasses import dataclass, field
from functools import partial
from typing import Callable, Dict, Optional

import numpy as np
from ray.tune import CLIReporter
from ray.tune.schedulers import PopulationBasedTraining

from transformers import AutoConfig, AutoModelForSequenceClassification, AutoTokenizer, EvalPrediction, GlueDataset
from transformers import GlueDataTrainingArguments as DataTrainingArguments
from transformers import (
    Trainer,
    TrainingArguments,
    glue_compute_metrics,
    glue_output_modes,
    glue_tasks_num_labels,
)

from ray import tune
# __import_end__

# __huggingface_begin__
@dataclass
class ModelArguments:
    """
    Arguments pertaining to which model/config/tokenizer we are going to fine-tune from.
    """

    model_name_or_path: str = field(
        metadata={"help": "Path to pretrained model or model identifier from huggingface.co/models"}
    )
    config_name: Optional[str] = field(
        default=None, metadata={"help": "Pretrained config name or path if not the same as model_name"}
    )
    tokenizer_name: Optional[str] = field(
        default=None, metadata={"help": "Pretrained tokenizer name or path if not the same as model_name"}
    )
    cache_dir: Optional[str] = field(
        default=None, metadata={"help": "Where do you want to store the pretrained models downloaded from s3"}
    )


def get_trainer(data_args, model_args, training_args):
    try:
        num_labels = glue_tasks_num_labels[data_args.task_name]
        output_mode = glue_output_modes[data_args.task_name]
    except KeyError:
        raise ValueError("Task not found: %s" % (data_args.task_name))

    config = AutoConfig.from_pretrained(
        model_args.config_name if model_args.config_name else model_args.model_name_or_path,
        num_labels=num_labels,
        finetuning_task=data_args.task_name,
        cache_dir=model_args.cache_dir,
    )
    tokenizer = AutoTokenizer.from_pretrained(
        model_args.tokenizer_name if model_args.tokenizer_name else model_args.model_name_or_path,
        cache_dir=model_args.cache_dir,
    )
    model = AutoModelForSequenceClassification.from_pretrained(
        model_args.model_name_or_path,
        from_tf=bool(".ckpt" in model_args.model_name_or_path),
        config=config,
        cache_dir=model_args.cache_dir,
    )

    # Get datasets
    train_dataset = (
        GlueDataset(data_args, tokenizer=tokenizer, cache_dir=model_args.cache_dir) if training_args.do_train else None
    )
    eval_dataset = (
        GlueDataset(data_args, tokenizer=tokenizer, mode="dev", cache_dir=model_args.cache_dir)
        if training_args.do_eval
        else None
    )

    def build_compute_metrics_fn(task_name: str) -> Callable[[EvalPrediction], Dict]:
        def compute_metrics_fn(p: EvalPrediction):
            if output_mode == "classification":
                preds = np.argmax(p.predictions, axis=1)
            elif output_mode == "regression":
                preds = np.squeeze(p.predictions)
            metrics = glue_compute_metrics(task_name, preds, p.label_ids)
            tune.report(mean_accuracy=metrics["acc"])
            return metrics

        return compute_metrics_fn

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        compute_metrics=build_compute_metrics_fn(data_args.task_name)
    )
    return trainer
# __huggingface_end__


# __download_begin__
def download_data(model_name, task_name, data_dir="./data"):
    # Triggers tokenizer download to cache
    AutoTokenizer.from_pretrained(model_name)

    # Triggers model download to cache
    AutoModelForSequenceClassification.from_pretrained(
        model_name,
        from_tf=bool(".ckpt" in model_name)
    )

    # Download RTE training data
    import urllib
    import zipfile
    if task_name == "rte":
        url = "https://firebasestorage.googleapis.com/v0/b/mtl-sentence-representations.appspot.com/o/data%2FRTE.zip?alt=media&token=5efa7e85-a0bb-4f19-8ea2-9e1840f077fb"
    else:
        raise ValueError("Unknown task: {}".format(task_name))
    data_file = os.path.join(data_dir, "{}.zip".format(task_name))
    if not os.path.exists(data_file):
        urllib.request.urlretrieve(url, data_file)
        with zipfile.ZipFile(data_file) as zip_ref:
            zip_ref.extractall(data_dir)
        print("Downloaded data for task {} to {}".format(task_name, data_dir))
# __download_end__

# __train_begin__
def train_transformer(config, model_name, task_name, num_epochs=3, data_dir="./data"):
    data_args = DataTrainingArguments(
        task_name=task_name,
        data_dir=data_dir
    )
    model_args = ModelArguments(
        model_name_or_path=model_name
    )
    training_args = TrainingArguments(
        output_dir=tune.get_trial_dir(),
        learning_rate=config["lr"],
        do_train=True,
        do_eval=True,
        num_train_epochs=num_epochs,
        per_device_train_batch_size=config["batch_size"],
        per_device_eval_batch_size=config["batch_size"],
        warmup_steps=500,
        weight_decay=config["weight_decay"],
        logging_dir='./logs',
    )

    trainer = get_trainer(data_args, model_args, training_args)
    trainer.train(model_name)
# __train_end__


# __tune_begin__
def tune_transformer(num_samples=8, num_epochs=3, gpus_per_trial=0):
    data_dir = os.path.abspath(os.path.join(os.getcwd(), "./data"))
    if not os.path.exists(data_dir):
        os.mkdir(data_dir, 0o755)
    model_name = "bert-base-uncased"
    task_name = "rte"
    task_data_dir = os.path.join(data_dir, task_name.upper())

    download_data(model_name, task_name, data_dir)

    config = {
        "batch_size": 64,
        "lr": 1e-3,
        "weight_decay": 1e-2
    }

    scheduler = PopulationBasedTraining(
        metric="mean_accuracy",
        mode="max",
        perturbation_interval=4,
        hyperparam_mutations={
            "batch_size": [32, 64, 128],
            "lr": lambda: tune.loguniform(1e-4, 1e-1).func(None),
            "weight_decay": lambda: tune.loguniform(1e-3, 1e-1).func(None),
        })

    reporter = CLIReporter(
        parameter_columns=["weight_decay", "lr", "batch_size"],
        metric_columns=["mean_accuracy", "training_iteration"])

    tune.run(
        partial(
            train_transformer,
            model_name=model_name,
            task_name=task_name,
            data_dir=task_data_dir,
            num_epochs=num_epochs),
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
        tune_transformer(num_samples=8, num_epochs=3, gpus_per_trial=0)
