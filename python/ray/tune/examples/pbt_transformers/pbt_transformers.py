# flake8: noqa
# yapf: disable

import os
from typing import Dict, Optional, Tuple

# Ray imports
import ray
from ray import tune
from ray.tune import CLIReporter
from ray.tune.schedulers import PopulationBasedTraining

import torch
from torch.utils.data import Dataset

# Transformer imports
import transformers
from transformers import (
    AutoConfig,
    AutoModelForSequenceClassification,
    AutoTokenizer,
    GlueDataset,
    GlueDataTrainingArguments as DataTrainingArguments,
    Trainer,
    TrainingArguments,
    glue_tasks_num_labels,
)

"""A Trainer class integrated with Tune.
The only changes to the original transformers.Trainer are:
    - Report eval metrics to Tune
    - Save state using Tune's checkpoint directories
    - Pass in extra wandb args
"""
class TuneTransformerTrainer(transformers.Trainer):
    def __init__(self, *args, wandb_args=None, **kwargs):
        self.wandb_args = wandb_args
        super().__init__(*args, **kwargs)

    def get_optimizers(
            self, num_training_steps: int
    ) -> Tuple[torch.optim.Optimizer, torch.optim.lr_scheduler.LambdaLR]:
        self.current_optimizer, self.current_scheduler = super(
        ).get_optimizers(num_training_steps)
        return (self.current_optimizer, self.current_scheduler)

    def evaluate(self,
                 eval_dataset: Optional[Dataset] = None) -> Dict[str, float]:
        eval_dataloader = self.get_eval_dataloader(eval_dataset)
        output = self._prediction_loop(
            eval_dataloader, description="Evaluation")
        self._log(output.metrics)

        tune.report(**output.metrics)

        self.save_state()

        return output.metrics

    def save_state(self):
        self.args.output_dir = tune.make_checkpoint_dir()
        output_dir = os.path.join(
            self.args.output_dir,
            f"{PREFIX_CHECKPOINT_DIR}-{self.global_step}")
        self.save_model(output_dir)
        if self.is_world_master():
            torch.save(self.current_optimizer.state_dict(),
                       os.path.join(output_dir, "optimizer.pt"))
            torch.save(self.current_scheduler.state_dict(),
                       os.path.join(output_dir, "scheduler.pt"))
        tune.save_checkpoint(output_dir)

    def _setup_wandb(self):
        if self.is_world_master() and self.wandb_args is not None:
            wandb.init(
                project=self.wandb_args["project_name"],
                name=self.wandb_args["run_name"],
                id=self.wandb_args["run_name"],
                config=vars(self.args),
                reinit=True,
                allow_val_change=True,
                resume=self.wandb_args["run_name"])
            # keep track of model topology and gradients, unsupported on TPU
            if not is_torch_tpu_available(
            ) and self.wandb_args["watch"] != "false":
                wandb.watch(
                    self.model,
                    log=self.wandb_args["watch"],
                    log_freq=max(100, self.args.logging_steps))


def get_trainer(model_name_or_path, train_dataset, eval_dataset, task_name, training_args, wandb_args=None):
    try:
        num_labels = glue_tasks_num_labels[task_name]
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
    tune_trainer = trainer.TuneTransformerTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        compute_metrics=utils.build_compute_metrics_fn(task_name),
        wandb_args=wandb_args
    )

    return tune_trainer

def get_datasets(config):
    data_args = DataTrainingArguments(
        task_name=config["task_name"],
        data_dir=config["data_dir"]
    )
    tokenizer = AutoTokenizer.from_pretrained(config["model_name"])
    train_dataset = GlueDataset(data_args, tokenizer=tokenizer, mode="train", cache_dir=config["data_dir"])
    eval_dataset = GlueDataset(data_args, tokenizer=tokenizer, mode="dev", cache_dir=config["data_dir"])
    eval_dataset = eval_dataset[:len(eval_dataset) // 2]
    return train_dataset, eval_dataset

def get_wandb_args():
    # Arguments for W&B.
    name = tune.get_trial_name()
    wandb_args = {
        "project_name": "transformers_pbt",
        "watch": "false",  # Either set to gradient, false, or all
        "run_name": name,
    }
    return wandb_args


# __train_begin__
def train_transformer(config, checkpoint=None):
    train_dataset, eval_dataset = get_datasets(config)

    training_args = TrainingArguments(
        output_dir=tune.get_trial_dir(),
        learning_rate=config["learning_rate"],
        do_train=True,
        do_eval=True,
        evaluate_during_training=True,
        eval_steps=(len(train_dataset) // config["per_gpu_train_batch_size"]) + 1,
        save_steps=0,  # We explicitly set save here to 0, and do saving in evaluate instead
        num_train_epochs=config["num_epochs"],
        max_steps=config["max_steps"],
        per_device_train_batch_size=config["per_gpu_train_batch_size"],
        per_device_eval_batch_size=config["per_gpu_val_batch_size"],
        warmup_steps=0,
        weight_decay=config["weight_decay"],
        logging_dir="./logs",
    )

    wandb_args = get_wandb_args()

    model_name_or_path = checkpoint if checkpoint is not None or len(checkpoint) > 0 else config["model_name"]
    task_name = config["task_name"]

    try:
        num_labels = glue_tasks_num_labels[task_name]
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
    tune_trainer = trainer.TuneTransformerTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        compute_metrics=utils.build_compute_metrics_fn(task_name),
        wandb_args=wandb_args
    )

    tune_trainer.train(model_name_or_path)


# __train_end__


# __tune_begin__
def tune_transformer(num_samples=8, gpus_per_trial=0, smoke_test=False):
    ray.init(address="auto" if not smoke_test else None, log_to_driver=False)
    data_dir = os.path.abspath(os.path.join(os.getcwd(), "./data"))
    if not os.path.exists(data_dir):
        os.mkdir(data_dir, 0o755)
    model_name = "bert-base-uncased"
    task_name = "rte"

    task_data_dir = os.path.join(data_dir, task_name.upper())

    # Download and cache tokenizer, model, and features
    print("Downloading and caching Tokenizer")

    # Triggers tokenizer download to cache
    AutoTokenizer.from_pretrained(model_name)
    print("Downloading and caching pre-trained model")

    # Triggers model download to cache
    AutoModelForSequenceClassification.from_pretrained(
        model_name,
    )

    # Download data.
    download_data(task_name, data_dir)

    config = {
        "model_name": model_name,
        "task_name": task_name,
        "data_dir": task_data_dir,
        "per_gpu_val_batch_size": 32,
        "per_gpu_train_batch_size": tune.choice([16, 32, 64]),
        "learning_rate": tune.uniform(1e-5, 5e-5),
        "weight_decay": tune.uniform(0.0, 0.3),
        "num_epochs": tune.choice([2, 3, 4, 5]),
        "max_steps": -1 if not smoke_test else 3,
    }

    scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        metric="eval_acc",
        mode="max",
        perturbation_interval=1,
        hyperparam_mutations={
            "weight_decay": lambda: tune.uniform(0.0, 0.3).func(None),
            "learning_rate": lambda: tune.uniform(1e-5, 5e-5).func(None),
            "per_gpu_train_batch_size": [16, 32, 64],
        })

    reporter = CLIReporter(
        parameter_columns={
            "weight_decay": "w_decay",
            "learning_rate": "lr",
            "per_gpu_train_batch_size": "train_bs/gpu",
            "num_epochs": "num_epochs"},
        metric_columns=["eval_acc", "eval_loss", "epoch", "training_iteration"])

    analysis = tune.run(
        train_transformer,
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        keep_checkpoints_num=3,
        checkpoint_score_attr="training_iteration",
        progress_reporter=reporter,
        name="tune_transformer_pbt")

    if not smoke_test:
        test_best_model(analysis, config["model_name"], config["task_name"], config["data_dir"])


# __tune_end__

def test_best_model(analysis, model_name, task_name, data_dir):
    data_args = DataTrainingArguments(
        task_name=task_name,
        data_dir=data_dir
    )

    tokenizer = AutoTokenizer.from_pretrained(model_name)

    best_config = analysis.get_best_config(metric="eval_acc", mode="max")
    print(best_config)
    best_checkpoint = analysis.get_best_trial(metric="eval_acc", mode="max").checkpoint.value
    print(best_checkpoint)
    best_model = AutoModelForSequenceClassification.from_pretrained(best_checkpoint).to("cuda")

    test_args = TrainingArguments(
        output_dir="./best_model_results",
    )
    test_dataset = GlueDataset(data_args, tokenizer=tokenizer, mode="dev", cache_dir=data_dir)
    test_dataset = test_dataset[len(test_dataset) // 2:]

    test_trainer = Trainer(best_model, test_args, compute_metrics=build_compute_metrics_fn(task_name))

    metrics = test_trainer.evaluate(test_dataset)
    print(metrics)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        tune_transformer(num_samples=1, gpus_per_trial=0, smoke_test=True)
    else:
        # You can change the number of GPUs here:
        tune_transformer(num_samples=8, gpus_per_trial=1)
