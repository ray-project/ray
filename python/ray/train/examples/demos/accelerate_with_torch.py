import os

import evaluate
import torch
from datasets import load_dataset
from torch.distributed.fsdp.fully_sharded_data_parallel import (
    CPUOffload,
    FullStateDictConfig,
)
from torch.utils.data import DataLoader
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    get_linear_schedule_with_warmup,
    set_seed,
)
from tqdm.auto import tqdm
from accelerate import Accelerator, FullyShardedDataParallelPlugin

from ray.air import session
from ray.train.torch import TorchTrainer, TorchCheckpoint
from ray.air.config import RunConfig, ScalingConfig, CheckpointConfig


MAX_GPU_BATCH_SIZE = 16
EVAL_BATCH_SIZE = 32
MODEL_NAME = "gpt2"
CHECKPOINT_DIR = "./ckpts"


def prepare_dataloaders(datasets, batch_size):
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    tokenizer.pad_token = tokenizer.eos_token

    def tokenize_function(examples):
        # max_length=None => use the model max length (it's actually the default)
        outputs = tokenizer(
            examples["sentence1"],
            examples["sentence2"],
            truncation=True,
            max_length=None,
        )
        return outputs

    # Apply the method we just defined to all the examples in all the splits of the dataset
    tokenized_datasets = datasets.map(
        tokenize_function,
        batched=True,
        remove_columns=["idx", "sentence1", "sentence2"],
    )

    # We also rename the 'label' column to 'labels' which is the expected name for labels by the models of the
    # transformers library
    tokenized_datasets = tokenized_datasets.rename_column("label", "labels")

    def collate_fn(examples):
        return tokenizer.pad(
            examples,
            padding="longest",
            max_length=128,
            pad_to_multiple_of=8,
            return_tensors="pt",
        )

    # Instantiate dataloaders.
    train_dataloader = DataLoader(
        tokenized_datasets["train"],
        shuffle=True,
        collate_fn=collate_fn,
        batch_size=batch_size,
    )
    eval_dataloader = DataLoader(
        tokenized_datasets["validation"],
        shuffle=False,
        collate_fn=collate_fn,
        batch_size=EVAL_BATCH_SIZE,
    )
    return train_dataloader, eval_dataloader


def train_loop_per_worker(config):
    # Extract hyper-parameters from config dict
    lr = config["lr"]
    num_epochs = int(config["num_epochs"])
    seed = int(config["seed"])
    batch_size = int(config["batch_size"])
    gradient_accumulation_steps = 1

    # Prepare data
    datasets = load_dataset("glue", "mrpc")
    metric = evaluate.load("glue", "mrpc")
    train_dataloader, eval_dataloader = prepare_dataloaders(datasets, batch_size)

    # Initialize accelerator
    # Pass the advanced FSDP settings not part of the accelerate config by creating fsdp_plugin
    fsdp_plugin = FullyShardedDataParallelPlugin(
        state_dict_config=FullStateDictConfig(offload_to_cpu=False, rank0_only=False),
        cpu_offload=CPUOffload(offload_params=False),
    )
    accelerator = Accelerator(
        mixed_precision="fp16",
        log_with="wandb",
        project_dir="./project_dir",
        fsdp_plugin=fsdp_plugin,
    )
    accelerator.print(accelerator.distributed_type)

    # Instantiate the model (we build the model here so that the seed also control new weights initialization)
    set_seed(seed)
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_NAME, return_dict=True, low_cpu_mem_usage=True
    )
    model.config.pad_token_id = model.config.eos_token_id

    # For FSDP feature, it is highly recommended and efficient to prepare the model before creating optimizer
    model = accelerator.prepare(model)
    accelerator.print(model)

    # Instantiate optimizer
    # For FSDP feature, at present it doesn't support multiple parameter groups,
    # so we need to create a single parameter group for the whole model
    optimizer = torch.optim.AdamW(params=model.parameters(), lr=lr, weight_decay=2e-4)

    # Instantiate scheduler
    lr_scheduler = get_linear_schedule_with_warmup(
        optimizer=optimizer,
        num_warmup_steps=10,
        num_training_steps=(len(train_dataloader) * num_epochs)
        // gradient_accumulation_steps,
    )

    # For FSDP feature, prepare everything except the model as we have already prepared the model
    # before creating the optimizer
    # There is no specific order to remember, we just need to unpack the objects in the same order we gave them to the
    # prepare method.
    optimizer, train_dataloader, eval_dataloader, lr_scheduler = accelerator.prepare(
        optimizer, train_dataloader, eval_dataloader, lr_scheduler
    )

    overall_step = 0

    # Now we train the model
    for epoch in range(num_epochs):
        total_loss = 0
        # Training Loop
        model.train()
        for step, batch in enumerate(
            tqdm(train_dataloader, disable=not accelerator.is_main_process)
        ):
            outputs = model(**batch)
            loss = outputs.loss
            loss = loss / gradient_accumulation_steps
            total_loss += loss.detach().float()

            accelerator.backward(loss)
            if step % gradient_accumulation_steps == 0:
                optimizer.step()
                lr_scheduler.step()
                optimizer.zero_grad()

            overall_step += 1

        # Validation Loop
        model.eval()
        for step, batch in enumerate(eval_dataloader):
            with torch.no_grad():
                outputs = model(**batch)
            predictions = outputs.logits.argmax(dim=-1)
            predictions, references = accelerator.gather_for_metrics(
                (predictions, batch["labels"])
            )
            metric.add_batch(
                predictions=predictions,
                references=references,
            )

        eval_metric = metric.compute()
        accelerator.print(f"epoch {epoch}:", eval_metric)

        output_dir = os.path.join(CHECKPOINT_DIR, f"epoch_{epoch}")
        output_dir = os.path.abspath(output_dir)
        accelerator.save_state(output_dir)

        # Report at each epoch end
        checkpoint = TorchCheckpoint.from_directory(output_dir)
        session.report(metrics=eval_metric, checkpoint=checkpoint)


if __name__ == "__main__":
    config = {"lr": 2e-5, "num_epochs": 3, "seed": 42, "batch_size": 16}

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=config,
        run_config=RunConfig(
            name="accelerate_with_torch",
            checkpoint_config=CheckpointConfig(
                num_to_keep=2,
                checkpoint_score_attribute="accuracy",
                checkpoint_score_order="max",
                _checkpoint_keep_all_ranks=True,
            ),
        ),
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
    )
    trainer.fit()
