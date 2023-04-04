#!/usr/bin/env python
# coding=utf-8
# Copyright 2021 The HuggingFace Team All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Pre-training/Fine-tuning the library models for causal language modeling
(GPT, GPT-2, CTRL, ...) on a text file or a dataset.

Here is the full list of checkpoints on the hub that can be fine-tuned by this script:
https://huggingface.co/models?filter=text-generation
"""

from dataclasses import dataclass, field
import functools
from itertools import chain
import json
import logging
import os
from statistics import mean
import time
from typing import Optional

import datasets
from datasets import Dataset, load_dataset
import numpy as np
from tqdm import tqdm

import alpa
from alpa.global_env import global_config
from alpa.model.model_util import DynamicScale, TrainState
import jax
import jax.numpy as jnp
import optax
import transformers
import tensorflow as tf
from transformers import (
    FLAX_MODEL_FOR_CAUSAL_LM_MAPPING,
    AutoConfig,
    AutoTokenizer,
    FlaxAutoModelForCausalLM,
    HfArgumentParser,
)


logger = logging.getLogger(__name__)

MODEL_CONFIG_CLASSES = list(FLAX_MODEL_FOR_CAUSAL_LM_MAPPING.keys())
MODEL_TYPES = tuple(conf.model_type for conf in MODEL_CONFIG_CLASSES)


def setup_logging():
    # Make one log on every process with the configuration for debugging.
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        level=logging.INFO,
    )

    # Setup logging, we only want one process per machine to log things on the screen.
    logger.setLevel(logging.INFO)

    datasets.utils.logging.set_verbosity_warning()
    # Set the verbosity to info of the Transformers logger (on main process only):
    transformers.utils.logging.set_verbosity_info()


@dataclass
class TrainingArguments:
    output_dir: str = field(
        metadata={
            "help": "The output directory where the model and checkpoints are saved."
        },
    )
    per_device_train_batch_size: int = field(
        default=1, metadata={"help": "Batch size per GPU/TPU core/CPU for training."}
    )
    num_micro_batches: int = field(
        default=1,
        metadata={"help": "The number of micro batches for gradient accumulation."},
    )
    operator_parallel: int = field(
        default=1, metadata={"help": "The degree of operator model parallelism."}
    )
    pipeline_parallel: int = field(
        default=1, metadata={"help": "The degree of pipeline model parallelism."}
    )
    learning_rate: float = field(
        default=5e-5, metadata={"help": "The initial learning rate for AdamW."}
    )
    num_train_epochs: int = field(
        default=1, metadata={"help": "Total number of training epochs to perform."}
    )
    logging_steps: int = field(
        default=10, metadata={"help": "Log every X updates steps."}
    )
    save_steps: int = field(
        default=100, metadata={"help": "Save checkpoint every X updates steps."}
    )


@dataclass
class ModelArguments:
    """
    Arguments pertaining to which model/config/tokenizer we are going to fine-tune,
    or train from scratch.
    """

    model_name_or_path: Optional[str] = field(
        metadata={"help": "The model checkpoint for weights initialization."},
    )
    model_type: Optional[str] = field(
        default=None,
        metadata={
            "help": "If training from scratch, pass a model type from the list: "
            + ", ".join(MODEL_TYPES)
        },
    )
    tokenizer_name: Optional[str] = field(
        default=None,
        metadata={
            "help": "Pretrained tokenizer name or path if not the same as model_name"
        },
    )


@dataclass
class DataTrainingArguments:
    """Arguments pertaining to what data we are going to use for training and eval."""

    train_file: Optional[str] = field(
        metadata={"help": "The input training data file (a text file)."}
    )
    max_train_samples: Optional[int] = field(
        default=None,
        metadata={
            "help": (
                "For debugging purposes or quicker training, truncate the number of "
                "training examples to this value if set."
            )
        },
    )
    block_size: Optional[int] = field(
        default=1024,
        metadata={
            "help": (
                "Optional input sequence length after tokenization. "
                "The training dataset will be truncated in block of this size. "
                "Default to the model max input length for single sentence inputs "
                "(take into account special tokens)."
            )
        },
    )
    preprocessing_num_workers: Optional[int] = field(
        default=1,
        metadata={"help": "The number of processes to use for the preprocessing."},
    )


def data_loader(dataset: Dataset, batch_size: int, shuffle: bool = False):
    """Returns batches of size `batch_size` from truncated `dataset`,
    sharded over all local devices. Shuffle batches if `shuffle` is `True`.
    """
    data_collator = transformers.DefaultDataCollator("np")
    tf_dataset = dataset.to_tf_dataset(
        batch_size=batch_size,
        columns=dataset.column_names,
        collate_fn=data_collator,
        shuffle=shuffle,
        drop_remainder=True,
    )

    for batch in tf_dataset:
        batch = {k: v._numpy() for k, v in batch.items()}
        yield batch


# Main data processing function that will concatenate all texts from
# our dataset and generate chunks of block_size.
def group_texts(block_size, examples):
    # Concatenate all texts.
    concatenated_examples = {k: list(chain(*v)) for k, v in examples.items()}
    # Length of first concatenated example.
    total_length = len(next(iter(concatenated_examples.values())))
    # We drop the small remainder, we could add padding if the model supported
    # it instead of this drop, you can customize this part to your needs.
    if total_length >= block_size:
        total_length = (total_length // block_size) * block_size
    # Split by chunks of max_len.
    result = {
        k: [t[i : i + block_size] for i in range(0, total_length, block_size)]
        for k, t in concatenated_examples.items()
    }
    result["labels"] = result["input_ids"].copy()
    return result


def preprocess(tokenizer, dataset, data_args):
    """Tokenize a single dataset."""
    text_column_name = (
        "text" if "text" in dataset.column_names else dataset.column_names[0]
    )

    print("Tokenize dataset ...")
    tokenized_dataset = dataset.map(
        lambda row: tokenizer(row[text_column_name]),
        # Note that with `batched=True`, this map processes BLOCK_SIZE
        # of texts together, and throws away a remainder for each of
        # those grouped texts.
        batched=True,
        num_proc=data_args.preprocessing_num_workers,
        remove_columns=dataset.column_names,
        load_from_cache_file=False,
    )

    print("Build dataset ...")
    block_size = min(data_args.block_size, tokenizer.model_max_length)
    lm_dataset = tokenized_dataset.map(
        functools.partial(group_texts, block_size),
        batched=True,
        num_proc=data_args.preprocessing_num_workers,
        load_from_cache_file=False,
    )

    if data_args.max_train_samples > 0:
        max_samples = min(len(dataset), data_args.max_train_samples)
        lm_dataset = lm_dataset.select(range(max_samples))

    return lm_dataset


def build_datasets(tokenizer, data_args):
    # TODO(jungong) : replace huggingface dataset with Ray dataset.
    # Manually create train split.
    dataset = load_dataset(
        "text",
        data_files={
            "train": data_args.train_file,
        },
        keep_linebreaks=False,
    )

    train_dataset = preprocess(tokenizer, dataset["train"], data_args)

    return train_dataset


# Define gradient update step fn
def train_step(state, batch):
    """Main training step function."""

    def loss_fn(logits, labels):
        shift_logits = logits[..., :-1, :]
        shift_labels = labels[..., 1:]
        loss = optax.softmax_cross_entropy(
            shift_logits, jax.nn.one_hot(shift_labels, logits.shape[-1])
        )
        return loss.mean()

    def compute_loss(params):
        labels = batch.pop("labels")
        logits = state.apply_fn(**batch, params=params, deterministic=True)[0]
        loss = loss_fn(logits, labels)
        return loss

    dynamic_scale = state.dynamic_scale
    grad_fn = dynamic_scale.value_and_grad(compute_loss)
    dynamic_scale, is_fin, loss, grads = grad_fn(state.params)

    new_state = state.apply_gradients(grads=grads)
    new_state = new_state.replace(
        opt_state=jax.tree_map(
            functools.partial(jnp.where, is_fin),
            new_state.opt_state,
            state.opt_state,
        ),
        params=jax.tree_map(
            functools.partial(jnp.where, is_fin), new_state.params, state.params
        ),
        master_copy=jax.tree_map(
            functools.partial(jnp.where, is_fin),
            new_state.master_copy,
            state.master_copy,
        ),
        dynamic_scale=dynamic_scale,
    )

    metrics = {"loss": loss}

    return new_state, metrics


def save_checkpoint(state, model, tokenizer, training_args):
    """Util to checkpoint model in output_dir."""
    alpa.prefetch(state.params)
    params = alpa.util.map_to_nparray(state.params)
    model.save_pretrained(training_args.output_dir, params=params)
    tokenizer.save_pretrained(training_args.output_dir)


def log_metrics(
    config, epochs, metrics_to_report, batch, latency, epoch, step, train_metric
):
    """Log metrics to stdout."""
    throughput_tokens = np.prod(batch["input_ids"].shape) / latency
    throughput_tflops = alpa.util.compute_gpt_tflops(
        batch_size=batch["input_ids"].shape[0],
        seq_len=batch["input_ids"].shape[1],
        num_layers=config.num_hidden_layers,
        hidden_size=config.hidden_size,
        vocab_size=config.vocab_size,
        num_gpus=alpa.get_global_num_devices(),
        latency=latency,
    )

    train_metric = jax.tree_map(np.mean, train_metric)

    # Metrics we report from the release test.
    metrics_to_report["tokens"].append(throughput_tokens)
    metrics_to_report["tflops"].append(throughput_tflops)

    epochs.write(
        f"Epoch: {epoch} | "
        f"Step: {step} | "
        f"Loss: {train_metric['loss'].mean():.4f}, "
        f"Throughput: {throughput_tokens:.2f} token/s, "
        f"{throughput_tflops:.2f} TFLOP/s"
    )


def save_json_metrics(metrics):
    # Skip the first couple of data points for a more accurate throughput.
    to_report = {
        "throughput_tokens": (
            mean(metrics["tokens"][2:]) if len(metrics["tokens"]) > 2 else 0.0
        ),
        "throughput_tflops": (
            mean(metrics["tflops"][2:]) if len(metrics["tflops"]) > 2 else 0.0
        ),
    }
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/alpa_opt_2_7b_sanity_check.json"
    )

    print("Writing metrics: ", to_report, f" to {test_output_json}")

    with open(test_output_json, "wt") as f:
        json.dump(to_report, f)


def main():
    # Global initialization.
    alpa.init(cluster="ray")

    tf.config.experimental.set_visible_devices([], "GPU")

    # "cupy" doesn't really work, use "xla_extension" instead.
    global_config.nccl_mode = "xla_extension"

    # See all possible arguments in src/transformers/training_args.py
    # or by passing the --help flag to this script.
    # We now keep distinct sets of args, for a cleaner separation of concerns.
    parser = HfArgumentParser(
        (ModelArguments, DataTrainingArguments, TrainingArguments)
    )
    model_args, data_args, training_args = parser.parse_args_into_dataclasses()

    if os.path.exists(training_args.output_dir) and os.listdir(
        training_args.output_dir
    ):
        raise ValueError(
            f"Directory ({training_args.output_dir}) already exists and is not empty."
        )

    logger.info(f"Training/evaluation parameters {training_args}")

    setup_logging()

    # Load pretrained model and tokenizer

    # Distributed training:
    config = AutoConfig.from_pretrained(model_args.model_name_or_path)
    tokenizer = AutoTokenizer.from_pretrained(
        model_args.model_name_or_path,
        use_fast=True,
    )

    assert model_args.model_name_or_path, "model_name_or_path is required"
    model = FlaxAutoModelForCausalLM.from_pretrained(
        model_args.model_name_or_path,
        config=config,
        dtype=getattr(jnp, "float16"),
        use_auth_token=None,
    )

    # Training dataset.
    train_dataset = build_datasets(tokenizer, data_args)

    # Adjust batch size and num_micro_batches for small datasets
    num_devices = alpa.get_global_num_devices()
    # Store some constant
    num_epochs = training_args.num_train_epochs
    data_parallel = num_devices // (
        training_args.operator_parallel * training_args.pipeline_parallel
    )
    train_batch_size = training_args.per_device_train_batch_size * data_parallel
    steps_per_epoch = len(train_dataset) // train_batch_size
    total_train_steps = steps_per_epoch * num_epochs

    # create adam optimizer
    optimizer = optax.chain(
        optax.clip_by_global_norm(1.0),
        optax.adamw(learning_rate=training_args.learning_rate),
    )

    # Setup train state
    state = TrainState.create(
        apply_fn=model.__call__,
        params=model.params,
        tx=optimizer,
        dynamic_scale=DynamicScale(),
        use_master_copy=True,
    )

    # Create parallel version of the train and eval step
    method = alpa.get_3d_parallel_method(
        num_micro_batches=training_args.num_micro_batches,
        data_parallel=-1,
        operator_parallel=training_args.operator_parallel,
        pipeline_parallel=training_args.pipeline_parallel,
    )

    p_train_step = alpa.parallelize(train_step, method=method, donate_argnums=(0,))

    logger.info("***** Training *****")
    logger.info(f"  Num examples = {len(train_dataset)}")
    logger.info(f"  Num Epochs = {num_epochs}")
    logger.info(
        "  Batch size per device (w. accumulation) = "
        f"{training_args.per_device_train_batch_size}"
    )
    logger.info(
        f"  Global train batch size (w. parallel & distributed) = {train_batch_size}"
    )
    logger.info(f"  Total optimization steps = {total_train_steps}")
    logger.info(f"  NCCL mode = {global_config.nccl_mode}")

    step_ct = 0
    last_time = 0

    epochs = tqdm(range(num_epochs), desc="Epoch ... ", position=0)
    epochs.write("Initial compilation. This might take some minutes...")

    # Track and report throughput per iteration. These are the metrics we
    # care about over time.
    metrics_to_report = {
        "tokens": [],
        "tflops": [],
    }
    for epoch in epochs:
        # Generate an epoch by shuffling sampling indices from the train dataset
        train_loader = data_loader(train_dataset, train_batch_size, shuffle=True)

        last_time = time.time()
        for step in tqdm(
            range(steps_per_epoch), desc="Training...", position=1, leave=False
        ):
            batch = next(train_loader)
            batch["position_ids"] = (
                batch["attention_mask"].cumsum(axis=1) * batch["attention_mask"]
            ) - 1
            state, train_metric = p_train_step(state, batch)

            cur_step = epoch * steps_per_epoch + step

            step_ct += 1
            if cur_step % training_args.logging_steps == 0 and cur_step > 0:
                latency = (time.time() - last_time) / step_ct

                log_metrics(
                    config,
                    epochs,
                    metrics_to_report,
                    batch,
                    latency,
                    epoch,
                    cur_step,
                    train_metric,
                )

                step_ct = 0
                last_time = time.time()

            if cur_step % training_args.save_steps == 0 and cur_step > 0:
                # save checkpoint after each epoch
                epochs.write("\nSave checkpoint...")
                save_checkpoint(state, model, tokenizer, training_args)

    # Save the final model
    epochs.write("\nSave the final model...")
    save_checkpoint(state, model, tokenizer, training_args)

    # Save JSON metrics
    save_json_metrics(metrics_to_report)


if __name__ == "__main__":
    main()
