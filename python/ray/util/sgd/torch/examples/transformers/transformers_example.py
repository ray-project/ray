# coding=utf-8
# This is a modified example originally from The Google AI Language Team
# Authors and The HuggingFace Inc. team.
# Modified by Richard Liaw.
# Copyright 2018 The Google AI Language Team Authors,
# The HuggingFace Inc. team.
# Copyright (c) 2018, NVIDIA CORPORATION.  All rights reserved.
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
""" Finetuning the library models for sequence classification on GLUE (
Bert, XLM, XLNet, RoBERTa, Albert, XLM-RoBERTa)."""

import argparse
import logging
import json
import os
import time
from filelock import FileLock
from dataclasses import dataclass, field
from typing import Optional
import random

import numpy as np
import torch
from torch.utils.data import DataLoader, RandomSampler
from tqdm import trange
import torch.distributed as dist

from transformers import (MODEL_FOR_SEQUENCE_CLASSIFICATION_MAPPING,
                          ALL_PRETRAINED_CONFIG_ARCHIVE_MAP, AdamW, AutoConfig,
                          AutoModelForSequenceClassification, AutoTokenizer,
                          get_linear_schedule_with_warmup, HfArgumentParser,
                          TrainingArguments)
from transformers import glue_output_modes as output_modes
from transformers import glue_processors as processors

import ray
from ray.util.sgd.torch import TrainingOperator
from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch.examples.transformers.utils import (
    evaluate, load_and_cache_examples, save_and_evaluate_checkpoints)

try:
    from apex import amp
except ImportError:
    amp = None

MODEL_CONFIG_CLASSES = list(MODEL_FOR_SEQUENCE_CLASSIFICATION_MAPPING.keys())
MODEL_TYPES = tuple(conf.model_type for conf in MODEL_CONFIG_CLASSES)

ALL_MODELS = sum(
    (tuple(key for key in ALL_PRETRAINED_CONFIG_ARCHIVE_MAP.keys()
           if key.startswith(conf.model_type))
     for conf in MODEL_CONFIG_CLASSES),
    (),
)

logger = logging.getLogger(__name__)


def set_seed(args):
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)
    torch.cuda.manual_seed_all(args.seed)


def announce_training(args, dataset_len, t_total):
    # Train!
    logger.info("***** Running training *****")
    logger.info("  Num examples = %d", dataset_len)
    logger.info("  Num Epochs = %d", args.num_train_epochs)
    logger.info("  Instantaneous batch size per device = %d",
                args.per_device_train_batch_size)
    logger.info(
        "  Total train batch size (w. parallel, distributed & accum) = %d",
        args.per_device_train_batch_size * args.gradient_accumulation_steps *
        args.num_workers,
    )
    logger.info("  Gradient Accumulation steps = %d",
                args.gradient_accumulation_steps)
    logger.info("  Total optimization steps = %d", t_total)


class TransformerOperator(TrainingOperator):
    def setup(self, config):
        self.args = args = config["args"]
        start = time.time()
        self.tokenizer = AutoTokenizer.from_pretrained(
            args.tokenizer_name
            if args.tokenizer_name else args.model_name_or_path,
            cache_dir=args.cache_dir if args.cache_dir else None,
        )
        logger.info(f"tokenizer instantiation time: {time.time() - start}")

        # Load data.
        train_dataset = load_and_cache_examples(
            args, args.task_name, self.tokenizer, evaluate=False)
        train_sampler = RandomSampler(
            train_dataset) if not dist.is_initialized() else None
        train_loader = DataLoader(
            train_dataset,
            sampler=train_sampler,
            batch_size=args.per_device_train_batch_size)

        # Create model.
        with FileLock(os.path.expanduser("~/.download.lock")):
            processor = processors[args.task_name]()
            label_list = processor.get_labels()
            num_labels = len(label_list)
            model_config = AutoConfig.from_pretrained(
                args.config_name
                if args.config_name else args.model_name_or_path,
                num_labels=num_labels,
                finetuning_task=args.task_name,
                cache_dir=args.cache_dir if args.cache_dir else None,
            )
            model = AutoModelForSequenceClassification.from_pretrained(
                args.model_name_or_path,
                from_tf=bool(".ckpt" in args.model_name_or_path),
                config=model_config,
                cache_dir=args.cache_dir if args.cache_dir else None,
            )

        # Create optimizer.
        no_decay = ["bias", "LayerNorm.weight"]
        optimizer_grouped_parameters = [
            {
                "params": [
                    p for n, p in model.named_parameters()
                    if not any(nd in n for nd in no_decay)
                ],
                "weight_decay": args.weight_decay,
            },
            {
                "params": [
                    p for n, p in model.named_parameters()
                    if any(nd in n for nd in no_decay)
                ],
                "weight_decay": 0.0
            },
        ]

        optimizer = AdamW(
            optimizer_grouped_parameters,
            lr=args.learning_rate,
            eps=args.adam_epsilon)

        # Register components.
        self.model, self.optimizer = self.register(
            models=model,
            optimizers=optimizer,
            apex_args={"opt_level": args.fp16_opt_level})

        self.register_data(train_loader=train_loader, validation_loader=None)

        self.train_data_len = len(self.train_loader)
        self._warmup_scheduler = get_linear_schedule_with_warmup(
            self.optimizer,
            num_warmup_steps=args.warmup_steps,
            num_training_steps=self.calculate_t_total())
        self._global_step = 0

        announce_training(args, self.train_data_len, self.calculate_t_total())

    def train_batch(self, batch, batch_info=None):
        args = self.args
        model = self.model
        optimizer = self.optimizer
        step = batch_info["batch_idx"]

        model.train()
        batch = tuple(t.to(self.device) for t in batch)
        inputs = {
            "input_ids": batch[0],
            "attention_mask": batch[1],
            "labels": batch[3]
        }
        if args.model_type != "distilbert":
            # XLM, DistilBERT, RoBERTa, and XLM-RoBERTa don't use segment_ids
            inputs["token_type_ids"] = (batch[2] if args.model_type in [
                "bert", "xlnet", "albert"
            ] else None)
        outputs = model(**inputs)

        # model outputs are always tuple in transformers (see doc)
        loss = outputs[0]

        if args.gradient_accumulation_steps > 1:
            loss = loss / args.gradient_accumulation_steps

        if args.fp16:
            with amp.scale_loss(loss, optimizer) as scaled_loss:
                scaled_loss.backward()
        else:
            loss.backward()

        batch_loss = loss.item()

        # last step in epoch but step is always smaller
        # than gradient_accumulation_steps
        ending = (self.train_data_len <= args.gradient_accumulation_steps
                  and (step + 1) == self.train_data_len)
        if (step + 1) % args.gradient_accumulation_steps == 0 or ending:
            if args.fp16:
                torch.nn.utils.clip_grad_norm_(
                    amp.master_params(optimizer), args.max_grad_norm)
            else:
                torch.nn.utils.clip_grad_norm_(model.parameters(),
                                               args.max_grad_norm)

            self.optimizer.step()
            self._warmup_scheduler.step()  # Update learning rate schedule
            model.zero_grad()
            self._global_step += 1

        learning_rate_scalar = self._warmup_scheduler.get_lr()[0]
        return {"learning_rate": learning_rate_scalar, "loss": batch_loss}

    def calculate_t_total(self):
        args = self.args
        grad_accum_steps = args.gradient_accumulation_steps
        train_data_len = len(self.train_loader)
        if args.max_steps > 0:
            t_total = args.max_steps
            args.num_train_epochs = args.max_steps // (
                train_data_len // grad_accum_steps) + 1
        else:
            t_total = (
                train_data_len // grad_accum_steps * args.num_train_epochs)
        return t_total


@dataclass
class ModelArguments:
    """Arguments pertaining to model/config/tokenizer."""

    model_name_or_path: str = field(
        metadata=dict(help="Path to pre-trained model or shortcut name "
                      "selected in the list: " + ", ".join(ALL_MODELS)))
    model_type: str = field(
        metadata=dict(help="Model type selected "
                      "in the list: " + ", ".join(MODEL_TYPES)))
    config_name: Optional[str] = field(
        default=None,
        metadata=dict(
            help="Pretrained config name or path if not the same as model_name"
        ))
    tokenizer_name: Optional[str] = field(
        default=None,
        metadata=dict(help="Pretrained tokenizer name or path "
                      "if not the same as model_name"))
    cache_dir: Optional[str] = field(
        default=None,
        metadata=dict(help="Where do you want to store the pre-trained "
                      "models downloaded from s3"))


@dataclass
class DataProcessingArguments:
    task_name: str = field(
        metadata=dict(help="The name of the task to train selected "
                      "in the list: " + ", ".join(processors.keys())))
    data_dir: str = field(
        metadata=dict(help="The input data dir. Should contain "
                      "the .tsv files (or other data files) for the task."))
    max_seq_length: int = field(
        default=128,
        metadata=dict(help="The maximum total input sequence length "
                      "after tokenization. Sequences longer "
                      "than this will be truncated, sequences "
                      "shorter will be padded."))
    overwrite_cache: bool = field(
        default=False,
        metadata={"help": "Overwrite the cached training and evaluation sets"})


@dataclass
class RayArguments:
    num_workers: int = field(
        default=1,
        metadata={"help": "Number of data-parallel workers to use."})
    address: str = field(
        default=None,
        metadata={"help": "Address of the Ray cluster to connect to."})


def main():
    parser = HfArgumentParser((ModelArguments, DataProcessingArguments,
                               TrainingArguments, RayArguments))
    all_args = parser.parse_args_into_dataclasses()
    model_args, dataprocessing_args, training_args, ray_args = all_args

    # For now, let's merge all the sets of args into one,
    # but soon, we'll keep distinct sets of args, with a
    # cleaner separation of concerns.
    args = argparse.Namespace(**vars(model_args), **vars(dataprocessing_args),
                              **vars(training_args), **vars(ray_args))

    if (os.path.exists(args.output_dir) and os.listdir(args.output_dir)
            and args.do_train and not args.overwrite_output_dir):
        raise ValueError(
            "Output directory ({}) already exists and is not empty. "
            "Use --overwrite_output_dir to overcome.".format(args.output_dir))

    use_gpu = torch.cuda.is_available() and not args.no_cuda

    # Prepare GLUE task
    args.task_name = args.task_name.lower()
    if args.task_name not in processors:
        raise ValueError(f"Task not found: {args.task_name}")
    args.output_mode = output_modes[args.task_name]

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(name)s -   %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        level=logging.INFO)
    logger.info("Training/evaluation parameters %s", args)
    ray.init(address=args.address)
    # Training

    trainer = TorchTrainer(
        training_operator_cls=TransformerOperator,
        use_fp16=args.fp16,
        num_workers=args.num_workers,
        use_gpu=use_gpu,
        use_tqdm=True,
        config={"args": args})

    args.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    tokenizer = trainer.get_local_operator().tokenizer
    local_model = trainer.get_model()

    epochs_trained = 0
    train_iterator = trange(
        epochs_trained,
        int(args.num_train_epochs),
        desc="Epoch",
    )

    trainer.apply_all_workers(lambda: set_seed(args))
    if args.do_train:
        for _ in train_iterator:
            stats = trainer.train()
            print("Training stats:", stats)
            logs = evaluate(args, local_model, tokenizer)
            print(json.dumps(logs))

    # Post-training validation
    save_and_evaluate_checkpoints(args, local_model, tokenizer)


if __name__ == "__main__":
    main()
