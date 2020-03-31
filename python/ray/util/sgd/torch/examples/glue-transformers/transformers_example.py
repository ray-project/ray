# coding=utf-8
# Copyright 2018 The Google AI Language Team Authors and The HuggingFace Inc. team.
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
""" Finetuning the library models for sequence classification on GLUE (Bert, XLM, XLNet, RoBERTa, Albert, XLM-RoBERTa)."""

import argparse
import glob
import logging
import os
import random

import numpy as np
import torch
from torch.utils.data import DataLoader, RandomSampler, SequentialSampler, TensorDataset
from tqdm import tqdm, trange
import torch.distributed as dist

from transformers import (
    MODEL_FOR_SEQUENCE_CLASSIFICATION_MAPPING,
    WEIGHTS_NAME,
    AdamW,
    AutoConfig,
    AutoModelForSequenceClassification,
    AutoTokenizer,
    get_linear_schedule_with_warmup,
)
from transformers import glue_compute_metrics as compute_metrics
from transformers import glue_convert_examples_to_features as convert_examples_to_features
from transformers import glue_output_modes as output_modes
from transformers import glue_processors as processors

from filelock import FileLock
from ray.util.sgd.torch import TrainingOperator

try:
    from apex import amp
except ImportError:
    amp = None

logger = logging.getLogger(__name__)

MODEL_CONFIG_CLASSES = list(MODEL_FOR_SEQUENCE_CLASSIFICATION_MAPPING.keys())
MODEL_TYPES = tuple(conf.model_type for conf in MODEL_CONFIG_CLASSES)

ALL_MODELS = sum(
    (tuple(conf.pretrained_config_archive_map.keys())
     for conf in MODEL_CONFIG_CLASSES),
    (),
)

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
    logger.info("  Instantaneous batch size per GPU = %d",
                args.per_gpu_train_batch_size)
    logger.info(
        "  Total train batch size (w. parallel, distributed & accumulation) = %d",
        args.per_gpu_train_batch_size * args.gradient_accumulation_steps * args.num_workers,
    )
    logger.info("  Gradient Accumulation steps = %d",
                args.gradient_accumulation_steps)
    logger.info("  Total optimization steps = %d", t_total)

def model_creator(config):
    with FileLock(os.path.expanduser("~/.download.lock")):
        args = config["args"]
        processor = processors[args.task_name]()
        label_list = processor.get_labels()
        num_labels = len(label_list)
        config = AutoConfig.from_pretrained(
            args.config_name if args.config_name else args.model_name_or_path,
            num_labels=num_labels,
            finetuning_task=args.task_name,
            cache_dir=args.cache_dir if args.cache_dir else None,
        )
        model = AutoModelForSequenceClassification.from_pretrained(
            args.model_name_or_path,
            from_tf=bool(".ckpt" in args.model_name_or_path),
            config=config,
            cache_dir=args.cache_dir if args.cache_dir else None,
        )
    return model


def create_optimizer(model, cfg):
    args = cfg["args"]
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

    return AdamW(
        optimizer_grouped_parameters,
        lr=args.learning_rate,
        eps=args.adam_epsilon)


def data_creator(config):
    args = config["args"]
    tokenizer = AutoTokenizer.from_pretrained(
        args.tokenizer_name
        if args.tokenizer_name else args.model_name_or_path,
        do_lower_case=args.do_lower_case,
        cache_dir=args.cache_dir if args.cache_dir else None,
    )
    config["tokenizer"] = tokenizer

    train_dataset = load_and_cache_examples(
        args, args.task_name, tokenizer, evaluate=False)
    train_sampler = RandomSampler(
        train_dataset) if not dist.is_initialized() else None
    return DataLoader(
        train_dataset,
        sampler=train_sampler,
        batch_size=args.per_gpu_train_batch_size)


class TransformerOperator(TrainingOperator):
    def setup(self, config):
        self.args = args = config["args"]
        args.device = torch.device("cuda" if torch.cuda.is_available()
                                   and not args.no_cuda else "cpu")
        self.tokenizer = config["tokenizer"]

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
        batch = tuple(t.to(args.device) for t in batch)
        inputs = {
            "input_ids": batch[0],
            "attention_mask": batch[1],
            "labels": batch[3]
        }
        if args.model_type != "distilbert":
            inputs["token_type_ids"] = (
                batch[2]
                if args.model_type in ["bert", "xlnet", "albert"] else None
            )  # XLM, DistilBERT, RoBERTa, and XLM-RoBERTa don't use segment_ids
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
        train_data_len = len(self.train_loader)
        if args.max_steps > 0:
            t_total = args.max_steps
            args.num_train_epochs = args.max_steps // (
                train_data_len // args.gradient_accumulation_steps) + 1
        else:
            t_total = train_data_len // args.gradient_accumulation_steps * args.num_train_epochs
        return t_total

    def validate(self, _):
        args = self.args
        model = self.model
        tokenizer = self.tokenizer
        return evaluate(args, model, tokenizer)


def evaluate(args, model, tokenizer, prefix=None):
    # Loop to handle MNLI double evaluation (matched, mis-matched)
    eval_task_names = ("mnli", "mnli-mm") if args.task_name == "mnli" else (
        args.task_name, )
    eval_outputs_dirs = (args.output_dir, args.output_dir + "-MM"
                         ) if args.task_name == "mnli" else (args.output_dir, )

    results = {}
    for eval_task, eval_output_dir in zip(eval_task_names, eval_outputs_dirs):
        eval_dataset = load_and_cache_examples(
            args, eval_task, tokenizer, evaluate=True)

        if not os.path.exists(eval_output_dir) and args.local_rank in [-1, 0]:
            os.makedirs(eval_output_dir)

        args.eval_batch_size = args.per_gpu_eval_batch_size
        # Note that DistributedSampler samples randomly
        eval_sampler = SequentialSampler(eval_dataset)
        eval_dataloader = DataLoader(
            eval_dataset,
            sampler=eval_sampler,
            batch_size=args.eval_batch_size)

        # Eval!
        logger.info("***** Running evaluation {} *****".format(prefix))
        logger.info("  Num examples = %d", len(eval_dataset))
        logger.info("  Batch size = %d", args.eval_batch_size)
        eval_loss = 0.0
        nb_eval_steps = 0
        preds = None
        out_label_ids = None
        for batch in tqdm(eval_dataloader, desc="Evaluating"):
            model.eval()
            batch = tuple(t.to(args.device) for t in batch)

            with torch.no_grad():
                inputs = {
                    "input_ids": batch[0],
                    "attention_mask": batch[1],
                    "labels": batch[3]
                }
                if args.model_type != "distilbert":
                    inputs["token_type_ids"] = (
                        batch[2]
                        if args.model_type in ["bert", "xlnet",
                                               "albert"] else None
                    )  # XLM, DistilBERT, RoBERTa, and XLM-RoBERTa don't use segment_ids
                outputs = model(**inputs)
                tmp_eval_loss, logits = outputs[:2]

                eval_loss += tmp_eval_loss.mean().item()
            nb_eval_steps += 1
            if preds is None:
                preds = logits.detach().cpu().numpy()
                out_label_ids = inputs["labels"].detach().cpu().numpy()
            else:
                preds = np.append(preds, logits.detach().cpu().numpy(), axis=0)
                out_label_ids = np.append(
                    out_label_ids,
                    inputs["labels"].detach().cpu().numpy(),
                    axis=0)

        eval_loss = eval_loss / nb_eval_steps
        if args.output_mode == "classification":
            preds = np.argmax(preds, axis=1)
        elif args.output_mode == "regression":
            preds = np.squeeze(preds)
        result = compute_metrics(eval_task, preds, out_label_ids)
        results.update(result)
    return results


def main():
    parser = argparse.ArgumentParser()
    add_transformers_parameters(parser)
    args = parser.parse_args()

    if (os.path.exists(args.output_dir) and os.listdir(args.output_dir)
            and args.do_train and not args.overwrite_output_dir):
        raise ValueError(
            "Output directory ({}) already exists and is not empty. "
            "Use --overwrite_output_dir to overcome.".format(args.output_dir))

    # Setup logging

    def init_hook():
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(name)s -   %(message)s",
            datefmt="%m/%d/%Y %H:%M:%S",
            level=logging.INFO,
        )
    # Set seed

    # Prepare GLUE task
    args.task_name = args.task_name.lower()
    if args.task_name not in processors:
        raise ValueError("Task not found: %s" % (args.task_name))
    args.output_mode = output_modes[args.task_name]

    logger.info("Training/evaluation parameters %s", args)
    from ray.util.sgd import TorchTrainer
    import ray
    ray.init(address="auto")
    # Training

    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=create_optimizer,
        training_operator_cls=TransformerOperator,
        initialization_hook=init_hook,
        use_fp16=args.fp16,
        num_workers=args.num_workers,
        use_gpu=True,
        use_tqdm=True,
        config={"args": args})
    if args.model_name_or_path:
        try:
            trainer.load(args.model_name_or_path)
        except Exception:
            logging.error(f"looks like {args.model_name_or_path} is not valid")

    global_step = 0
    epochs_trained = 0
    tr_loss = 0.0
    train_iterator = trange(
        epochs_trained,
        int(args.num_train_epochs),
        desc="Epoch",
    )

    trainer.apply_all_workers(lambda: set_seed(args))

    for _ in train_iterator:
        stats = trainer.train()
        print(stats)

    # Post-training validation
    save_and_evaluate_checkpoints(args, trainer.get_model(), None)


if __name__ == "__main__":
    main()
