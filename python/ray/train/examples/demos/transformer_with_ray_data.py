# Based on
# huggingface/notebooks/examples/language_modeling_from_scratch.ipynb

# Hugging Face imports
from datasets import load_dataset
from ray.train.torch import TorchTrainer
import transformers
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer, default_data_collator

import ray
from ray.train.huggingface import TransformersTrainer
from ray.air.config import ScalingConfig
from ray.train.torch import RayIterableDataset
from ray.air import session

# If using GPUs, set this to True.
use_gpu = True

tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"
block_size = 128
datasets = load_dataset("wikitext", "wikitext-2-raw-v1")
tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)

def tokenize_function(examples):
    return tokenizer(examples["text"])

tokenized_datasets = datasets.map(
    tokenize_function, batched=True, num_proc=1, remove_columns=["text"]
)

def group_texts(examples):
    # Concatenate all texts.
    concatenated_examples = {
        k: sum(examples[k], []) for k in examples.keys()
    }
    total_length = len(concatenated_examples[list(examples.keys())[0]])
    # We drop the small remainder, we could add padding if the model
    # supported it.
    # instead of this drop, you can customize this part to your needs.
    total_length = (total_length // block_size) * block_size
    # Split by chunks of max_len.
    result = {
        k: [
            t[i : i + block_size]
            for i in range(0, total_length, block_size)
        ]
        for k, t in concatenated_examples.items()
    }
    result["labels"] = result["input_ids"].copy()
    return result

hf_dataset = tokenized_datasets.map(
    group_texts,
    batched=True,
    batch_size=1000,
    num_proc=1,
)

def train_loop_per_worker(config):
    model_checkpoint = "gpt2"
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    args = transformers.TrainingArguments(
        output_dir=f"{model_checkpoint}-wikitext2",
        evaluation_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="epoch",
        learning_rate=2e-5,
        weight_decay=0.01,
        no_cuda=(not use_gpu),
        # Take a small subset for doctest
        max_steps=100,
    )

    train_ds = RayIterableDataset(session.get_dataset_shard("train"))
    eval_ds = RayIterableDataset(session.get_dataset_shard("validation"))

    trainer = transformers.Trainer(
        model=model,
        args=args,
        train_dataset=train_ds,
        eval_dataset=eval_ds,
        data_collator=default_data_collator,
    )

    # Diff
    trainer = ray.train.huggingface.transformers.prepare_trainer(trainer)
    trainer.train()

scaling_config = ScalingConfig(num_workers=4, use_gpu=use_gpu)
trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=scaling_config,
    datasets={
        "train": ray.data.from_huggingface(hf_dataset["train"]),
        "validation": ray.data.from_huggingface(hf_dataset["validation"]),
    }
)
result = trainer.fit()