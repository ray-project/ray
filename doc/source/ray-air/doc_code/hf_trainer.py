# __hf_trainer_start__

# Based on
# huggingface/notebooks/examples/language_modeling_from_scratch.ipynb

# Hugging Face imports
from datasets import load_dataset
import transformers
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer

import ray
from ray.train.huggingface import HuggingFaceTrainer
from ray.air.config import ScalingConfig

model_checkpoint = "gpt2"
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
    concatenated_examples = {k: sum(examples[k], []) for k in examples.keys()}
    total_length = len(concatenated_examples[list(examples.keys())[0]])
    # We drop the small remainder, we could add padding if the model
    # supported it.
    # instead of this drop, you can customize this part to your needs.
    total_length = (total_length // block_size) * block_size
    # Split by chunks of max_len.
    result = {
        k: [t[i : i + block_size] for i in range(0, total_length, block_size)]
        for k, t in concatenated_examples.items()
    }
    result["labels"] = result["input_ids"].copy()
    return result


lm_datasets = tokenized_datasets.map(
    group_texts,
    batched=True,
    batch_size=1000,
    num_proc=1,
)
ray_train_ds = ray.data.from_huggingface(lm_datasets["train"])
ray_evaluation_ds = ray.data.from_huggingface(lm_datasets["validation"])


def trainer_init_per_worker(train_dataset, eval_dataset, **config):
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    args = transformers.TrainingArguments(
        output_dir=f"{model_checkpoint}-wikitext2",
        evaluation_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="epoch",
        learning_rate=2e-5,
        weight_decay=0.01,
        no_cuda=True,  # Set to False for GPU training
    )
    return transformers.Trainer(
        model=model,
        args=args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
    )


scaling_config = ScalingConfig(num_workers=3)
# If using GPUs, use the below scaling config instead.
# scaling_config = ScalingConfig(num_workers=3, use_gpu=True)
trainer = HuggingFaceTrainer(
    trainer_init_per_worker=trainer_init_per_worker,
    scaling_config=scaling_config,
    datasets={"train": ray_train_ds, "evaluation": ray_evaluation_ds},
)
result = trainer.fit()

# __hf_trainer_end__
