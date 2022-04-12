# Based on
# huggingface/notebooks/examples/language_modeling_from_scratch.ipynb

from datasets import load_dataset
from transformers import (
    AutoTokenizer,
    AutoConfig,
    AutoModelForCausalLM,
    Trainer,
    TrainingArguments,
)

import ray
import ray.data
from ray.ml.train.integrations.huggingface import HuggingFaceTrainer

model_checkpoint = "gpt2"
tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"

# block_size = tokenizer.model_max_length
block_size = 128


def get_dataset():
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
        # We drop the small remainder, we could add padding if the model supported it instead of this drop, you can
        # customize this part to your needs.
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
    return lm_datasets


lm_dataset = get_dataset()
ray_train = ray.data.from_arrow(lm_dataset["train"]._data.table)
ray_validation = ray.data.from_arrow(lm_dataset["validation"]._data.table)


def train_function(train_dataset, eval_dataset=None, **config):
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    print("Initializing TrainingArguments...")
    training_args = TrainingArguments(
        f"{model_checkpoint}-wikitext2",
        evaluation_strategy="epoch",
        num_train_epochs=2,
        learning_rate=2e-5,
        weight_decay=0.01,
        disable_tqdm=True,
        save_strategy="epoch",
    )
    print("Initializing Trainer...")
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
    )
    print("Trainer initialized! Starting training...")
    return trainer


trainer = HuggingFaceTrainer(
    trainer_init_per_worker=train_function,
    scaling_config={"num_workers": 2, "use_gpu": False},
    datasets={"train": ray_train.limit(16), "evaluation": ray_validation.limit(8)},
)
results = trainer.fit()
print(results.metrics)
