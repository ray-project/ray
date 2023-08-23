# Based on
# huggingface/notebooks/examples/language_modeling_from_scratch.ipynb

# This example is tested with transformers==4.19.1

import argparse
import tempfile

import torch
from datasets import load_dataset
from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoTokenizer,
    Trainer,
    TrainingArguments,
)

import ray
import ray.data
from ray.train.huggingface import TransformersTrainer
from ray.train import ScalingConfig


def main(
    model_checkpoint="gpt2",
    tokenizer_checkpoint="sgugger/gpt2-like-tokenizer",
    dataset_name="wikitext-2-raw-v1",
    dataset_path="wikitext",
    num_epochs=5,
    num_workers=2,
    use_gpu=False,
    smoke_test=False,
):
    block_size = 128

    # Uncomment the following if the maximum length the model was
    # pretrained with can fit in your memory.
    # block_size = tokenizer.model_max_length

    # Run this as a remote function to avoid downloading on the driver
    @ray.remote
    def get_dataset():
        datasets = load_dataset(dataset_path, dataset_name)
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
            # We drop the small remainder. We could add padding if the model supported
            # it instead of this drop. You can customize this part to your needs.
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
        ray_train = ray.data.from_huggingface(lm_datasets["train"])
        ray_validation = ray.data.from_huggingface(lm_datasets["validation"])
        return ray_train, ray_validation

    ray_train, ray_validation = ray.get(get_dataset.remote())

    def train_function(train_dataset, eval_dataset=None, **config):
        model_config = AutoConfig.from_pretrained(model_checkpoint)
        model = AutoModelForCausalLM.from_config(model_config)
        print("Initializing TrainingArguments...")
        # The checkpoints will be moved to Ray Tune results
        # directory automatically
        training_dir = tempfile.mkdtemp()
        training_args = TrainingArguments(
            training_dir,
            evaluation_strategy="epoch",
            save_strategy="epoch",
            logging_strategy="epoch",
            num_train_epochs=num_epochs,
            learning_rate=2e-5,
            weight_decay=0.01,
            disable_tqdm=True,
            # Required to avoid an exception
            no_cuda=not torch.cuda.is_available(),
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

    if smoke_test:
        ray_train = ray_train.limit(16)
        ray_validation = ray_validation.limit(8)

    # Materialize the datasets so that they will have __len__.
    ray_train = ray_train.materialize()
    ray_validation = ray_validation.materialize()

    trainer = TransformersTrainer(
        trainer_init_per_worker=train_function,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        datasets={"train": ray_train, "evaluation": ray_validation},
    )
    results = trainer.fit()
    print(results.metrics)


if __name__ == "__main__":
    # Training settings
    parser = argparse.ArgumentParser(
        description="Language modelling from scratch with TransformersTrainer Example",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--model-checkpoint",
        type=str,
        default="gpt2",
        help="Model checkpoint name to download from HF hub",
    )
    parser.add_argument(
        "--tokenizer-checkpoint",
        type=str,
        default="sgugger/gpt2-like-tokenizer",
        help="Tokenizer checkpoint name to download from HF hub",
    )
    parser.add_argument(
        "--dataset-name",
        type=str,
        default="wikitext-2-raw-v1",
        help="Dataset name to download from HF hub",
    )
    parser.add_argument(
        "--dataset-path",
        type=str,
        default="wikitext",
        help="Path on the head node to save the dataset to",
    )
    parser.add_argument(
        "--num-epochs",
        type=int,
        default=5,
        help="number of epochs to train (default: 5)",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="enables CUDA training"
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=2,
        help="Number of Ray workers to use for training.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Limit dataset size to finish quickly for testing",
    )
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        default=None,
        help="Address of Ray cluster.",
    )

    args = parser.parse_args()

    # Requires at least torch 1.11 to pass
    runtime_env = {"pip": ["torch==1.11.0"]}
    if args.address:
        ray.init(args.address, runtime_env=runtime_env)
    else:
        ray.init(runtime_env=runtime_env)

    main(
        model_checkpoint=args.model_checkpoint,
        tokenizer_checkpoint=args.tokenizer_checkpoint,
        dataset_name=args.dataset_name,
        dataset_path=args.dataset_path,
        num_epochs=args.num_epochs,
        num_workers=args.num_workers,
        use_gpu=args.use_gpu,
        smoke_test=args.smoke_test,
    )
