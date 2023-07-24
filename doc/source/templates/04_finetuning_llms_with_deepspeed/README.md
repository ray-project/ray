# Finetuning Llama-2 series models with Deepspeed, Accelerate, and Ray Train TorchTrainer
| Template Specification | Description |
| ---------------------- | ----------- |
| Summary | This template, demonstrates how to perform full parameter fine-tuning for Llama-2 series models (7B, 13B, and 70B) using TorchTrainer with the DeepSpeed ZeRO-3 strategy. |
| Time to Run | Around 15 min. for 7B for 1 epoch on 3.5M tokens. |
| Minimum Compute Requirements | At least 1xg5.16xlarge for head-node and 15xg5.4xlarge for worker nodes |
| Cluster Environment | This template uses a docker image built on top of the latest Anyscale-provided Ray image using Python 3.9: [`anyscale/ray:latest-py39-cu116`](https://docs.anyscale.com/reference/base-images/overview). |

## Getting Started

For 7B, set up a cluster on AWS with the following settings:

|            | num | instance type | GPU per node | GPU Memory | CPU Memory |
|------------|-----|---------------|--------------|------------|------------|
| Head node  | 1   | g5.16xlarge   | 1 x A10G     | 24 GB      | 256 GB     |
| Worker node| 15  | g5.4xlarge    | 1 x A10G     | 24 GB      | 64 GB      |

And launch the following script:

```
./run_llama_7b_chat.sh [--as-test]
```

The flag `--as-test` is for demo / testing purposes as it runs through only one forward and backward pass of the model. The model loading, and remote checkpointing would still run. 

## What is happening under the hood?

### Downloading the pre-trained checkpoint on to all GPU nodes. 

The pre-trained models for these models is quite large (12.8G for 7B model and 128G for 70B model). In order to make loading these models faster, we have mirrored the weights on to an AWS S3 bucket which can result in up 10GB/s download speed if the aws configs are setup correctly. We have a default setup that can provide 1.5GB/s download speed. Therefore we do not have to wait long for the model weights to get downloaded. 

### Cloud storage

Similarly the checkpoints during training can be quite large and we would like to be able to save those checkpoints to the familiar huggingface format so that we can serve it conveniently. The fine-tuning script in this template uses Ray Air Checkpointing to sync the checkpoints created by each node back to a centralized cloud storage on AWS S3. The final file structure for each checkpoint will have a look similar to the following structure:

```
epoch-0
├── .is_checkpoint
├── .metadata.pkl
├── .tune_metadata
├── _metadata.meta.pkl
├── _preprocessor
├── _preprocessor.meta.pkl
├── added_tokens.json
├── config.json
├── generation_config.json
├── model-00001-of-00002.safetensors
├── model-00002-of-00002.safetensors
├── model.safetensors
├── model.safetensors.index.json
├── special_tokens_map.json
├── tokenizer.json
├── tokenizer.model
└── tokenizer_config.json
```

After training we can use [Aviary](https://github.com/ray-project/aviary) to deploy our fine-tuned LLM by providing the checkpoint path stored on cloud directly.

### Creating the dataset

The main fine-tuning script is written in a general format that would require you to provide a `jsonl` file for train and test datasets in addition to a `json` file listing the special tokens used in your dataset. 

For example each row in your dataset might be formated like the following:

```
{"input": "<ASSISTANT>How can I help you?</ASSISTANT><USER>how is the weather?</USER>}
```

And the special tokens can be:

```
{"tokens": ["<ASSISTANT>", "</ASSISTANT>", "<USER>", "</USER>"]}
```

Depending on the dataset you want to finetune on, the tokenization and dataset pre-processing will likely need to be adjusted. The current code is configured to train on the Grade School Math 8k (GSM8K) dataset. By running the code below we create three files that are needed to launch the training script with. 

```
python create_dataset.py

>>> data/train.jsonl # 7.4k training data
>>> data/test.jsonl # 1.3k test data
>>> tokens.json # a list of special tokens
```

This dataset is trained with a context length of 512 which includes excessive padding to keep all samples limited to 512 tokens. This means that the training dataset has 3.5 M tokens.

### Launching fine-tuning

The script is written using Ray Train + Deepspeed integration via accelerate API. The script is general enough that it can be used to fine-tune all released sizes of Llama-2 models. 

The CLI for seeing all the options is:

```
python finetune_hf_llm.py --help
```

This script was tested across three model sizes on the following cluster configurations on Anyscale platform. 


| Model Size | Base HF Model ID             | Batch size per device | Instance Type    | GPUs           | Time per epoch (min.) |
|------------|------------------------------|-----------------------|------------------|----------------|-----------------------|
| 7B         | `meta-llama/Llama-2-7b-hf`   | 16                    | 16x `g5.xlarge`  | 16x A10G (24G) | ~14 min.              |
| 13B        | `meta-llama/Llama-2-13b-hf`  | 16                    | 16x `g5.xlarge`  | 16x A10G (24G) | ~20 min.              |
| 70B        | `meta-llama/Llama-2-70b-hf`  | 4                     | 4x  `g5.24xlarge`| 32x A10G (24G) |                       |
