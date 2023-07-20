# Finetuning Llama-2 series models 7B, 13B, and 70B with Ray Train


## Get Started


### Create the dataset

Depending on the dataset you want to finetune on, the tokenization and dataset pre-processing will likely need to be adjusted. The current code is configured to train on the Grade School Math 8k (GSM8K) dataset. By running the code below we create three files that are needed to launch the training script with. 

```
data/train.jsonl # 9.2k training data
data/test.jsonl # 1.3k test data
tokens.json # a list of special tokens used to encode the dataset with
```

### End-to-end script
You can launch the training jobs by running the provided bash scripts. 

```
./run_llama_7b_chat.sh
```

### Downloading the model weights to remote nodes

To make things faster and easier we have created a mirror link of huggingface weights on to S3. We first will download the weights on the nvme storage of any node that has a GPU. This will save us some time when iterating on the training script. 

Note: For some reason the mirrored links are twice the size that they should be. 

```
python prepare_nodes.py --hf-model-id meta-llama/Llama-2-7b-chat-hf
```

### Launching fine-tuning

The script is written using Ray Train + Deepspeed integration via accelerate API. The script is general enough that it can be used to fine-tune all released sizes of Llama-2 models. 

The CLI for launching the training job is as follows:

```
python finetune_hf_llm.py \
    -bs 16 \
    -nd 16 \
    --model_name ${MODEL_ID} \
    --output_dir /mnt/cluster_storage/finetuning-llama/ \
    --train_path ./data/train.jsonl \
    --test_path ./data/test.jsonl  \
    --special_token_path ./data/tokens.json
```

This script was tested across three model sizes on the following cluster configurations on Anyscale platform. 


| Model Size | HF Model ID                     | Batch size per device | Instance Type  | GPUs         |
|------------|--------------------------------|------------------------|----------------|--------------|
| 7B         | `meta-llama/Llama-2-7b-chat-hf`   | 16         | 16x `g5.xlarge` | 8x A10G (24G) |
| 13B        | `meta-llama/Llama-2-13b-chat-hf`  | 16         | 4x `g5.24xlarge`| 32x A10G (24G)|
| 70B        | `meta-llama/Llama-2-70b-chat-hf`   | 4          | 4x `g5.24xlarge`| 32x A10G (24G)|
