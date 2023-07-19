# Finetuning Llama-2 series models 7B, 13B, and 70B with Ray Train


## Get Started


### Create the dataset

Depending on the dataset you want to finetune on, the tokenization and dataset pre-processing will likely need to be adjusted. The current code is configured to train on the Grade School Math 8k (GSM8K) dataset. By running the code below we create three files that are needed to launch the training script with. 

```
data/train.jsonl # 9.2k training data
data/test.jsonl # 1.3k test data
tokens.json # a list of special tokens used to encode the dataset with
```

### Launching fine-tuning

The script is written using Ray Train + Deepspeed integration via accelerate API. 
The script is general enough that it can be used to fine-tune all released sizes of Llama-2 models. 

The CLI for launching the training job is as follows:

```
python finetune_llama.py --bs 4 --model_name [hf_model_id] --output_dir /mnt/shared_storage/finetuning-llama/ --train_path ./data/train.jsonl --test_path ./data/test.jsonl  --special_token_path ./gsm/tokens.json 
```

This script was tested across three model sizes on the following cluster configurations on Anyscale platform. 


| Model Size | HF Model ID                     | Instance Type  | GPUs         |
|------------|--------------------------------|----------------|--------------|
| 7B         | `daryl149/llama-2-7b-chat-hf`   | 16x `g5.xlarge` | 8x A10G (24G) |
| 13B        | `daryl149/llama-2-13b-chat-hf`  | 4x `g5.24xlarge`| 32x A10G (24G)|
| 70B        | `anonymous4chan/llama-2-70b`   | 4x `g5.24xlarge`| 32x A10G (24G)|
