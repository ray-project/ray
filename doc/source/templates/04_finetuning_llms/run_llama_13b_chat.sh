
MODEL_ID="meta-llama/Llama-2-13b-chat-hf"

python prepare_nodes.py --hf-model-id ${MODEL_ID}

python finetune_hf_llm.py \
    -bs 16 \
    -nd 16 \
    --model_name ${MODEL_ID} \
    --output_dir /mnt/cluster_storage/finetuning-llama/ \
    --train_path ./data/train.jsonl \
    --test_path ./data/test.jsonl  \
    --special_token_path ./data/tokens.json