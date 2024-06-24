#!/bin/bash

TOTAL_UPDATES=125000       # Total number of training steps
WARMUP_UPDATES=10000       # Warmup the learning rate over this many updates
PEAK_LR=0.0005             # Peak learning rate, adjust as needed
TOKENS_PER_SAMPLE=512      # Max sequence length
#MAX_POSITIONS=512         # Num. positional embeddings (usually same as above)
MAX_SENTENCES=8            # Number of sequences per batch on one GPU (batch size)
FIX_BATCH_SIZE=2048        # Number of batch size in total (max_sentences * update_freq * n_gpus)
SAVE_INTERVAL_UPDATES=1000 # save a checkpoint every N updates

LOG_DIR=$HOME/efs/lm/log/
DATA_DIR=$HOME/efs/lm/data-bin/wikitext-103/
mkdir -p "$LOG_DIR"

python "$HOME"/efs/lm/ray_train.py --fp16 "$DATA_DIR" \
    --task masked_lm --criterion masked_lm \
    --arch roberta_base --sample-break-mode complete --tokens-per-sample $TOKENS_PER_SAMPLE \
    --optimizer adam --adam-betas '(0.9, 0.98)' --adam-eps 1e-6 --clip-norm 0.0 \
    --lr-scheduler polynomial_decay --lr $PEAK_LR --warmup-updates $WARMUP_UPDATES --total-num-update $TOTAL_UPDATES \
    --dropout 0.1 --attention-dropout 0.1 --weight-decay 0.01 \
    --max-sentences $MAX_SENTENCES \
    --fix-batch-size $FIX_BATCH_SIZE \
    --max-update $TOTAL_UPDATES --log-format simple --log-interval 1 \
    --save-interval-updates $SAVE_INTERVAL_UPDATES \
    --save-dir "$LOG_DIR" --ddp-backend=no_c10d
