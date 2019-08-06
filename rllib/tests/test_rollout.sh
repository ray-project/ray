#!/bin/bash -e

TRAIN=/ray/rllib/train.py
if [ ! -e "$TRAIN" ]; then
    TRAIN=../train.py
fi
ROLLOUT=/ray/rllib/rollout.py
if [ ! -e "$ROLLOUT" ]; then
    ROLLOUT=../rollout.py
fi

TMP=`mktemp -d`
echo "Saving results to $TMP"

$TRAIN --local-dir=$TMP --run=IMPALA --checkpoint-freq=1 \
    --config='{"num_workers": 1, "num_gpus": 0}' --env=Pong-ram-v4 \
    --stop='{"training_iteration": 1}'
find $TMP

CHECKPOINT_PATH=`ls $TMP/default/*/checkpoint_1/checkpoint-1`
echo "Checkpoint path $CHECKPOINT_PATH"
test -e "$CHECKPOINT_PATH"

$ROLLOUT --run=IMPALA "$CHECKPOINT_PATH" --steps=100 \
    --out="$TMP/rollouts.pkl" --no-render
test -e "$TMP/rollouts.pkl"
rm -rf "$TMP"
echo "OK"
