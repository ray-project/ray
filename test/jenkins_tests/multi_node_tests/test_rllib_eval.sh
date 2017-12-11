#!/bin/sh

# TODO: Test AC3
ALGS='DQN PPO'
GYM_ENV='CartPole-v0'

for ALG in $ALGS
do
  EXPERIMENT_NAME=$GYM_ENV'_'$ALG
  python /ray/python/ray/rllib/train.py --run $ALG --env $GYM_ENV \
    --stop '{"training_iteration": 2}' --experiment-name $EXPERIMENT_NAME \
    --checkpoint-freq 1

  EXPERIMENT_PATH='/tmp/ray/'$EXPERIMENT_NAME
  CHECKPOINT_FOLDER=$(ls $EXPERIMENT_PATH)
  CHECKPOINT=$EXPERIMENT_PATH'/'$CHECKPOINT_FOLDER'/checkpoint-1'

  python /ray/python/ray/rllib/eval.py $CHECKPOINT --run $ALG \
    --env $GYM_ENV --no-render

  # Clean up
  rm -rf $EXPERIMENT_PATH
done
