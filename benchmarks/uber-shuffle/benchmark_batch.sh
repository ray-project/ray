#!/bin/bash

# Exit immediately if any command fails.
set -exo pipefail

num_row_groups=20
num_rows_per_group=4000000
num_row_groups_per_file=1
batch_size=250000
num_trials=3
num_epochs=10

num_rounds_list=(1 2 4 8 20 40)
num_trainers_list=(1 2 4)
max_concurrent_epochs_list=(1 2 3)
max_concurrent_rounds_list=(2 4 8)
shuffle_type_flag=("" "--use-from-disk-shuffler")

for num_trainers in "${num_trainers_list[@]}"; do
        for num_rounds in "${num_rounds_list[@]}"; do
                for max_concurrent_epochs in "${max_concurrent_epochs_list[@]}"; do
                        for max_concurrent_rounds in "${max_concurrent_rounds_list[@]}"; do
                                for shuffle_type in "${shuffle_type_flag[@]}"; do
                                        batches_per_round=$(( num_row_groups * num_rows_per_group / \
                                                num_trainers / batch_size / num_rounds ))
                                        ray submit single-node.yaml shuffle.py -- \
                                                --num-rows-per-group "$num_rows_per_group" \
                                                --num-row-groups "$num_row_groups" \
                                                --num-row-groups-per-file "$num_row_groups_per_file" \
                                                --batch-size "$batch_size" \
                                                --num-trials "$num_trials" \
                                                --cluster \
                                                --num-trainers "$num_trainers" \
                                                --num-epochs "$num_epochs" \
                                                --batches-per-round "$batches_per_round"  \
                                                --max-concurrent-epochs "$max_concurrent_epochs" \
                                                --max-concurrent-rounds "$max_concurrent_rounds" \
                                                "$shuffle_type"
                                        done
                                done
                        done
                done
        done
