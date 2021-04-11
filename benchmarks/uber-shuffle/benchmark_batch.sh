#!/bin/bash

# Exit immediately if any command fails.
set -exo pipefail

num_row_groups=50
num_rows_per_group=4000000
num_row_groups_per_file=2
batch_size=250000
num_trials=3

batches_per_round_list=(800 400 200 100 50 25 16 8)
# num_trainers_list=(1 2 4)
num_trainers_list=(2 4)
shuffle_type_flag=("" "--use-from-disk-shuffler")

for num_trainers in "${num_trainers_list[@]}"; do
        for batches_per_round in "${batches_per_round_list[@]}"; do
                for shuffle_type in "${shuffle_type_flag[@]}"; do
                        ray submit single-node.yaml shuffle.py -- \
                                --num-rows-per-group "$num_rows_per_group" \
                                --num-row-groups "$num_row_groups" \
                                --num-row-groups-per-file "$num_row_groups_per_file" \
                                --batch-size "$batch_size" \
                                --num-trials "$num_trials" \
                                --cluster \
                                --num-trainers "$num_trainers" \
                                --batches-per-round "$batches_per_round" \
                                "$shuffle_type"
                        done
                done
        done
