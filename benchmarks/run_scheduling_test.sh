#! /bin/bash
echo "Run many short tasks (0s) single node test"
python distributed/test_scheduling.py \
       --total-num-task=1984000 \
       --num-cpu-per-task=1 \
       --task-duration-s=0 \
       --total-num-actors=1 \
       --num_actors_per_nodes=1

echo "Run many short tasks (0s) many nodes test"
python distributed/test_scheduling.py \
       --total-num-task=1984000 \
       --num-cpu-per-task=1 \
       --task-duration-s=0 \
       --total-num-actors=32 \
       --num_actors_per_nodes=1

echo "Run many long tasks (5s) single node test "
python distributed/test_scheduling.py \
       --total-num-task=19840 \
       --num-cpu-per-task=1 \
       --task-duration-s=5 \
       --total-num-actors=1 \
       --num_actors_per_nodes=1

echo "Run many long tasks (5s) many node test "
python distributed/test_scheduling.py \
       --total-num-task=19840 \
       --num-cpu-per-task=1 \
       --task-duration-s=5 \
       --total-num-actors=32 \
       --num_actors_per_nodes=1
