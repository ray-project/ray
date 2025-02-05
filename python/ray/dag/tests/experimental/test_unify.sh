#!/bin/bash

export TZ="America/Los_Angeles"
timestamp=$(date "+%Y%m%d_%H%M%S")

log=log/$timestamp.log
echo $log
touch $log

export RAY_DEDUP_LOGS=0
export RAY_PYTEST_USE_GPU=1

# [UNIT]
python -m pytest \
	--log-level=INFO -v -s \
	test_torch_tensor_dag.py::test_torch_tensor_exceptions \
	>$log 2>&1

# [OVERLAP]
# python -m pytest \
# 	--log-level=INFO -v -s \
# 	test_torch_tensor_dag.py::test_torch_tensor_nccl_overlap_p2p \
# 	test_torch_tensor_dag.py::test_torch_tensor_nccl_overlap_collective \
# 	test_torch_tensor_dag.py::test_torch_tensor_nccl_overlap_p2p_and_collective \
# 	>$log 2>&1

# [SEA2BEL]
# python -m pytest \
# 	--log-level=INFO -v -s \
# 	test_execution_schedule.py \
# 	test_torch_tensor_dag.py \
# 	test_collective_dag.py \
# 	test_detect_deadlock_dag.py \
# 	>$log 2>&1

# [MI]
# python -m pytest \
# 	--log-level=INFO -v -s \
# 	test_execution_schedule.py \
# 	test_torch_tensor_dag.py \
# 	test_collective_dag.py \
# 	test_detect_deadlock_dag.py \
# 	test_accelerated_dag.py \
# 	test_multi_node_dag.py \
# 	../test_class_dag.py \
# 	>$log 2>&1

RED='\e[31m'
GREEN='\e[32m'
RESET='\e[0m'

tail -n 1 $log | while read line; do
	if [[ "$line" == *"failed"* ]]; then
		echo -e "${RED}${line}${RESET}"
	else
		echo -e "${GREEN}${line}${RESET}"
	fi
done
