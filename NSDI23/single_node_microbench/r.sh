#! /bin/bash

for w in {1,2,4,8,16}
do
	echo RAY_enable_BlockTasks=true python test_pipeline.py -w $w -o 4000000000 -os $os 10000000
	RAY_object_spilling_threshold=100.0 RAY_enable_BlockTasks=true python test_pipeline_dynamic.py -w $w -o 4000000000 -os $os 10000000 
done
