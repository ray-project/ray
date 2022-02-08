#! /bin/bash

for i in {1}
do
	echo 'RAY_BACKEND_LOG_LEVEL=debug RAY_record_ref_creation_sites=1 RAY_enable_BlockTasks=true python test_pipeline.py'
	 RAY_BACKEND_LOG_LEVEL=debug RAY_record_ref_creation_sites=1  RAY_enable_BlockTasks=true python test_pipeline.py
done
