#! /bin/bash

echo 'RAY_BACKEND_LOG_LEVEL=debug RAY_record_ref_creation_sites=1 RAY_object_spilling_threshold=0.1 RAY_enable_BlockTasks=true python deadlock1.py'
RAY_BACKEND_LOG_LEVEL=debug RAY_record_ref_creation_sites=1 RAY_object_spilling_threshold=0.1 RAY_enable_BlockTasks=true python deadlock1.py
