# Minimum Cluster Requirements
You must have at least 1 worker machine with at least 60GB of memory dedicated to the object store.

# What does the script do?
The script tests Dask based workloads on a Ray cluster.

It auto-determines how much work to send to the cluster at a given time, based on `num_workers` and `worker_obj_store_size_in_gb`.
If `trigger_object_spill` is specified, then the script will send to the cluster more work than it can handle in-memory,
triggering object spill condition. If `trigger_object_spill` is not specified, then the script will not overwhelm the cluster.

# Commands to submit to Ray cluster

## To trigger object spill
ray submit ray_cluster.yaml \
--cluster-name jkkwon \
/Volumes/workplace/ray/release/data_processing_tests/workloads/dask_on_ray_large_scale_test.py  \
--num_workers 10 --worker_obj_store_size_in_gb 360 --error_rate 0 --data_save_path /efs/xarrays --trigger-object-spill


## To not trigger object spill
ray submit ray_cluster.yaml \
--cluster-name jkkwon \
/Volumes/workplace/ray/release/data_processing_tests/workloads/dask_on_ray_large_scale_test.py  \
--num_workers 10 --worker_obj_store_size_in_gb 360 --error_rate 0 --data_save_path /efs/xarrays


## To stimulate error conditions while loading data 
ray submit ray_cluster.yaml \
--cluster-name jkkwon \
/Volumes/workplace/ray/release/data_processing_tests/workloads/dask_on_ray_large_scale_test.py  \
--num_workers 10 --worker_obj_store_size_in_gb 360 --error_rate 0.3 --data_save_path /efs/xarrays


## To run locally on a single machine for debugging purposes
ray submit ray_cluster.yaml \
--cluster-name jkkwon \
/Volumes/workplace/ray/release/data_processing_tests/workloads/dask_on_ray_large_scale_test.py  \
--num_workers 1 --worker_obj_store_size_in_gb 360 --error_rate 0.3 --data_save_path /efs/xarrays --run_locally
