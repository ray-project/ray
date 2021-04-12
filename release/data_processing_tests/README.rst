Data processing test
--------------------
There are 2 workloads. Each workerload requires a different cluster.yaml.

Make sure to copy & paste both drivers.

Run `unset RAY_ADDRESS; python workloads/streaming_shuffle.py`. Use `cluster.yaml` for this release test.
Run `unset RAY_ADDRESS; python workloads/dask_on_ray_large_scale_test.py`. Use `dask_on_ray.yaml` for this release test.

Note that when you run `dask_on_ray.yaml`, you need to follow the below procedures.

```
ray up dask_on_ray.yaml -y # Start the ray cluster.
# Wait until the cluster nodes are up. Use `watch ray status` and wait until all worker nodes are up.
ray down dask_on_ray.yaml -y # After the cluster is up, you should call ray down.
ray up dask_on_ray.yaml -y
```

This process is required because ulimit is not permitted for images that we are using. Ulimit is necessary for large cluster testing like this.
Check out https://discuss.ray.io/t/setting-ulimits-on-ec2-instances/590/2 for more details

Success Criteria
================
For `streaming_shuffle.py`, make sure to include the output string to the release logs.

For `dask_on_ray_large_scale_test.py`, make sure the test runs for at least for an hour. This test should succeed, otherwise, it is a release blocker.
Check out https://github.com/ray-project/ray/pull/14340#discussion_r599271079 to learn the success condition of this test.


Dask on Ray compatibility test
------------------------------
It tests the compatibility to last N dask versions.

Please follow the steps below. Note that the test is only working in MacOS with Python 3.7 right now. Feel free to contribute.

1. Go to https://pypi.org/project/dask/#history and make sure the newer versions are included in `dask-on-ray-test.sh`.
2. `chmod 777 dask-on-ray-test.sh`
3. `./dask-on-ray-test.sh`

Success Criteria
================
Make sure all tests are passing on all dask versions.
