Running the benchmarks
======================

You can run the benchmark suite by doing the following:

1. Install https://github.com/ray-project/asv: ``cd asv; pip install -e .``
2. Run ``asv dev`` in this directory.

To run ASV inside docker, you can use the following command:
``docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA bash -c '/ray/test/jenkins_tests/run_asv.sh'``

Visualizing Benchmarks
======================

To visualize benchmarks, you must copy the S3 bucket down to `$RAY_DIR/python`. Assuming asv is installed,

```
cd $RAY_DIR/python
aws s3 sync s3://BUCKET/ .
```
Then, you can run:
```
asv publish --no-pull
asv preview
```

This creates 
