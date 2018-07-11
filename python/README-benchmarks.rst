Running the benchmarks
======================

You can run the benchmark suite by doing the following:

1. Install https://github.com/ray-project/asv: ``cd asv; pip install -e .``
2. Run ``asv dev`` in this directory.

To run ASV inside docker, you can use the following command:
``docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA bash -c '/ray/test/jenkins_tests/run_asv.sh'``
``docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA bash -c '/ray/test/jenkins_tests/run_rllib_asv.sh'``


Visualizing Benchmarks
======================

For visualizing regular Ray benchmarks, you must copy the S3 bucket down to `$RAY_DIR/python`.

.. code-block::

  cd $RAY_DIR/python
  aws s3 sync s3://$BUCKET/ASV/ .

For rllib, you must sync a _particular_ folder down to `$RLLIB_DIR (ray/python/ray/rllib)`.

.. code-block::

  cd $RAY_DIR/python/ray/rllib
  aws s3 sync s3://$BUCKET/RLLIB_RESULTS/ ./RLLIB_RESULTS

Then, in the directory, you can run:

.. code-block::

  asv publish --no-pull
  asv preview

This creates the directory and then launches a server at which you can visualize results.
