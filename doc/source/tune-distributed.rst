Distributed Experiments
=======================

Tune is commonly used for large-scale distributed hyperparameter optimization. Tune provides many utilities that enable an effective workflow for interacting with a cluster.

Quick Start
-----------

To launch a distributed hyperparameter search, you can follow the instructions below:

1. Download a full example Tune experiment script here: :download:`mnist_pytorch.py <../../python/ray/tune/examples/mnist_pytorch.py>`
2. Download an example cluster yaml here: :download:`tune-default.yaml <../../python/ray/tune/examples/quickstart/tune-default.yaml>`
3. Set up your AWS credentials (``aws configure``).
4. Run ``ray submit`` as below. This will start 3 AWS nodes and run Tune across them. Append ``[--stop]`` to automatically shutdown your nodes after running:

.. code-block:: bash

    export CLUSTER=tune-default.yaml
    ray submit $CLUSTER mnist_pytorch.py --args="--ray-redis-address=localhost:6379" --start

Connecting to a cluster
-----------------------

One common approach to modifying an existing Tune experiment to go distributed is to set an argparse variable so that toggling between distributed and single-node is seamless. This allows Tune to utilize all the resources available to the Ray cluster.

.. code-block:: python

    import ray
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--ray-redis-address")
    args = parser.parse_args()
    ray.init(redis_address=args.ray_redis_address)

Compare single node vs cluster execution. Note that connecting to cluster requires a pre-existing Ray cluster to be setup already (`Manual Cluster Setup <using-ray-on-a-cluster.html>`_). The script should be run on the head node of the Ray cluster.

.. code-block:: bash

    # Single-node execution
    $ python tune_script.py

    # On the head node, connect to an existing ray cluster
    $ python tune_script.py --ray-redis-address=localhost:1234


Launching a cloud cluster
~~~~~~~~~~~~~~~~~~~~~~~~~

.. tip:: If you have already have a list of nodes, skip down to the `Local Cluster Usage <tune-distributed.html#local-cluster-usage>`_ section.

Ray currently supports AWS and GCP. Below, we will launch nodes on AWS that will default to using the Deep Learning AMI. See the `cluster setup documentation <autoscaling.html>`_.

.. literalinclude:: ../../python/ray/tune/examples/quickstart/tune-default.yaml



This code starts a cluster as specified by the given cluster configuration YAML file.

.. code-block:: bash

    export CLUSTER=path_to_cluster_yaml
    $ ray submit $CLUSTER tune_mnist_large.py --start

    # Analyze your results on TensorBoard. This starts TensorBoard on the remote machine.
    # Go to `http://localhost:6006` to access TensorBoard.
    ray exec $CLUSTER 'tensorboard --logdir=~/ray_results/ --port 6006' --port-forward 6006


Local Cluster Usage
~~~~~~~~~~~~~~~~~~~

If you run into issues (or want to add nodes manually), you can use the manual cluster setup `documentation here <using-ray-on-a-cluster.html>`__. At a glance, On the head node, run the following.

.. code-block:: bash

    # If the ``--redis-port`` argument is omitted, Ray will choose a port at random.
    $ ray start --head --redis-port=6379

The command will print out the address of the Redis server that was started (and some other address information).

**Then on all of the other nodes**, run the following. Make sure to replace ``<redis-address>`` with the value printed by the command on the head node (it should look something like ``123.45.67.89:6379``).

.. code-block:: bash

    $ ray start --redis-address=<redis-address>

If you have already have a list of nodes, you can follow the private autoscaling cluster setup `instructions here <autoscaling.html>`__ - below is a configuration file for a private autoscaling cluster.

.. code-block:: yaml

    cluster_name: default
    max_workers: 0  # TODO: specify the number of workers here.
    provider:
        type: local
        head_ip: YOUR_HEAD_NODE_HOSTNAME
        worker_ips: []  # TODO: Put other nodes here
    auth:
        ssh_user: YOUR_USERNAME
        ssh_private_key: ~/.ssh/id_rsa
    file_mounts: {}
    setup_commands:
        - pip install -U ray
    head_start_ray_commands:
        - ray stop
        - >-
            ulimit -c unlimited &&
            ray start --head --redis-port=6379 --autoscaling-config=~/ray_bootstrap_config.yaml
    worker_start_ray_commands:
        - ray stop
        - ray start --redis-address=$RAY_HEAD_IP:6379


Pre-emptible Instances (Cloud)
------------------------------

Running on spot instances (or pre-emptible instances) can reduce the cost of your experiment. You can enable spot instances in AWS via the following configuration modification:

.. code-block:: yaml

    # Provider-specific config for worker nodes, e.g. instance type.
    worker_nodes:
        InstanceType: m5.large
        ImageId: ami-0b294f219d14e6a82 # Deep Learning AMI (Ubuntu) Version 21.0

        # Run workers on spot by default. Comment this out to use on-demand.
        InstanceMarketOptions:
            MarketType: spot
            SpotOptions:
                MaxPrice: 1.0  # Max Hourly Price

In GCP, you can use the following configuration modification:

.. code-block:: yaml

    worker_nodes:
        machineType: n1-standard-2
        disks:
          - boot: true
            autoDelete: true
            type: PERSISTENT
            initializeParams:
              diskSizeGb: 50
              # See https://cloud.google.com/compute/docs/images for more images
              sourceImage: projects/deeplearning-platform-release/global/images/family/tf-1-13-cpu

        # Run workers on preemtible instances.
        scheduling:
          - preemptible: true

Spot instances may be removed suddenly while trials are still running. Often times this may be difficult to deal with when using other distributed hyperparameter optimization frameworks. Tune allows users to mitigate the effects of this by preserving the progress of your model training through checkpointing. The easiest way to do this is to subclass the pre-defined ``Trainable`` class and implement ``_save``, and ``_restore`` abstract methods, as seen in `this example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__. See the `Checkpointing <tune-usage.html#trainable-trial-checkpointing>`__ section for more details.

You can also specify ``tune.run(upload_dir=...)`` to sync results with a cloud storage like S3, persisting results in case you want to start and stop your cluster automatically.

Common Commands
---------------

Below are some commonly used commands for submitting experiments. Please see the `Autoscaler page <autoscaling.html>`__ to see find more comprehensive documentation of commands.

.. code-block:: bash

    # Upload `tune_experiment.py` from your local machine onto the cluster. Then,
    # run `python tune_experiment.py --redis-address=localhost:6379` on the remote machine.
    $ ray submit CLUSTER.YAML tune_experiment.py --args="--redis-address=localhost:6379"

    # Start a cluster and run an experiment in a detached tmux session.
    # Shut down the cluster as soon as the experiment completes.
    # In `tune_experiment.py`, set `tune.run(upload_dir="s3://...")` to persist results
    $ ray submit CLUSTER.YAML --tmux --start --stop tune_experiment.py --args="--redis-address=localhost:6379"

    # Run Tensorboard and forward the port to your own machine.
    $ ray exec CLUSTER.YAML 'tensorboard --logdir ~/ray_results/ --port 6006' --port-forward 6006

    # Run Jupyter Lab and forward the port to your own machine.
    $ ray exec CLUSTER.YAML 'jupyter lab --port 6006' --port-forward 6006

    # See all the experiments and trials that have executed so far
    $ ray exec CLUSTER.YAML 'tune ls ~/ray_results'

    # If you modify any of the file_mounts (like in a project repository), you can upload
    # and sync all of the files up to the cluster with this command.
    $ ray rsync-up CLUSTER.YAML

    # Download the results directory from your cluster head node to your local machine
    $ ray rsync-down CLUSTER.YAML '~/ray_results' ~/cluster_results
