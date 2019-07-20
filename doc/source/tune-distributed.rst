Distributed Experiments
=======================

Tune is commonly used for large-scale distributed hyperparameter optimization. Tune provides many utilities that enable an effective workflow for interacting with a cluster.

In this guide, we will use Ray's cluster launcher/autoscaler utility to start a cluster of machines on AWS. Then, we will modify an existing hyperparameter tuning script to connect to the Ray cluster, and launch the script. Finally, we will analyze the results.

Connecting to a cluster
~~~~~~~~~~~~~~~~~~~~~~~

Modifying an existing Tune Experiment to ray. One common approach is to

.. code-block:: python

    import ray
    from ray import tune
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-address", default=None)
    args = parser.parse_args()
    ray.init(redis_address=args.redis_address)

.. code-block:: bash

    # Single-node execution
    python script.py

    # Ray multi-node execution
    python script.py --redis-address=localhost:1234


Using a local cluster
~~~~~~~~~~~~~~~~~~~~~


If you have a list of nodes, you can follow the private cluster setup `instructions here <autoscaling.html>`__ to setup a Ray cluster.

.. code-block:: yaml

    cluster_name: default
    max_workers: 0
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

Alternatively, if you run into issues (or want to add nodes manually), you can use the manual cluster setup `instructions here <using-ray-on-a-cluster.html>`__


Launching a cloud cluster
~~~~~~~~~~~~~~~~~~~~~~~~

You can use this YAML configuration file to kick off your cluster. The following instructions are for AWS and wil require you to run ``aws configure``.
Ray currently supports AWS and GCP.

.. code-block:: yaml

    cluster_name: minimal

    # The maximum number of workers nodes to launch in addition to the head node.
    max_workers: 1

    # Cloud-provider specific configuration.
    provider:
        type: aws
        region: us-west-2
        availability_zone: us-west-2a

    # How Ray will authenticate with newly launched nodes.
    auth:
        ssh_user: ubuntu


This code starts a cluster as specified by the given cluster configuration YAML file.

.. code-block:: bash

    export CLUSTER=path_to_cluster_yaml
    ray submit $CLUSTER tune_mnist_large.py --start

    # Analyze your results on TensorBoard. This starts TensorBoard on the remote machine.
    # Go to `http://localhost:6006` to access TensorBoard.
    ray exec $CLUSTER 'tensorboard --logdir=~/ray_results/ --port 6006' --port-forward 6006


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

Spot instances may be removed suddenly while trials are still running. You can easily mitigate the effects of this by preserving the progress of your model training through checkpointing - The easiest way to do this is to subclass the pre-defined ``Trainable`` class and implement ``_save``, and ``_restore`` abstract methods, as seen in `this example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__. See the `Checkpointing <tune-checkpointing.html>`__ page for more details.

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
