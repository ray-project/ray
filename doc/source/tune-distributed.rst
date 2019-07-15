How To: Distributed Tuning on the cloud
=======================================

This is an example walkthrough for a distributed hyperparameter search in Tune. Launching a large cluster on AWS or GCP is simple with Ray.

In this guide, we will use Ray's cluster launcher/autoscaler utility to start a cluster of machines on AWS. Then, we will modify an existing hyperparameter tuning script to connect to the Ray cluster, and launch the script. Finally, we will analyze the results.


Quick Start
-----------

You can use this YAML configuration file to kick off your cluster.

.. code-block::yaml

    TODO

This code starts a cluster as specified by the given cluster configuration YAML file.


.. code-block::bash

    export CLUSTER=[path/to/cluster/yaml]
    ray submit $CLUSTER tune_mnist_large.py --start
    ray exec $CLUSTER 'tensorboard --logdir=~/ray_results/ --port 6006' --port-forward 6006


Pre-emptible Instances (Cloud)
------------------------------

Running on spot instances (or pre-emptible instances) can reduce the cost of your experiment. You can enable spot instances in AWS via the following configuration modification:

.. code-block::yaml

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

.. code-block::yaml

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

Spot instances may be removed suddenly while trials are still running. You can easily mitigate the effects of this by preserving the progress of your model training through checkpointing - The easiest way to do this is to subclass the pre-defined ``Trainable`` class and implement ``_save``, and ``_restore`` abstract methods, as seen in `this example<https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__. See the `Checkpointing <tune-checkpointing.html>`__ page for more details.

Common Commands
---------------

Below are some commonly used commands for submitting experiments. Please see the `Autoscaler page <autoscaling.html>`__ to see find more comprehensive documentation of commands.


    # Run a Python script in a detached tmux session
    $ ray submit cluster.yaml --tmux --start --stop tune_experiment.py

