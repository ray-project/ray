How To: Distributed Tuning
==========================

This is an example walkthrough for a distributed hyperparameter search in Tune. At a high level, we will use Ray's cluster launcher/autoscaler utility to start a cluster of nodes, all of which are connected by Ray. Then, we will modify an existing hyperparameter tuning script to connect to the Ray cluster, and launch the script. Finally, we will analyze the results.


Quick Start
-----------

.. code-block::bash

    export CLUSTER=[path/to/cluster/yaml]
    ray submit $CLUSTER tune_mnist_large.py --start
    ray exec $CLUSTER 'tensorboard --logdir=~/ray_results/ --port 6006' --port-forward 6006

This code starts a cluster as specified by the given cluster configuration YAML file.



Fault Tolerance
---------------



(Recommended) Use the Ray Autoscaler to launch a cluster

(Recommended) Enable Checkpointing

(Recommended) `ray submit --start`


