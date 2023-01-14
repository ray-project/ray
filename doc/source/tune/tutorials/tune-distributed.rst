.. _tune-distributed-ref:

Tune Distributed Experiments
============================

Tune is commonly used for large-scale distributed hyperparameter optimization. This page will overview how to setup and launch a distributed experiment along with :ref:`commonly used commands <tune-distributed-common>` for Tune when running distributed experiments.

.. contents::
    :local:
    :backlinks: none

Summary
-------

To run a distributed experiment with Tune, you need to:

1. First, :ref:`start a Ray cluster <cluster-index>` if you have not already.
2. Run the script on the head node, or use :ref:`ray submit <ray-submit-doc>`, or use :ref:`Ray Job Submission <jobs-overview>`.

.. tune-distributed-cloud:

Example: Tune on AWS VMs
------------------------

Follow the instructions below to launch nodes on AWS (using the Deep Learning AMI). See the :ref:`cluster setup documentation <cluster-index>`. Save the below cluster configuration (``tune-default.yaml``):

.. literalinclude:: /../../python/ray/tune/examples/tune-default.yaml
   :language: yaml
   :name: tune-default.yaml

``ray up`` starts Ray on the cluster of nodes.

.. code-block:: bash

    ray up tune-default.yaml

``ray submit --start`` starts a cluster as specified by the given cluster configuration YAML file, uploads ``tune_script.py`` to the cluster, and runs ``python tune_script.py [args]``.

.. code-block:: bash

    ray submit tune-default.yaml tune_script.py --start -- --ray-address=localhost:6379

.. image:: /images/tune-upload.png
    :scale: 50%
    :align: center

Analyze your results on TensorBoard by starting TensorBoard on the remote head machine.

.. code-block:: bash

    # Go to http://localhost:6006 to access TensorBoard.
    ray exec tune-default.yaml 'tensorboard --logdir=~/ray_results/ --port 6006' --port-forward 6006


Note that you can customize the directory of results by specifying: ``air.RunConfig(local_dir=..)``, taken in by ``Tuner``. You can then point TensorBoard to that directory to visualize results. You can also use `awless <https://github.com/wallix/awless>`_ for easy cluster management on AWS.


Running a distributed experiment
--------------------------------

Running a distributed (multi-node) experiment requires Ray to be started already. You can do this on local machines or on the cloud.

Across your machines, Tune will automatically detect the number of GPUs and CPUs without you needing to manage ``CUDA_VISIBLE_DEVICES``.

To execute a distributed experiment, call ``ray.init(address=XXX)`` before ``Tuner.fit()``, where ``XXX`` is the Ray address, which defaults to ``localhost:6379``. The Tune python script should be executed only on the head node of the Ray cluster.

One common approach to modifying an existing Tune experiment to go distributed is to set an ``argparse`` variable so that toggling between distributed and single-node is seamless.

.. code-block:: python

    import ray
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--address")
    args = parser.parse_args()
    ray.init(address=args.address)

    tuner = tune.Tuner(...)
    tuner.fit()

.. code-block:: bash

    # On the head node, connect to an existing ray cluster
    $ python tune_script.py --ray-address=localhost:XXXX

If you used a cluster configuration (starting a cluster with ``ray up`` or ``ray submit --start``), use:

.. code-block:: bash

    ray submit tune-default.yaml tune_script.py -- --ray-address=localhost:6379

.. tip::

    1. In the examples, the Ray address commonly used is ``localhost:6379``.
    2. If the Ray cluster is already started, you should not need to run anything on the worker nodes.


Storage Options
---------------

In a distributed experiment, you should try to use :ref:`cloud checkpointing <tune-cloud-checkpointing>` to
reduce synchronization overhead. For this, you just have to specify an ``upload_dir`` in the
:class:`tune.SyncConfig <ray.tune.SyncConfig>`.

`my_trainable` is a user-defined :ref:`Tune Trainable <tune_60_seconds_trainables>` in the following example:

.. code-block:: python

    from ray import air, tune
    from my_module import my_trainable

    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(
            name="experiment_name"
            sync_config=tune.SyncConfig(
                upload_dir="s3://bucket-name/sub-path/"
            )
        )
    )
    tuner.fit()

For more details or customization, see our
:ref:`guide on configuring storage in a distributed Tune experiment <tune-storage-options>`.



.. _tune-distributed-spot:

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

Spot instances may be removed suddenly while trials are still running. Often times this may be difficult to deal with when using other distributed hyperparameter optimization frameworks. Tune allows users to mitigate the effects of this by preserving the progress of your model training through :ref:`checkpointing <tune-function-checkpointing>`.

.. literalinclude:: /../../python/ray/tune/tests/tutorial.py
    :language: python
    :start-after: __trainable_run_begin__
    :end-before: __trainable_run_end__


Example for using spot instances (AWS)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is an example for running Tune on spot instances. This assumes your AWS credentials have already been setup (``aws configure``):

1. Download a full example Tune experiment script here. This includes a Trainable with checkpointing: :download:`mnist_pytorch_trainable.py </../../python/ray/tune/examples/mnist_pytorch_trainable.py>`. To run this example, you will need to install the following:

.. code-block:: bash

    $ pip install ray torch torchvision filelock

2. Download an example cluster yaml here: :download:`tune-default.yaml </../../python/ray/tune/examples/tune-default.yaml>`
3. Run ``ray submit`` as below to run Tune across them. Append ``[--start]`` if the cluster is not up yet. Append ``[--stop]`` to automatically shutdown your nodes after running.

.. code-block:: bash

    ray submit tune-default.yaml mnist_pytorch_trainable.py --start -- --ray-address=localhost:6379


4. Optionally for testing on AWS or GCP, you can use the following to kill a random worker node after all the worker nodes are up

.. code-block:: bash

    $ ray kill-random-node tune-default.yaml --hard

To summarize, here are the commands to run:

.. code-block:: bash

    wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/tune/examples/mnist_pytorch_trainable.py
    wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/tune/tune-default.yaml
    ray submit tune-default.yaml mnist_pytorch_trainable.py --start -- --ray-address=localhost:6379

    # wait a while until after all nodes have started
    ray kill-random-node tune-default.yaml --hard

You should see Tune eventually continue the trials on a different worker node. See the :ref:`Fault Tolerance <tune-fault-tol>` section for more details.

You can also specify ``sync_config=tune.SyncConfig(upload_dir=...)``, as part of ``air.RunConfig``, which is taken in by ``Tuner``, to sync results with a cloud storage like S3, allowing you to persist results in case you want to start and stop your cluster automatically.

.. _tune-fault-tol:

Fault Tolerance
---------------

Tune will automatically restart trials in case of trial failures/error (if ``max_failures != 0``), both in the single node and distributed setting.

Tune will restore trials from the latest checkpoint, where available. In the distributed setting, Tune will automatically sync the trial folder with the driver. For example, if a node is lost while a trial (specifically, the corresponding Trainable actor of the trial) is still executing on that node and a checkpoint of the trial exists, Tune will wait until available resources are available to begin executing the trial again.
See :ref:`here for information on checkpointing <tune-function-checkpointing>`.


If the trial/actor is placed on a different node, Tune will automatically push the previous checkpoint file to that node and restore the remote trial actor state, allowing the trial to resume from the latest checkpoint even after failure.

Recovering From Failures
~~~~~~~~~~~~~~~~~~~~~~~~

Tune automatically persists the progress of your entire experiment (a ``Tuner.fit()`` session), so if an experiment crashes or is otherwise cancelled, it can be resumed through :meth:`Tuner.restore() <ray.tune.tuner.Tuner.restore>`.

.. _tune-distributed-common:

Common Commands
---------------

Below are some commonly used commands for submitting experiments. Please see the :ref:`Clusters page <cluster-index>` to see find more comprehensive documentation of commands.

.. code-block:: bash

    # Upload `tune_experiment.py` from your local machine onto the cluster. Then,
    # run `python tune_experiment.py --address=localhost:6379` on the remote machine.
    $ ray submit CLUSTER.YAML tune_experiment.py -- --address=localhost:6379

    # Start a cluster and run an experiment in a detached tmux session,
    # and shut down the cluster as soon as the experiment completes.
    # In `tune_experiment.py`, set `tune.SyncConfig(upload_dir="s3://...")`
    # and pass it to `sync_config=...` to persist results
    $ ray submit CLUSTER.YAML --tmux --start --stop tune_experiment.py -- --address=localhost:6379

    # To start or update your cluster:
    $ ray up CLUSTER.YAML [-y]

    # Shut-down all instances of your cluster:
    $ ray down CLUSTER.YAML [-y]

    # Run TensorBoard and forward the port to your own machine.
    $ ray exec CLUSTER.YAML 'tensorboard --logdir ~/ray_results/ --port 6006' --port-forward 6006

    # Run Jupyter Lab and forward the port to your own machine.
    $ ray exec CLUSTER.YAML 'jupyter lab --port 6006' --port-forward 6006

    # Get a summary of all the experiments and trials that have executed so far.
    $ ray exec CLUSTER.YAML 'tune ls ~/ray_results'

    # Upload and sync file_mounts up to the cluster with this command.
    $ ray rsync-up CLUSTER.YAML

    # Download the results directory from your cluster head node to your local machine on ``~/cluster_results``.
    $ ray rsync-down CLUSTER.YAML '~/ray_results' ~/cluster_results

    # Launching multiple clusters using the same configuration.
    $ ray up CLUSTER.YAML -n="cluster1"
    $ ray up CLUSTER.YAML -n="cluster2"
    $ ray up CLUSTER.YAML -n="cluster3"

Troubleshooting
---------------

Sometimes, your program may freeze. Run this to restart the Ray cluster without running any of the installation commands.

.. code-block:: bash

    $ ray up CLUSTER.YAML --restart-only
