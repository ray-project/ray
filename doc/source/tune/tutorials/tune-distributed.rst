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
2. Specify ``ray.init(address=...)`` in your script :ref:`to connect to the existing Ray cluster <using-ray-on-a-cluster>`.
3. Run the script on the head node, or use :ref:`ray submit <ray-submit-doc>`, or use :ref:`Ray Job Submission <jobs-overview>` (in beta starting with Ray 1.12).


.. _tune-distributed-local:

Local Cluster Setup
-------------------

If you already have a list of nodes, you can follow the local :ref:`private cluster setup <cluster-private-setup>`. Below is an example cluster configuration as ``tune-default.yaml``:

.. literalinclude:: /../../python/ray/tune/examples/tune-local-default.yaml
   :language: yaml

``ray up`` starts Ray on the cluster of nodes.

.. code-block:: bash

    ray up tune-default.yaml

``ray submit`` uploads ``tune_script.py`` to the cluster and runs ``python tune_script.py [args]``.

.. code-block:: bash

    ray submit tune-default.yaml tune_script.py -- --ray-address=localhost:6379

Manual Local Cluster Setup
~~~~~~~~~~~~~~~~~~~~~~~~~~

If you run into issues using the local cluster setup (or want to add nodes manually), you can use :ref:`the manual cluster setup <cluster-index>`. At a glance,

.. tune-distributed-cloud:

Launching a cloud cluster
-------------------------

.. tip::

    If you have already have a list of nodes, go to :ref:`tune-distributed-local`.

Ray currently supports AWS and GCP. Follow the instructions below to launch nodes on AWS (using the Deep Learning AMI). See the :ref:`cluster setup documentation <cluster-cloud>`. Save the below cluster configuration (``tune-default.yaml``):

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


Note that you can customize the directory of results by running: ``tune.run(local_dir=..)``. You can then point TensorBoard to that directory to visualize results. You can also use `awless <https://github.com/wallix/awless>`_ for easy cluster management on AWS.


Running a distributed experiment
--------------------------------

Running a distributed (multi-node) experiment requires Ray to be started already. You can do this on local machines or on the cloud.

Across your machines, Tune will automatically detect the number of GPUs and CPUs without you needing to manage ``CUDA_VISIBLE_DEVICES``.

To execute a distributed experiment, call ``ray.init(address=XXX)`` before ``tune.run``, where ``XXX`` is the Ray address, which defaults to ``localhost:6379``. The Tune python script should be executed only on the head node of the Ray cluster.

One common approach to modifying an existing Tune experiment to go distributed is to set an ``argparse`` variable so that toggling between distributed and single-node is seamless.

.. code-block:: python

    import ray
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--address")
    args = parser.parse_args()
    ray.init(address=args.address)

    tune.run(...)

.. code-block:: bash

    # On the head node, connect to an existing ray cluster
    $ python tune_script.py --ray-address=localhost:XXXX

If you used a cluster configuration (starting a cluster with ``ray up`` or ``ray submit --start``), use:

.. code-block:: bash

    ray submit tune-default.yaml tune_script.py -- --ray-address=localhost:6379

.. tip::

    1. In the examples, the Ray address commonly used is ``localhost:6379``.
    2. If the Ray cluster is already started, you should not need to run anything on the worker nodes.


Syncing
-------

In a distributed experiment, you should try to use :ref:`cloud checkpointing <tune-cloud-checkpointing>` to
reduce synchronization overhead. For this, you just have to specify an ``upload_dir`` in the
:class:`tune.SyncConfig <ray.tune.SyncConfig>`:

.. code-block:: python

    from ray import tune

    tune.run(
        trainable,
        name="experiment_name",
        sync_config=tune.SyncConfig(
            upload_dir="s3://bucket-name/sub-path/"
        )
    )


For more details or customization, see our
:ref:`guide on checkpointing <tune-checkpoint-syncing>`.



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

You can also specify ``tune.run(sync_config=tune.SyncConfig(upload_dir=...))`` to sync results with a cloud storage like S3, allowing you to persist results in case you want to start and stop your cluster automatically.

.. _tune-fault-tol:

Fault Tolerance
---------------

Tune will automatically restart trials in case of trial failures/error (if ``max_failures != 0``), both in the single node and distributed setting.

Tune will restore trials from the latest checkpoint, where available. In the distributed setting, Tune will automatically sync the trial folder with the driver. For example, if a node is lost while a trial (specifically, the corresponding Trainable actor of the trial) is still executing on that node and a checkpoint of the trial exists, Tune will wait until available resources are available to begin executing the trial again.
See :ref:`our checkpointing guide <tune-checkpoint-syncing>`.


If the trial/actor is placed on a different node, Tune will automatically push the previous checkpoint file to that node and restore the remote trial actor state, allowing the trial to resume from the latest checkpoint even after failure.

Recovering From Failures
~~~~~~~~~~~~~~~~~~~~~~~~

Tune automatically persists the progress of your entire experiment (a ``tune.run`` session), so if an experiment crashes or is otherwise cancelled, it can be resumed by passing one of True, False, "LOCAL", "REMOTE", or "PROMPT" to ``tune.run(resume=...)``. Note that this only works if trial checkpoints are detected, whether it be by manual or periodic checkpointing.

**Settings:**

- The default setting of ``resume=False`` creates a new experiment.
- ``resume="LOCAL"`` and ``resume=True`` restore the experiment from ``local_dir/[experiment_name]``.
- ``resume="REMOTE"`` syncs the upload dir down to the local dir and then restores the experiment from ``local_dir/experiment_name``.
- ``resume="ERRORED_ONLY"`` will look for errored trials in ``local_dir/[experiment_name]`` and only run these (and start from scratch).
- ``resume="PROMPT"`` will cause Tune to prompt you for whether you want to resume. You can always force a new experiment to be created by changing the experiment name.
- ``resume="AUTO"`` will automatically look for an existing experiment at ``local_dir/[experiment_name]``. If found, it will be continued (as if ``resume=True``), otherwise a new experiment is started.

Note that trials will be restored to their last checkpoint. If trial checkpointing is not enabled, unfinished trials will be restarted from scratch.

E.g.:

.. code-block:: python

    tune.run(
        my_trainable,  # Function trainable that saves checkpoints
        local_dir="~/path/to/results",
        resume=True
    )

Upon a second run, this will restore the entire experiment state from ``~/path/to/results/my_experiment_name``. Importantly, any changes to the experiment specification upon resume will be ignored. For example, if the previous experiment has reached its termination, then resuming it with a new stop criterion will not run. The new experiment will terminate immediately after initialization. If you want to change the configuration, such as training more iterations, you can do so restore the checkpoint by setting ``restore=<path-to-checkpoint>`` - note that this only works for a single trial.

.. _tune-distributed-common:

Common Commands
---------------

Below are some commonly used commands for submitting experiments. Please see the :ref:`Autoscaler page <cluster-cloud>` to see find more comprehensive documentation of commands.

.. code-block:: bash

    # Upload `tune_experiment.py` from your local machine onto the cluster. Then,
    # run `python tune_experiment.py --address=localhost:6379` on the remote machine.
    $ ray submit CLUSTER.YAML tune_experiment.py -- --address=localhost:6379

    # Start a cluster and run an experiment in a detached tmux session,
    # and shut down the cluster as soon as the experiment completes.
    # In `tune_experiment.py`, set `tune.SyncConfig(upload_dir="s3://...")`
    # and pass it to `tune.run(sync_config=...)` to persist results
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
