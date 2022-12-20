.. _ray-yarn-deploy:

Deploying on YARN
=================

.. warning::

  Running Ray on YARN is still a work in progress. If you have a
  suggestion for how to improve this documentation or want to request
  a missing feature, please feel free to create a pull request or get in touch
  using one of the channels in the `Questions or Issues?`_ section below.

This document assumes that you have access to a YARN cluster and will walk
you through using `Skein`_ to deploy a YARN job that starts a Ray cluster and
runs an example script on it.

Skein uses a declarative specification (either written as a yaml file or using the Python API) and allows users to launch jobs and scale applications without the need to write Java code.

You will first need to install Skein: ``pip install skein``.

The Skein ``yaml`` file and example Ray program used here are provided in the
`Ray repository`_ to get you started. Refer to the provided ``yaml``
files to be sure that you maintain important configuration options for Ray to
function properly.

.. _`Ray repository`: https://github.com/ray-project/ray/tree/master/doc/yarn

Skein Configuration
-------------------

A Ray job is configured to run as two `Skein services`:

1. The ``ray-head`` service that starts the Ray head node and then runs the
   application.
2. The ``ray-worker`` service that starts worker nodes that join the Ray cluster.
   You can change the number of instances in this configuration or at runtime
   using ``skein container scale`` to scale the cluster up/down.

The specification for each service consists of necessary files and commands that will be run to start the service.

.. code-block:: yaml

    services:
        ray-head:
            # There should only be one instance of the head node per cluster.
            instances: 1
            resources:
                # The resources for the worker node.
                vcores: 1
                memory: 2048
            files:
                ...
            script:
                ...
        ray-worker:
            # Number of ray worker nodes to start initially.
            # This can be scaled using 'skein container scale'.
            instances: 3
            resources:
                # The resources for the worker node.
                vcores: 1
                memory: 2048
            files:
                ...
            script:
                ...

Packaging Dependencies
----------------------

Use the ``files`` option to specify files that will be copied into the YARN container for the application to use. See `the Skein file distribution page <https://jcrist.github.io/skein/distributing-files.html>`_ for more information.

.. code-block:: yaml

    services:
        ray-head:
            # There should only be one instance of the head node per cluster.
            instances: 1
            resources:
                # The resources for the head node.
                vcores: 1
                memory: 2048
            files:
                # ray/doc/yarn/example.py
                example.py: example.py
            #     # A packaged python environment using `conda-pack`. Note that Skein
            #     # doesn't require any specific way of distributing files, but this
            #     # is a good one for python projects. This is optional.
            #     # See https://jcrist.github.io/skein/distributing-files.html
            #     environment: environment.tar.gz

Ray Setup in YARN
-----------------

Below is a walkthrough of the bash commands used to start the ``ray-head`` and ``ray-worker`` services. Note that this configuration will launch a new Ray cluster for each application, not reuse the same cluster.

Head node commands
~~~~~~~~~~~~~~~~~~

Start by activating a pre-existing environment for dependency management.

.. code-block:: bash

    source environment/bin/activate

Register the Ray head address needed by the workers in the Skein key-value store.

.. code-block:: bash

    skein kv put --key=RAY_HEAD_ADDRESS --value=$(hostname -i) current

Start all the processes needed on the ray head node. By default, we set object store memory
and heap memory to roughly 200 MB. This is conservative and should be set according to application needs.

.. code-block:: bash

    ray start --head --port=6379 --object-store-memory=200000000 --memory 200000000 --num-cpus=1

Execute the user script containing the Ray program.

.. code-block:: bash

    python example.py

Clean up all started processes even if the application fails or is killed.

.. code-block:: bash

    ray stop
    skein application shutdown current

Putting things together, we have:

.. literalinclude:: /cluster/doc_code/yarn/ray-skein.yaml
   :language: yaml
   :start-after: # Head service
   :end-before: # Worker service


Worker node commands
~~~~~~~~~~~~~~~~~~~~

Fetch the address of the head node from the Skein key-value store.

.. code-block:: bash

    RAY_HEAD_ADDRESS=$(skein kv get current --key=RAY_HEAD_ADDRESS)

Start all of the processes needed on a ray worker node, blocking until killed by Skein/YARN via SIGTERM. After receiving SIGTERM, all started processes should also die (ray stop).

.. code-block:: bash

    ray start --object-store-memory=200000000 --memory 200000000 --num-cpus=1 --address=$RAY_HEAD_ADDRESS:6379 --block; ray stop

Putting things together, we have:

.. literalinclude:: /cluster/doc_code/yarn/ray-skein.yaml
   :language: yaml
   :start-after: # Worker service

Running a Job
-------------

Within your Ray script, use the following to connect to the started Ray cluster:

.. literalinclude:: /cluster/doc_code/yarn/example.py
    :language: python
    :start-after: if __name__ == "__main__"

You can use the following command to launch the application as specified by the Skein YAML file.

.. code-block:: bash

    skein application submit [TEST.YAML]

Once it has been submitted, you can see the job running on the YARN dashboard.

.. image:: /cluster/images/yarn-job.png

Cleaning Up
-----------

To clean up a running job, use the following (using the application ID):

.. code-block:: bash

    skein application shutdown $appid

Questions or Issues?
--------------------

.. include:: /_includes/_help.rst

.. _`Skein`: https://jcrist.github.io/skein/
