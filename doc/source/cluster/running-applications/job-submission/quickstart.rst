.. _jobs-quickstart:

=================================
Quickstart Using the Ray Jobs CLI
=================================

In this guide, we will walk through the Ray Jobs CLIs available for submitting and interacting with a Ray Job.

.. note::

  The Jobs API is in beta and may change before becoming stable.

Setup
-----

Ray Jobs is available in versions 1.9+ and requires a full installation of Ray. You can do this by running:

.. code-block:: shell

    pip install ray[default]

See the :ref:`installation guide <installation>` for more details on installing Ray.

To run a Ray Job, we also need to be able to send HTTP requests to a Ray Cluster.
For convenience, this guide will assume that you are using a local Ray Cluster, which we can start by running:

.. code-block:: shell

    ray start --head
    # ...
    # 2022-08-10 09:54:57,664   INFO services.py:1476 -- View the Ray dashboard at http://127.0.0.1:8265
    # ...

This will create a Ray head node on our local machine that we can use for development purposes.
Note the Ray Dashboard URL that is printed when starting or connecting to a Ray Cluster; we will use this URL later to submit a Ray Job.
For more details on production deployment scenarios, check out the guides for deploying Ray on :ref:`VMs <vm-cluster-quick-start>` and :ref:`Kubernetes <kuberay-quickstart>`.


Submitting a Ray Job
--------------------

Let's start with a sample script that can be run locally. The following script uses Ray APIs to submit a task and print its return value:

.. code-block:: python

    # script.py
    import ray

    @ray.remote
    def hello_world():
        return "hello world"

    ray.init()
    print(ray.get(hello_world.remote()))

Put this file in a local directory of your choice, with the filename ``script.py``, so your working directory will look like:

.. code-block:: bash

  | your_working_directory ("./")
  | ├── script.py

Next, we will find the HTTP address of the Ray Cluster to which we can submit a Ray Job request.
Ray Jobs are submitted to the same address used by the **Ray Dashboard**.
By default, this uses port 8265.

If you are using a local Ray Cluster (``ray start --head``), you can connect directly at ``http://127.0.0.1:8265``.
If you are using a Ray Cluster started on VMs or Kubernetes, you can follow the instructions there for setting up network access from a client.
.. TODO: Add links to networking tips for VMs and kubernetes.

To tell the Ray Jobs CLI how to find your Ray Cluster, we will pass the Ray Dashboard address. This can be done by setting the ``RAY_ADDRESS`` environment variable:

.. code-block:: bash

    $ export RAY_ADDRESS="http://127.0.0.1:8265"

Alternatively, you can also pass the ``--address=http://127.0.0.1:8265`` flag explicitly to each Ray Jobs CLI command, or prepend each command with ``RAY_ADDRESS=http://127.0.0.1:8265``.

To submit the Ray Job, we use ``ray job submit``:

.. code-block:: bash

    $ ray job submit -- python script.py 

    # Job submission server address: http://127.0.0.1:8265

    # -------------------------------------------------------
    # Job 'raysubmit_inB2ViQuE29aZRJ5' submitted successfully
    # -------------------------------------------------------

    # Next steps
    #   Query the logs of the job:
    #     ray job logs raysubmit_inB2ViQuE29aZRJ5
    #   Query the status of the job:
    #     ray job status raysubmit_inB2ViQuE29aZRJ5
    #   Request the job to be stopped:
    #     ray job stop raysubmit_inB2ViQuE29aZRJ5

    # Tailing logs until the job exits (disable with --no-wait):
    # hello world

    # ------------------------------------------
    # Job 'raysubmit_inB2ViQuE29aZRJ5' succeeded
    # ------------------------------------------

By default, this command will run the script on the Ray Cluster and wait until the job has finished. Also note the ``hello world`` printout: the stdout of the job is streamed back to the client.

Interacting with Long-running Jobs
----------------------------------

For long-running applications, it is not desirable to require the client to wait for the job to finish.
To do this, we can pass the ``--no-wait`` flag to ``ray job submit`` and use the other CLI commands to check on the job's status.
Let's try this out with a modified script that submits a task every second in an infinite loop:

.. code-block:: python

    # script.py
    import ray
    time.sleep(1)

    @ray.remote
    def hello_world():
        return "hello world"

    ray.init()
    while True:
        print(ray.get(hello_world.remote()))
        time.sleep(1)

Now let's submit the Ray Job:

.. code-block:: shell

	$ ray job submit --no-wait -- python script.py 
	# Job submission server address: http://127.0.0.1:8265

	# -------------------------------------------------------
	# Job 'raysubmit_tUAuCKubPAEXh6CW' submitted successfully
	# -------------------------------------------------------

	# Next steps
	#   Query the logs of the job:
	# 	ray job logs raysubmit_tUAuCKubPAEXh6CW
	#   Query the status of the job:
	# 	ray job status raysubmit_tUAuCKubPAEXh6CW
	#   Request the job to be stopped:
	# 	ray job stop raysubmit_tUAuCKubPAEXh6CW

We can later get the stdout using the provided ``ray job logs`` command:

.. code-block:: shell

    $ ray job logs raysubmit_tUAuCKubPAEXh6CW
    # Job submission server address: http://127.0.0.1:8265
    # hello world
    # hello world
    # hello world
    # hello world
    # hello world

And the current status of the job using ``ray job status``:

.. code-block:: shell

    $ ray job status raysubmit_tUAuCKubPAEXh6CW
    # Job submission server address: http://127.0.0.1:8265
    # Status for job 'raysubmit_tUAuCKubPAEXh6CW': RUNNING
    # Status message: Job is currently running.

Finally, if we want to cancel the job, we can use ``ray job stop``:

.. code-block:: shell

    $ ray job stop raysubmit_tUAuCKubPAEXh6CW
    # Job submission server address: http://127.0.0.1:8265
    # Attempting to stop job raysubmit_tUAuCKubPAEXh6CW
    # Waiting for job 'raysubmit_tUAuCKubPAEXh6CW' to exit (disable with --no-wait):
    # Job 'raysubmit_tUAuCKubPAEXh6CW' was stopped

    $ ray job status raysubmit_tUAuCKubPAEXh6CW
    # Job submission server address: http://127.0.0.1:8265
    # Job 'raysubmit_tUAuCKubPAEXh6CW' was stopped


Dependency Management
---------------------

To run a distributed application, we need to make sure that all workers run in the same environment.
This can be challenging if multiple applications in the same Ray Cluster have different and conflicting dependencies.

To avoid dependency conflicts, Ray provides a mechanism called :ref:`runtime environments <runtime-environments>`. Runtime environments allow an application to override the default environment on the Ray Cluster and run in an isolated environment, similar to virtual environments in single-node Python. Dependencies can include both files and Python packages.

Ray Jobs provides an option to specify the runtime environment when submitting a job. On the Ray Cluster, Ray will then install the runtime environment across the workers and ensure that tasks in that job run in the same environment. To see how this works, we'll use a Python script that prints the current version of the ``requests`` module in a Ray task.

.. code-block:: python

    import ray
    import requests

    @ray.remote
    def get_requests_version():
        return requests.__version__

    ray.init()
    print("requests version:", ray.get(get_requests_version.remote()))

First, let's submit this job using the default environment. This is the environment in which the Ray Cluster was started.

.. code-block:: bash

    $ ray job submit -- python script.py 
    # Job submission server address: http://127.0.0.1:8265
    # 
    # -------------------------------------------------------
    # Job 'raysubmit_seQk3L4nYWcUBwXD' submitted successfully
    # -------------------------------------------------------
    # 
    # Next steps
    #   Query the logs of the job:
    #     ray job logs raysubmit_seQk3L4nYWcUBwXD
    #   Query the status of the job:
    #     ray job status raysubmit_seQk3L4nYWcUBwXD
    #   Request the job to be stopped:
    #     ray job stop raysubmit_seQk3L4nYWcUBwXD
    # 
    # Tailing logs until the job exits (disable with --no-wait):
    # requests version: 2.28.1
    # 
    # ------------------------------------------
    # Job 'raysubmit_seQk3L4nYWcUBwXD' succeeded
    # ------------------------------------------

Now let's try it with a runtime environment that pins the version of the ``requests`` module:

.. code-block:: bash

    $ ray job submit --runtime-env-json='{"pip": ["requests==2.26.0"]}' -- python script.py 
    # Job submission server address: http://127.0.0.1:8265

    # -------------------------------------------------------
    # Job 'raysubmit_vGGV4MiP9rYkYUnb' submitted successfully
    # -------------------------------------------------------

    # Next steps
    #   Query the logs of the job:
    #     ray job logs raysubmit_vGGV4MiP9rYkYUnb
    #   Query the status of the job:
    #     ray job status raysubmit_vGGV4MiP9rYkYUnb
    #   Request the job to be stopped:
    #     ray job stop raysubmit_vGGV4MiP9rYkYUnb

    # Tailing logs until the job exits (disable with --no-wait):
    # requests version: 2.26.0

    # ------------------------------------------
    # Job 'raysubmit_vGGV4MiP9rYkYUnb' succeeded
    # ------------------------------------------

The full API reference for the Ray Jobs CLI can be found :ref:`here <ray-job-submission-api-ref>`.
For more information on other ways to submit Ray Jobs, check out the guides for :ref:`programmatic job submission <ray-job-sdk>` and :ref:`job submission using REST <ray-job-rest-api>`.
