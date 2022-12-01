.. _ray-job-sdk:

Python SDK Overview
^^^^^^^^^^^^^^^^^^^

The Ray Jobs Python SDK is the recommended way to submit jobs programmatically. Jump to the :ref:`API Reference <ray-job-submission-sdk-ref>`, or continue reading for a quick overview.

Setup
-----

Ray Jobs is available in versions 1.9+ and requires a full installation of Ray. You can do this by running:

.. code-block:: shell

    pip install "ray[default]"

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
See :ref:`Using a Remote Cluster <jobs-remote-cluster>` for tips on port-forwarding if using a remote cluster.
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

SDK calls are made via a ``JobSubmissionClient`` object.  To initialize the client, provide the Ray cluster head node address and the port used by the Ray Dashboard (``8265`` by default). For this example, we'll use a local Ray cluster, but the same example will work for remote Ray cluster addresses.

.. code-block:: python

    from ray.job_submission import JobSubmissionClient

    # If using a remote cluster, replace 127.0.0.1 with the head node's IP address.
    client = JobSubmissionClient("http://127.0.0.1:8265")
    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint="python script.py",
        # Path to the local directory that contains the script.py file
        runtime_env={"working_dir": "./"}
    )
    print(job_id)

.. tip::

    By default, the Ray job server will generate a new ``job_id`` and return it, but you can alternatively choose a unique ``job_id`` string first and pass it into :code:`submit_job`.
    In this case, the Job will be executed with your given id, and will throw an error if the same ``job_id`` is submitted more than once for the same Ray cluster.

Because job submission is asynchronous, the above call will return immediately with output like the following:

.. code-block:: bash

    raysubmit_g8tDzJ6GqrCy7pd6

Now we can write a simple polling loop that checks the job status until it reaches a terminal state (namely, ``JobStatus.SUCCEEDED``, ``JobStatus.STOPPED``, or ``JobStatus.FAILED``).
We can also get the output of the job by calling ``client.get_job_logs``.

.. code-block:: python

    from ray.job_submission import JobSubmissionClient, JobStatus
    import time

    # If using a remote cluster, replace 127.0.0.1 with the head node's IP address.
    client = JobSubmissionClient("http://127.0.0.1:8265")
    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint="python script.py",
        # Path to the local directory that contains the script.py file
        runtime_env={"working_dir": "./"}
    )
    print(job_id)

    def wait_until_status(job_id, status_to_wait_for, timeout_seconds=5):
        start = time.time()
        while time.time() - start <= timeout_seconds:
            status = client.get_job_status(job_id)
            print(f"status: {status}")
            if status in status_to_wait_for:
                break
            time.sleep(1)


    wait_until_status(job_id, {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED})
    logs = client.get_job_logs(job_id)
    print(logs)

The output should look something like this:

.. code-block:: bash

    raysubmit_pBwfn5jqRE1E7Wmc
    status: PENDING
    status: PENDING
    status: RUNNING
    status: RUNNING
    status: RUNNING
    2022-08-22 15:05:55,652 INFO worker.py:1203 -- Using address 127.0.0.1:6379 set in the environment variable RAY_ADDRESS
    2022-08-22 15:05:55,652 INFO worker.py:1312 -- Connecting to existing Ray cluster at address: 127.0.0.1:6379...
    2022-08-22 15:05:55,660 INFO worker.py:1487 -- Connected to Ray cluster. View the dashboard at http://127.0.0.1:8265.
    hello world

Interacting with Long-running Jobs
----------------------------------

In addition to getting the current status and output of a job, a submitted job can also be stopped by the user before it finishes executing.

.. code-block:: python

    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint="python -c 'import time; print(\"Sleeping...\"); time.sleep(60)'"
    )
    wait_until_status(job_id, {JobStatus.RUNNING})
    print(f'Stopping job {job_id}')
    client.stop_job(job_id)
    wait_until_status(job_id, {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED})
    logs = client.get_job_logs(job_id)
    print(logs)

The output should look something like the following:

.. code-block:: bash

    status: PENDING
    status: PENDING
    status: RUNNING
    Stopping job raysubmit_VYCZZ2BQb4tfeCjq
    status: STOPPED
    Sleeping...

To get information about all jobs, call ``client.list_jobs()``.  This returns a ``Dict[str, JobInfo]`` object mapping Job IDs to their information.

Job information (status and associated metadata) is stored on the cluster indefinitely.  
To delete this information, you may call ``client.delete_job(job_id)`` for any job that is already in a terminal state.  
See the :ref:`SDK API Reference <ray-job-submission-sdk-ref>` for more details.

Dependency Management
---------------------

Similar to the :ref:`Jobs CLI <jobs-quickstart>`, we can also package our application's dependencies by using a Ray :ref:`runtime environment <runtime-environments>`.
Using the Python SDK, the syntax looks something like this:

.. code-block:: python

    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint="python script.py",
        # Runtime environment for the job, specifying a working directory and pip package
        runtime_env={
            "working_dir": "./",
            "pip": ["requests==2.26.0"]
        }
    )

.. tip::

    Instead of a local directory (``"./"`` in this example), you can also specify remote URIs for your job's working directory, such as S3 buckets or Git repositories. See :ref:`remote-uris` for details.


For full details, see the :ref:`API Reference <ray-job-submission-sdk-ref>`.


Specifying CPU and GPU resources
--------------------------------

We recommend doing heavy computation within Ray tasks, actors, or Ray libraries, not directly in the top level of your entrypoint script.
No extra configuration is needed to do this.

However, if you need to do computation directly in the entrypoint script and would like to reserve CPU and GPU resources for the entrypoint script, you may specify the ``entrypoint_num_cpus``, ``entrypoint_num_gpus`` and ``entrypoint_resources`` arguments to ``submit_job``.  These arguments function
identically to the ``num_cpus``, ``num_gpus``, and ``resources`` arguments to ``@ray.remote()`` decorator for tasks and actors as described in :ref:`resource-requirements`.

.. code-block:: python

    job_id = client.submit_job(
        entrypoint="python script.py",
        runtime_env={
            "working_dir": "./",
        }
        # Reserve 1 GPU for the entrypoint script
        entrypoint_num_gpus=1
    )

The same arguments are also available as options ``--entrypoint-num-cpus``, ``--entrypoint-num-gpus``, and ``--entrypoint-resources`` to ``ray job submit`` in the Jobs CLI; see :ref:`Ray Job Submission CLI Reference <ray-job-submission-cli-ref>`.

If ``num_gpus`` is not specified, GPUs will still be available to the entrypoint script, but Ray will not provide isolation in terms of visible devices. 
To be precise, the environment variable ``CUDA_VISIBLE_DEVICES`` will not be set in the entrypoint script; it will only be set inside tasks and actors that have `num_gpus` specified in their ``@ray.remote()`` decorator.

.. note::

    Resources specified by ``entrypoint_num_cpus``, ``entrypoint_num_gpus``, and ``entrypoint_resources`` are separate from any resources specified
    for tasks and actors within the job.  
    
    For example, if you specify ``entrypoint_num_gpus=1``, then the entrypoint script will be scheduled on a node with at least 1 GPU,
    but if your script also contains a Ray task defined with ``@ray.remote(num_gpus=1)``, then the task will be scheduled to use a different GPU (on the same node if the node has at least 2 GPUs, or on a different node otherwise).

.. note::
    
    As with the ``num_cpus``, ``num_gpus``, and ``resources`` arguments to ``@ray.remote()`` described in :ref:`resource-requirements`, these arguments only refer to logical resources used for scheduling purposes. The actual CPU and GPU utilization is not controlled or limited by Ray.


.. note::

    By default, 0 CPUs and 0 GPUs are reserved for the entrypoint script.