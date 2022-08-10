.. warning::
    This page is under construction!

.. _ray-job-sdk-under-construction:

Python SDK
^^^^^^^^^^

The Job Submission Python SDK is the recommended way to submit jobs programmatically. Jump to the :ref:`API Reference<ray-job-submission-sdk-ref>`, or continue reading for a quick overview.

.. note::
    This component is in **beta**. APIs may change before becoming stable.

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
For more details on production deployment scenarios, check out the guides for deploying Ray on :ref:`VMs <ref-cluster-quick-start-vms-under-construction>` and :ref:`Kubernetes <kuberay-quickstart>`.

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
    )

.. tip::

    By default, the Ray job server will generate a new ``job_id`` and return it, but you can alternatively choose a unique ``job_id`` string first and pass it into :code:`submit_job`.
    In this case, the Job will be executed with your given id, and will throw an error if the same ``job_id`` is submitted more than once for the same Ray cluster.

Because job submission is asynchronous, the above call will return immediately.
Now we can write a simple polling loop that checks the job status until it reaches a terminal state (namely, ``JobStatus.SUCCEEDED``, ``JobStatus.STOPPED``, or ``JobStatus.FAILED``).
We can also get the output of the job by calling ``client.get_job_logs``.

.. code-block:: python

    from ray.job_submission import JobStatus
    import time

    def wait_until_finish(job_id):
        start = time.time()
        timeout = 5
        while time.time() - start <= timeout:
            status = client.get_job_status(job_id)
            print(f"status: {status}")
            if status in {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}:
                break
            time.sleep(1)


    wait_until_finish(job_id)
    logs = client.get_job_logs(job_id)

The output should look something like this:

.. code-block:: bash

    status: JobStatus.SUCCEEDED
    hello world

Interacting with Long-running Jobs
----------------------------------

In addition to getting the current status and output of a job, a submitted job can also be stopped by the user before it finishes executing.

.. code-block:: python

    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint="python -c 'import time; time.sleep(60)'",
        runtime_env={}
    )
    wait_until_finish(job_id)
    client.stop_job(job_id)
    wait_until_finish(job_id)
    logs = client.get_job_logs(job_id)

To get information about all jobs, call ``client.list_jobs()``.  This returns a ``Dict[str, JobInfo]`` object mapping Job IDs to their information.

Dependency Management
---------------------

Similar to the :ref:`Jobs CLI <jobs-quickstart-under-construction>`, we can also package our application's dependencies by using a Ray :ref:`runtime environment <runtime environment>`.
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


For full details, see the :ref:`API Reference<ray-job-submission-sdk-ref>`.
