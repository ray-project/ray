.. warning::
    This page is under construction!

.. _ray-job-sdk:

Python SDK
^^^^^^^^^^

The Job Submission Python SDK is the recommended way to submit jobs programmatically.  Jump to the :ref:`API Reference<ray-job-submission-sdk-ref>`, or continue reading for a quick overview.

SDK calls are made via a ``JobSubmissionClient`` object.  To initialize the client, provide the Ray cluster head node address and the port used by the Ray Dashboard (``8265`` by default). For this example, we'll use a local Ray cluster, but the same example will work for remote Ray cluster addresses.

.. code-block:: python

    from ray.job_submission import JobSubmissionClient

    # If using a remote cluster, replace 127.0.0.1 with the head node's IP address.
    client = JobSubmissionClient("http://127.0.0.1:8265")

Then we can submit our application to the Ray cluster via the Job SDK.

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

    By default, the Ray job server will generate a new ``job_id`` and return it, but you can alternatively choose a unique ``job_id`` string first and pass it into :code:`submit_job`.
    In this case, the Job will be executed with your given id, and will throw an error if the same ``job_id`` is submitted more than once for the same Ray cluster.

Now we can write a simple polling loop that checks the job status until it reaches a terminal state (namely, ``JobStatus.SUCCEEDED``, ``JobStatus.STOPPED``, or ``JobStatus.FAILED``), and gets the logs at the end.
We expect to see the numbers printed from our actor, as well as the correct version of the :code:`requests` module specified in the ``runtime_env``.

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

The output should be as follows:

.. code-block:: bash

    status: JobStatus.PENDING
    status: JobStatus.RUNNING
    status: JobStatus.SUCCEEDED

    1
    2
    3
    4
    5

    2.26.0

.. tip::

    Instead of a local directory (``"./"`` in this example), you can also specify remote URIs for your job's working directory, such as S3 buckets or Git repositories. See :ref:`remote-uris` for details.

A submitted job can be stopped by the user before it finishes executing.

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

For full details, see the :ref:`API Reference<ray-job-submission-sdk-ref>`.
