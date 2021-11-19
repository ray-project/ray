

=========================
From laptop to production
=========================

.. warning::

    Ray Job Submission is at Alpha phase with APIs mostly stable but subject to change in the future.

Concepts
--------

- **Package**: A collection of files and configurations that defines an application, thus allowing it to be executed in a different environment remotely (ideally self-contained).

- **Job**: A Ray application that will be submitted to a Ray cluster for execution. Once a job is submitted, it runs once on the cluster to completion or failure. Retries or different runs with different parameters should be handled by the submitter. Jobs are scoped to the lifetime of a ray cluster.

- **Job Manager**: An entity that manages the lifecycle of a Job and potentially multiple ray clusters, such as scheduling, killing, polling status, getting logs, and persisting inputs / outputs. Should be highly available (HA) by default. Can be any framework with these abilities, such as Airflow.

Within the context of job submission, the packaging part is equivalent to :ref:`Runtime Environments<runtime-environments>`, where we can dynamically configure your desirable ray cluster, actor or task level runtime environment for your submitted job.

**Key inputs for job submission**

- **Entrypoint**: Shell command to execute once your code is unpackaged with runtime environment configured
    - Typically :code:`python your_script.py`, can also be any shell script such as :code:`echo hello`.
- **Runtime Environment**:
    - :code:`working_dir` as local directory: It will be automatically zipped and uploaded to target ray cluster, then unpacked to where your submitted application runs.
    - :code:`working_dir` as remote URIs, such as s3, git or others, it will be downloaded and unpacked to where your submitted application runs. For details, see :ref:`Runtime Environments<runtime-environments>` for details.

.. note::
  **The goal of Ray job submssion is to provide a lightweight mechanism for user to submit their locally developed and tested application to a running local / remote ray cluster, thus enabling the user to package, deploy, and manage their ray application as jobs. These jobs can be submitted by a job manager of their choice.**

Example - Setup
---------------

Let's start with a sample ray script as example for job submission. Once executed locally, this script will use ray APIs to print counter value of a remote actor from 1 to 5, and print the version of 'requests' module it's using.

We can put this file in a local directory of your choice, with filename "script.py", so your working directory will look like:

.. code-block:: bash

  | your_working_directory ("./")
  | ├── script.py

.. code-block:: python

    import ray
    import requests

    ray.init()

    @ray.remote
    class Counter:
        def __init__(self):
            self.counter = 0

        def inc(self):
            self.counter += 1

        def get_counter(self):
            return self.counter

    counter = Counter.remote()

    for _ in range(5):
        ray.get(counter.inc.remote())
        print(ray.get(counter.get_counter.remote()))

    print(requests.__version__)

Ray Job SDK
------------

Ray job sdk is the recommended way to submit jobs programmatically.

| Start a local ray cluster headnode

.. code-block:: bash

    ray start --head

We can import and intialize job submission client by providing an valid ray cluster headnode address where port is same as ray dashboard. We're using your local ray cluster as example but it works the same for remote ray cluster addresses.

.. code-block:: python

    from ray.dashboard.modules.job.sdk import JobSubmissionClient

    client = JobSubmissionClient("http://127.0.0.1:8265")

Then we can submit our application to ray cluster via job SDK.

.. code-block:: python

    job_id = client.submit_job(
        # Entry point to execute
        entrypoint="python script.py",
        # Working dir
        runtime_env={
            "working_dir": "./",
            "pip": ["requests==2.26.0"]
        }
    )

.. tip::

    By default ray job server will generate a new uuid as return value, but you can also generate your unique job_id first and pass it into :code:`submit_job`. In this case the job will be executed with your given id, and will throw error if same job_id is submitted more than once for the same ray cluster. This can facilitate job id management assuming you have your own job registry to generate and persist unique job ids.

Now we can have a simple polling loop that checks job status until it reaches terminal state, and get logs at the end. We are expected to see actor printed numbers as well as correct version of :code:`requests` module used via runtime_env.

.. code-block:: python

    from ray.dashboard.modules.job.common import JobStatus, JobStatusInfo

    def wait_until_finish(job_id):
        start = time.time()
        timeout = 5
        while time.time() - start <= timeout:
            status_info = client.get_job_status(job_id)
            status = status_info.status
            print(f"status: {status}")
            if status in {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}:
                break
            time.sleep(1)


    wait_until_finish(job_id)
    logs = client.get_job_logs(job_id)
    print(logs)

.. tip::

    We can also use other remote uris for runtime env, such as S3 or GIT. See "Remote URIs" section of :ref:`Runtime Environments<runtime-environments>` for details.

Submitted job can be stopped by user before finish executing.

.. code-block:: python

    job_id = client.submit_job(
        # Entry point to execute
        entrypoint="python -c 'import time; time.sleep(60)'",
        runtime_env={}
    )
    wait_until_finish(job_id)
    client.stop_job(job_id)
    wait_until_finish(job_id)
    logs = client.get_job_logs(job_id)
    print(logs)


Job CLI API
-----------

In addition to job SDK, we can also submit ray application via CLI.


| Ensure we have a local ray cluster with running headnode.

.. code-block:: bash

   ray start --head

.. code-block:: python

    ❯ ray job submit --address="127.0.0.1:8265" -- "python -c 'print(123); import time; time.sleep(5)'"
    2021-11-18 16:14:47,602	INFO cli.py:103 -- Job submitted successfully: raysubmit_GsQYzyvZpgNicU8F.
    2021-11-18 16:14:47,602	INFO cli.py:104 -- Query the status of the job using: `ray job status raysubmit_GsQYzyvZpgNicU8F`.


    ❯ ray job status raysubmit_GsQYzyvZpgNicU8F
    2021-11-18 16:15:07,727	INFO cli.py:125 -- Job status for 'raysubmit_GsQYzyvZpgNicU8F': SUCCEEDED.
    2021-11-18 16:15:07,727	INFO cli.py:127 -- Job finished successfully.


    ❯ ray job logs raysubmit_GsQYzyvZpgNicU8F
    123


Job HTTP API
------------

Under the hood, both Job Client and CLI make HTTP calls to the job server running on ray head node. Therefore user can also directly send requests to corresponding endpoints via HTTP if needed.

Submit job

.. code-block:: python

    resp = requests.post(
        "http://127.0.0.1:8265/api/jobs/submit",
        json={
            "entrypoint": "echo hello",
            "runtime_env": {},
            "job_id": None,
            "metadata": {"job_submission_id": "123"}
        }
    )
    rst = json.loads(resp.text)
    job_id = rst["job_id"]
    print(job_id)

Query and poll for job status

.. code-block:: python

    start = time.time()
    while time.time() - start <= 10:
        resp = requests.get(
            "http://127.0.0.1:8265/api/jobs/status",
            params={
                "job_id": job_id,
            }
        )
        rst = json.loads(resp.text)
        status = rst["job_status"]
        print(f"status: {status}")
        if status in {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}:
            break
        time.sleep(1)

Query for logs

.. code-block:: python

    resp = requests.get(
        "http://127.0.0.1:8265/api/jobs/logs",
        params={
            "job_id": job_id,
        }
    )
    rst = json.loads(resp.text)
    logs = rst["logs"]
    print(logs)


Job Submission Architecture
----------------------------

The following diagram shows the underlying structure and steps for each job submission.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/job/job_subimssion_arch.png
