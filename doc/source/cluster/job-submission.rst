.. _jobs-overview:

===========================================================
Ray Job Submission: Going from your laptop to production
===========================================================

.. note::

    This component is in **beta**.

Ray Job submission is a mechanism to submit locally developed and tested applications to a running remote Ray cluster. It simplifies the user experience of packaging, deploying, and manage their Ray application as a "job". Jobs can be submitted by a "job manager", like Airflow or Kubernetes Jobs.

Concepts
--------

- **Package**: A collection of files and configurations that defines a Ray application, thus allowing it to be executed in a different environment remotely (ideally self-contained). Within the context of Job submission, the packaging part is handled by :ref:`Runtime Environments<runtime-environments>`, where we can dynamically configure your desired Ray cluster environment, actor or task level runtime environment for your submitted Job.

- **Job**: A Ray application submitted to a Ray cluster for execution. Once a Job is submitted, it runs once on the cluster to completion or failure. Retries or different runs with different parameters should be handled by the submitter. Jobs are bound to the lifetime of a Ray cluster, so if your Ray cluster goes down, any running Jobs on that cluster will be terminated.

- **Job Manager**: An entity external to the Ray cluster that manages the lifecycle of a Job and potentially also Ray clusters, such as scheduling, killing, polling status, getting logs, and persisting inputs / outputs. Can be any existing framework with these abilities, such as Airflow.

Example - Setup
---------------

Let's start with a sample Ray script as an example for job submission. Once executed locally, this script will use Ray APIs to print counter value of a remote actor from 1 to 5, and print the version of 'requests' module it's using.

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


| Ensure we have a local Ray cluster with a running head node and the dashboard installed with :code:`pip install "ray[default]"`. The address and port shown in terminal should be where we submit Job requests to.

.. code-block:: bash

   ❯ ray start --head
    Local node IP: 127.0.0.1
    INFO services.py:1360 -- View the Ray dashboard at http://127.0.0.1:8265

.. note::

    If you're building from ray source instead of any pre-built or nightly wheels, please set extra environment variable if you want to use runtime environment features.
    :code:`export RAY_RUNTIME_ENV_LOCAL_DEV_MODE="1"`

Ray Job APIs
------------

We provide three APIs for Job submission: SDK, CLI and HTTP. Both the SDK and CLI use the same HTTP endpoints under the hood. The CLI is easy to use manually on the command line, and the SDK allows you to programmatically interact with jobs.

**Key inputs to Job submission**

- **Entrypoint**: Shell command to run the job.
    - Typically :code:`python your_script.py`, can also be any shell script such as :code:`echo hello`.
- **Runtime Environment**:
    - :code:`working_dir` as local directory: It will be automatically zipped and uploaded to the target Ray cluster, then unpacked to where your submitted application runs.
    - :code:`working_dir` as remote URIs, such as S3, Git or others: It will be downloaded and unpacked to where your submitted application runs. For details, see :ref:`Runtime Environments<runtime-environments>`.

.. warning::

    We currently don't support passing in :code:`requirements.txt` in :code:`pip` yet in job submission so user still need to pass in a list of packages. It will be supported in later releases.


Job CLI API
-----------

The easiest way to get started is to use Job submission CLI.

If we have :code:`RAY_ADDRESS` environment variable set with a local Ray cluster, or just manually set it first:

.. code-block:: bash

    export RAY_ADDRESS="http://127.0.0.1:8265"

.. code-block::

    ❯ ray job submit --runtime-env-json='{"working_dir": "./", "pip": ["requests==2.26.0"]}' -- "python script.py"
    2021-12-01 23:04:52,672	INFO cli.py:25 -- Creating JobSubmissionClient at address: http://127.0.0.1:8265
    2021-12-01 23:04:52,809	INFO sdk.py:144 -- Uploading package gcs://_ray_pkg_bbcc8ca7e83b4dc0.zip.
    2021-12-01 23:04:52,810	INFO packaging.py:352 -- Creating a file package for local directory './'.
    2021-12-01 23:04:52,878	INFO cli.py:105 -- Job submitted successfully: raysubmit_RXhvSyEPbxhcXtm6.
    2021-12-01 23:04:52,878	INFO cli.py:106 -- Query the status of the job using: `ray job status raysubmit_RXhvSyEPbxhcXtm6`.

    ❯ ray job status raysubmit_RXhvSyEPbxhcXtm6
    2021-12-01 23:05:00,356	INFO cli.py:25 -- Creating JobSubmissionClient at address: http://127.0.0.1:8265
    2021-12-01 23:05:00,371	INFO cli.py:127 -- Job status for 'raysubmit_RXhvSyEPbxhcXtm6': PENDING.
    2021-12-01 23:05:00,371	INFO cli.py:129 -- Job has not started yet, likely waiting for the runtime_env to be set up.

    ❯ ray job status raysubmit_RXhvSyEPbxhcXtm6
    2021-12-01 23:05:37,751	INFO cli.py:25 -- Creating JobSubmissionClient at address: http://127.0.0.1:8265
    2021-12-01 23:05:37,764	INFO cli.py:127 -- Job status for 'raysubmit_RXhvSyEPbxhcXtm6': SUCCEEDED.
    2021-12-01 23:05:37,764	INFO cli.py:129 -- Job finished successfully.

    ❯ ray job logs raysubmit_RXhvSyEPbxhcXtm6
    2021-12-01 23:05:59,026	INFO cli.py:25 -- Creating JobSubmissionClient at address: http://127.0.0.1:8265
    2021-12-01 23:05:23,037	INFO worker.py:851 -- Connecting to existing Ray cluster at address: 127.0.0.1:6379
    (pid=runtime_env) 2021-12-01 23:05:23,212	WARNING conda.py:54 -- Injecting /Users/jiaodong/Workspace/ray/python to environment /tmp/ray/session_2021-12-01_23-04-44_771129_7693/runtime_resources/conda/99305e1352b2dcc9d5f38c2721c7c1f1cc0551d5 because _inject_current_ray flag is on.
    (pid=runtime_env) 2021-12-01 23:05:23,212	INFO conda.py:328 -- Finished setting up runtime environment at /tmp/ray/session_2021-12-01_23-04-44_771129_7693/runtime_resources/conda/99305e1352b2dcc9d5f38c2721c7c1f1cc0551d5
    (pid=runtime_env) 2021-12-01 23:05:23,213	INFO working_dir.py:85 -- Setup working dir for gcs://_ray_pkg_bbcc8ca7e83b4dc0.zip
    1
    2
    3
    4
    5
    2.26.0



Ray Job SDK
------------

Ray Job SDK is the recommended way to submit Jobs programmatically.

We can import and initialize the Job submission client by providing a valid Ray cluster head node address where the port is same as the port used by Ray dashboard. We're using your local Ray cluster as an example but it works the same for remote Ray cluster addresses.

.. code-block:: python

    from ray.dashboard.modules.job.sdk import JobSubmissionClient

    client = JobSubmissionClient("http://127.0.0.1:8265")

Then we can submit our application to the Ray cluster via the Job SDK.

.. code-block:: python

    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint="python script.py",
        # Working dir
        runtime_env={
            "working_dir": "./",
            "pip": ["requests==2.26.0"]
        }
    )

.. tip::

    By default Ray Job server will generate a new ID as return value, but you can also generate your unique job_id first and pass it into :code:`submit_job`. In this case, the Job will be executed with your given id, and will throw error if same job_id is submitted more than once for the same Ray cluster.

Now we can have a simple polling loop that checks the job status until it reaches a terminal state (namely, ``JobStatus.SUCCEEDED``, ``JobStatus.STOPPED``, or ``JobStatus.FAILED``), and gets the logs at the end. We expect to see actor printed numbers as well as the correct version of the :code:`requests` module specified in the ``runtime_env``.

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

Expected output should be:

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

    We can also use other remote URIs for runtime env, such as S3 or Git. See "Remote URIs" section of :ref:`Runtime Environments<runtime-environments>` for details.

A submitted Job can be stopped by the user before it finishes executing.

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


REST API
------------

Under the hood, both the Job Client and the CLI make HTTP calls to the job server running on the ray head node. Therefore the user can also directly send requests to corresponding endpoints via HTTP if needed.

| **Submit Job**

.. code-block:: python

    resp = requests.post(
        "http://127.0.0.1:8265/api/jobs/",
        json={
            "entrypoint": "echo hello",
            "runtime_env": {},
            "job_id": None,
            "metadata": {"job_submission_id": "123"}
        }
    )
    rst = json.loads(resp.text)
    job_id = rst["job_id"]

**Query and poll for Job status**

.. code-block:: python

    start = time.time()
    while time.time() - start <= 10:
        resp = requests.get(
            "http://127.0.0.1:8265/api/jobs/<job_id>"
        )
        rst = json.loads(resp.text)
        status = rst["job_status"]
        print(f"status: {status}")
        if status in {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}:
            break
        time.sleep(1)

**Query for logs**

.. code-block:: python

    resp = requests.get(
        "http://127.0.0.1:8265/api/jobs/<job_id>/logs"
    )
    rst = json.loads(resp.text)
    logs = rst["logs"]


Job Submission Architecture
----------------------------

The following diagram shows the underlying structure and steps for each Job submission.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/job/job_submission_arch_v2.png
