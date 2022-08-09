.. warning::
    This page is under construction!

.. _jobs-overview-under-construction-cli:

==================
Ray Job Submission
==================

.. note::

    This component is in **beta**.  APIs may change before becoming stable.  This feature requires a full installation of Ray using ``pip install "ray[default]"``.

Ray Job submission is a mechanism to submit locally developed and tested applications to a remote Ray cluster. It simplifies the experience of packaging, deploying, and managing a Ray application.



Jump to the :ref:`API Reference<ray-job-submission-api-ref>`, or continue reading for a quick overview.

Concepts
--------

- **Job**: A Ray application submitted to a Ray cluster for execution. Consists of (1) an entrypoint command and (2) a :ref:`runtime environment<runtime-environments>`, which may contain file and package dependencies.

- **Job Lifecycle**: When a job is submitted, it runs once to completion or failure. Retries or different runs with different parameters should be handled by the submitter. Jobs are bound to the lifetime of a Ray cluster, so if the cluster goes down, all running jobs on that cluster will be terminated.

- **Job Manager**: An entity external to the Ray cluster that manages the lifecycle of a job (scheduling, killing, polling status, getting logs, and persisting inputs/outputs), and potentially also manages the lifecycle of Ray clusters. Can be any third-party framework with these abilities, such as Apache Airflow or Kubernetes Jobs.

Quick Start Example
-------------------

Let's start with a sample job that can be run locally. The following script uses Ray APIs to increment a counter and print its value, and print the version of the ``requests`` module it's using:

.. code-block:: python

    # script.py

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

Put this file in a local directory of your choice, with filename ``script.py``, so your working directory will look like:

.. code-block:: bash

  | your_working_directory ("./")
  | ├── script.py


Next, start a local Ray cluster:

.. code-block:: bash

   ❯ ray start --head
    Local node IP: 127.0.0.1
    INFO services.py:1360 -- View the Ray dashboard at http://127.0.0.1:8265

Note the address and port returned in the terminal---this will be where we submit job requests to, as explained further in the examples below.  If you do not see this, ensure the Ray Dashboard is installed by running :code:`pip install "ray[default]"`.

At this point, the job is ready to be submitted by one of the :ref:`Ray Job APIs<ray-job-apis>`.
Continue on to see examples of running and interacting with this sample job. 

Job Submission Architecture
----------------------------

The following diagram shows the underlying structure and steps for each submitted job.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/job/job_submission_arch_v2.png
