.. warning::
    This page is under construction!

.. _ray-job-rest-api:

REST API
^^^^^^^^

Under the hood, both the Python SDK and the CLI make HTTP calls to the job server running on the Ray head node. You can also directly send requests to the corresponding endpoints via HTTP if needed:

**Submit Job**

.. code-block:: python

    import requests
    import json
    import time

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
        status = rst["status"]
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

**List all jobs**

.. code-block:: python

    resp = requests.get(
        "http://127.0.0.1:8265/api/jobs/"
    )
    print(resp.json())
    # {"job_id": {"metadata": ..., "status": ..., "message": ...}, ...}
