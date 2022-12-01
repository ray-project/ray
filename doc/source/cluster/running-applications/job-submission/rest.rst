.. _ray-job-rest-api:

Ray Jobs REST API
^^^^^^^^^^^^^^^^^

Under the hood, both the Python SDK and the CLI make HTTP calls to the job server running on the Ray head node. You can also directly send requests to the corresponding endpoints via HTTP if needed:

Continue on for examples, or jump to the :ref:`OpenAPI specification <ray-job-rest-api-spec>`.

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
    print(job_id)

**Query and poll for Job status**

.. code-block:: python

    start = time.time()
    while time.time() - start <= 10:
        resp = requests.get(
            f"http://127.0.0.1:8265/api/jobs/{job_id}"
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
        f"http://127.0.0.1:8265/api/jobs/{job_id}/logs"
    )
    rst = json.loads(resp.text)
    logs = rst["logs"]
    print(logs)

**List all jobs**

.. code-block:: python

    resp = requests.get(
        "http://127.0.0.1:8265/api/jobs/"
    )
    print(resp.json())
    # {"job_id": {"metadata": ..., "status": ..., "message": ...}, ...}

.. _ray-job-rest-api-spec:

OpenAPI Documentation (Beta)
----------------------------

We provide an OpenAPI specification for the Ray Job API. You can use this to generate client libraries for other languages.

View the `Ray Jobs REST API OpenAPI documentation <api.html>`_.