API Comparisons
===============

Comparison between Ray Core APIs and Workflows
----------------------------------------------
Ray Workflows is built on top of Ray, and offers a mostly consistent subset of its API while providing durability. This section highlights some of the differences:

``func.remote`` vs ``func.bind``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
With Ray tasks, ``func.remote`` will submit a remote task to run eagerly; ``func.bind`` will generate
a node in a DAG, it will not be executed until the DAG is been executed.

Under the context of Ray Workflow, the execution of the DAG is deferred until ``workflow.run(dag, workflow_id=...)`` or ``workflow.run_async(dag, workflow_id=...)`` is called on the DAG.
Specifying the workflow id allows for resuming of the workflow by its id in case of cluster failure.

Other Workflow Engines
----------------------

Note: these comparisons are inspired by the `Serverless workflows comparisons repo <https://github.com/serverlessworkflow/specification/tree/main/comparisons>`__.

Argo API Comparison
~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://github.com/serverlessworkflow/specification/blob/main/comparisons/comparison-argo.md>`__.

Conditionals
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/conditionals_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/conditionals_workflow.py
   :caption: Workflow version:
   :language: python

DAG
^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/dag_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/dag_workflow.py
   :caption: Workflow version:
   :language: python

Multi-step Workflow
^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/multi_step_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/multi_step_workflow.py
   :caption: Workflow version:
   :language: python

Exit Handler
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/exit_handler_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/exit_handler_workflow.py
   :caption: Workflow version:
   :language: python

Loops
^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/loops_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/loops_workflow.py
   :caption: Workflow version:
   :language: python

Recursion
^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/recursion_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/recursion_workflow.py
   :caption: Workflow version:
   :language: python

Retries
^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/retry_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/argo/retry_workflow.py
   :caption: Workflow version:
   :language: python

Metaflow API Comparison
~~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://docs.metaflow.org/metaflow/basics#foreach>`__.

Foreach
^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/metaflow/foreach_metaflow.py.txt
   :caption: Metaflow version:
   :language: python

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/metaflow/foreach_workflow.py
   :caption: Workflow version:
   :language: python

Cadence API Comparison
~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://github.com/serverlessworkflow/specification/blob/main/comparisons/comparison-cadence.md>`__.

Sub Workflows
^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/cadence/sub_workflow_cadence.java
   :caption: Cadence version:
   :language: java

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/cadence/sub_workflow_workflow.py
   :caption: Workflow version:
   :language: python

File Processing
^^^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/cadence/file_processing_cadence.java
   :caption: Cadence version:
   :language: java

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/cadence/file_processing_workflow.py
   :caption: Workflow version:
   :language: python

Trip Booking
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/cadence/trip_booking_cadence.java
   :caption: Cadence version:
   :language: java

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/cadence/trip_booking_workflow.py
   :caption: Workflow version:
   :language: python

Google Cloud Workflows API Comparison
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://github.com/serverlessworkflow/specification/blob/main/comparisons/comparison-google-cloud-workflows.md>`__.

Data Conditional
^^^^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/google_cloud_workflows/data_cond_google.yaml
   :caption: Google Cloud version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/google_cloud_workflows/data_cond_workflow.py
   :caption: Workflow version:
   :language: python

Concat Array
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/google_cloud_workflows/concat_array_google.yaml
   :caption: Google Cloud version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/google_cloud_workflows/concat_array_workflow.py
   :caption: Workflow version:
   :language: python

Sub Workflows
^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/google_cloud_workflows/sub_workflows_google.yaml
   :caption: Google Cloud version:
   :language: yaml

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/google_cloud_workflows/sub_workflows_workflow.py
   :caption: Workflow version:
   :language: python

Prefect API Comparison
~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://docs.prefect.io/core/advanced_tutorials/task-looping.html>`__.

Looping
^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/prefect/compute_fib_prefect.py.txt
   :caption: Prefect version:
   :language: python

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/prefect/compute_fib_workflow.py
   :caption: Workflow version:
   :language: python

AirFlow API Comparison
~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html>`__.

ETL Workflow
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/airflow/etl_airflow.py.txt
   :caption: AirFlow version:
   :language: python

.. literalinclude:: ../../../python/ray/workflow/examples/comparisons/airflow/etl_workflow.py
   :caption: Workflow version:
   :language: python
