API Comparisons
===============

Comparison between Ray Core APIs and Workflows
----------------------------------------------
Workflows is built on top of Ray, and offers a mostly consistent subset of its API while providing durability. This section highlights some of the differences:

`func.remote` vs `func.step`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
With Ray tasks, ``func.remote`` will submit a remote task to run. In Ray workflows, ``func.step`` is used to create a ``Workflow`` object. Execution of the workflow is deferred until ``.run(workflow_id="id")`` or ``.run_async(workflow_id="id")`` is called on the ``Workflow``. Specifying the workflow id allows for resuming of the workflow by its id in case of cluster failure.


`Actor.remote` vs `Actor.get_or_create`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With Ray actors, ``Actor.remote`` will submit an actor creation task and create an actor process in the cluster. In Ray workflows, virtual actors are created by ``Actor.get_or_create``. The actor state is tracked as a dynamic workflow (durably logged) instead of in a running process. This means that the actor uses no resources when inactive, and can be used even after cluster restarts.

`actor.func.remote` vs `actor.func.run`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With Ray actors, ``actor.func.remote`` will submit a remote task to run which is similar as ``func.remote``. On the other hand ``actor.func.run`` on a virtual actor will read the actor state from storage, execute a step, and then write the new state back to storage. If the ``actor.func`` is decorated with ``workflow.virtual_actor.readonly``, its result will not be logged.

Other Workflow Engines
----------------------

Note: these comparisons are inspired by the `Serverless workflows comparisons repo <https://github.com/serverlessworkflow/specification/tree/main/comparisons>`__.

Argo API Comparison
~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://github.com/serverlessworkflow/specification/blob/main/comparisons/comparison-argo.md>`__.

Conditionals
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/conditionals_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/conditionals_workflow.py
   :caption: Workflow version:
   :language: python

DAG
^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/dag_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/dag_workflow.py
   :caption: Workflow version:
   :language: python

Multi-step Workflow
^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/multi_step_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/multi_step_workflow.py
   :caption: Workflow version:
   :language: python

Exit Handler
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/exit_handler_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/exit_handler_workflow.py
   :caption: Workflow version:
   :language: python

Loops
^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/loops_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/loops_workflow.py
   :caption: Workflow version:
   :language: python

Recursion
^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/recursion_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/recursion_workflow.py
   :caption: Workflow version:
   :language: python

Retries
^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/retry_argo.yaml
   :caption: Argo version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/argo/retry_workflow.py
   :caption: Workflow version:
   :language: python

Metaflow API Comparison
~~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://docs.metaflow.org/metaflow/basics#foreach>`__.

Foreach
^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/metaflow/foreach_metaflow.py.txt
   :caption: Metaflow version:
   :language: python

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/metaflow/foreach_workflow.py
   :caption: Workflow version:
   :language: python

Cadence API Comparison
~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://github.com/serverlessworkflow/specification/blob/main/comparisons/comparison-cadence.md>`__.

Sub Workflows
^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/cadence/sub_workflow_cadence.java
   :caption: Cadence version:
   :language: java

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/cadence/sub_workflow_workflow.py
   :caption: Workflow version:
   :language: python

File Processing
^^^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/cadence/file_processing_cadence.java
   :caption: Cadence version:
   :language: java

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/cadence/file_processing_workflow.py
   :caption: Workflow version:
   :language: python

Trip Booking
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/cadence/trip_booking_cadence.java
   :caption: Cadence version:
   :language: java

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/cadence/trip_booking_workflow.py
   :caption: Workflow version:
   :language: python

Google Cloud Workflows API Comparison
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://github.com/serverlessworkflow/specification/blob/main/comparisons/comparison-google-cloud-workflows.md>`__.

Data Conditional
^^^^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/google_cloud_workflows/data_cond_google.yaml
   :caption: Google Cloud version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/google_cloud_workflows/data_cond_workflow.py
   :caption: Workflow version:
   :language: python

Concat Array
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/google_cloud_workflows/concat_array_google.yaml
   :caption: Google Cloud version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/google_cloud_workflows/concat_array_workflow.py
   :caption: Workflow version:
   :language: python

Sub Workflows
^^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/google_cloud_workflows/sub_workflows_google.yaml
   :caption: Google Cloud version:
   :language: yaml

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/google_cloud_workflows/sub_workflows_workflow.py
   :caption: Workflow version:
   :language: python

Prefect API Comparison
~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://docs.prefect.io/core/advanced_tutorials/task-looping.html>`__.

Looping
^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/prefect/compute_fib_prefect.py.txt
   :caption: Prefect version:
   :language: python

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/prefect/compute_fib_workflow.py
   :caption: Workflow version:
   :language: python

AirFlow API Comparison
~~~~~~~~~~~~~~~~~~~~~~

The original source of these comparisons can be `found here <https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html>`__.

ETL Workflow
^^^^^^^^^^^^

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/airflow/etl_airflow.py.txt
   :caption: AirFlow version:
   :language: python

.. literalinclude:: ../../../python/ray/experimental/workflow/examples/comparisons/airflow/etl_workflow.py
   :caption: Workflow version:
   :language: python
