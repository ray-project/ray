.. _observability-getting-started-program:

Getting Started (API, CLI, SDK)
===============================

Monitoring and debugging capabilities in Ray are available through an API, CLI, or SDK.


Accessing Ray States
--------------------
Ray 2.0 and later versions support CLI and Python APIs for querying the state of resources (e.g., actor, task, object, etc.)

For example, the following command summarizes the task state of the cluster:

.. code-block:: bash

    ray summary tasks

.. code-block:: text

    ======== Tasks Summary: 2022-07-22 08:54:38.332537 ========
    Stats:
    ------------------------------------
    total_actor_scheduled: 2
    total_actor_tasks: 0
    total_tasks: 2


    Table (group by func_name):
    ------------------------------------
        FUNC_OR_CLASS_NAME        STATE_COUNTS    TYPE
    0   task_running_300_seconds  RUNNING: 2      NORMAL_TASK
    1   Actor.__init__            FINISHED: 2     ACTOR_CREATION_TASK

The following command lists all the actors from the cluster:

.. code-block:: bash

    ray list actors

.. code-block:: text

    ======== List: 2022-07-23 21:29:39.323925 ========
    Stats:
    ------------------------------
    Total: 2

    Table:
    ------------------------------
        ACTOR_ID                          CLASS_NAME    NAME      PID  STATE
    0  31405554844820381c2f0f8501000000  Actor                 96956  ALIVE
    1  f36758a9f8871a9ca993b1d201000000  Actor                 96955  ALIVE

See :ref:`Ray State API <state-api-overview-ref>` for more details.
