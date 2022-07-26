.. _state-api-overview-ref:

Monitoring Ray States
=====================

.. tip:: We'd love to hear your feedback on using Ray state APIs - `feedback form <https://forms.gle/gh77mwjEskjhN8G46>`_!

Ray state APIs allow users to conveniently access the current state (snapshot) of Ray through CLI or Python SDK.

.. note:: 

    APIs are at the alpha stage and under active development. APIs are subject to change and not stable across versions. This feature requires a full installation of Ray using ``pip install "ray[default]"``.

Getting Started
---------------

Run any workload. In this example, you will use the following script that runs 2 tasks and creates 2 actors.

.. code-block:: python

    import ray
    import time

    ray.init(num_cpus=4)

    @ray.remote
    def task_running_300_seconds():
        print("Start!")
        time.sleep(300)
    
    @ray.remote
    class Actor:
        def __init__(self):
            print("Actor created")
    
    # Create 2 tasks
    tasks = [task_running_300_seconds.remote() for _ in range(2)]

    # Create 2 actors
    actors = [Actor.remote() for _ in range(2)]

    ray.get(tasks)

Now, let's see the summarized states of tasks. If it doesn't return the output immediately, retry the command.

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

Let's list all actors.

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

You can get the state of a single task using the get API. 

.. code-block:: bash

    ray get actors <ACTOR_ID> # In this case, 31405554844820381c2f0f8501000000

.. code-block:: text

    ---
    actor_id: 31405554844820381c2f0f8501000000
    class_name: Actor
    death_cause: null
    is_detached: false
    name: ''
    pid: 96956
    resource_mapping: []
    serialized_runtime_env: '{}'
    state: ALIVE

You can also access logs through ``ray logs`` API.

.. code-block:: bash

    ray list actors
    ray logs --actor-id <ACTOR_ID>

.. code-block:: text

    --- Log has been truncated to last 1000 lines. Use `--tail` flag to toggle. ---

    :actor_name:Actor
    Actor created


Key Concepts
------------
Ray state APIs allow you to access **states** of **resources** through **summary**, **list**, and **get** APIs. It also supports **logs** API to access logs.

- **states**: The state of the cluster of corresponding resources. States consist of immutable metadata (e.g., actor's name) and mutable states (e.g., actor's scheduling state or pid).
- **resources**: Resources created by Ray. E.g., actors, tasks, objects, placement groups, and etc. 
- **summary**: API to return the summarized view of resources.
- **list**: API to return every individual entity of resources.
- **get**: API to return a single entity of resources in detail.
- **logs**: API to access the log of actors, tasks, workers, or system log files.

Summary 
-------
Return the summarized information of the given Ray resource (objects, actors, tasks).
It is recommended to start monitoring states through summary APIs first. When you find anomalies
(e.g., actors running for a long time, tasks that are not scheduled for a long time),
you can use ``list`` or ``get`` APIs to get more details for an individual abnormal resource.

E.g., Summarize all actors (e.g., number of alive actors, different actor classes, etc)

.. code-block:: bash

    ray summary actors

E.g., Summarize all tasks (e.g., task count in different states, type of different tasks, etc)  

.. code-block:: bash

    ray summary tasks

E.g., Summarize all objects (e.g., the total number of objects, size of all objects, etc) 

.. code-block:: bash

    # To get callsite info, set env variable `RAY_record_ref_creation_sites=1` when starting the ray cluster
    # RAY_record_ref_creation_sites=1 ray start --head
    ray summary objects 

List
----

Get a list of resources, possible resources include: 

- :ref:`Actors <actor-guide>`
- :ref:`Tasks <ray-remote-functions>`
- :ref:`Objects <objects-in-ray>`
- :ref:`Jobs <jobs-overview>`
- :ref:`Placement Groups <ray-placement-group-doc-ref>`
- Nodes (Ray worker nodes)
- Workers (Ray worker processes)
- :ref:`Runtime environments <runtime-environments>`

E.g., List all nodes

.. code-block:: bash

    ray list nodes 

E.g., List all placement groups

.. code-block:: bash

    ray list placement-groups

You can list resources with one or multiple filters.
 
E.g., List local referenced objects created by a process

.. code-block:: bash

    ray list objects -f pid=12345 -f reference_type=LOCAL_REFERENCE

E.g., List alive actors

.. code-block:: bash

    ray list actors -f state=ALIVE

E.g., List running tasks

.. code-block:: bash

    ray list tasks -f scheduling_state=RUNNING

E.g., List non-running tasks

.. code-block:: bash

    ray list tasks -f shceduling_state!=RUNNING

E.g., List running tasks that have a name func

.. code-block:: bash

    ray list tasks -f scheduling_state=RUNNING -f name=func

E.g., List tasks with more details. When ``--detail`` is specified, the API can query more data sources to obtain state information in details.

.. code-block:: bash

    ray list tasks --detail

Get
---

E.g., Get a task info

.. code-block:: bash

    ray get tasks <worker_id> 

E.g., Get a node info

.. code-block:: bash

    ray get nodes <node_id> 


Logs
----

State API also allows you to conveniently access ray logs. Note that you cannot access the logs from a dead node.
By default, the API prints log from a head node.

E.g., Get all retrievable log file names

.. code-block:: bash

    ray logs 

E.g., Get a particular log file from a node

.. code-block:: bash

    # You could get the node id / node ip from `ray list nodes` 
    ray logs gcs_server.out --node-id <XYZ> 

E.g., Stream a log file from a node

.. code-block:: bash

    ray logs -f raylet.out --node-ip 172.31.47.143

E.g., Stream actor log with actor id 

.. code-block:: bash

    # You could use ray list actors to get the actor ids
    ray logs --actor-id=<XXX>

E.g., Stream log from a pid 

.. code-block:: bash

    ray logs --pid=<XXX> --follow

Failure Semantics
-----------------

The state APIs don't guarantee to return a consistent/complete snapshot of the cluster all the time. By default,
all Python SDKs raise an exception when there's a missing output from the API. And CLI returns a partial result
and provides warning messages. Here are cases where there can be missing output from the API.

Query Failures
~~~~~~~~~~~~~~

State APIs query "data sources" (e.g., GCS, raylets, etc.) to obtain and build the snapshot of the cluster.
However, data sources are sometimes unavailable (e.g., the source is down or overloaded). In this case, APIs
will return a partial (incomplete) snapshot of the cluster, and users are informed that the output is incomplete through a warning message.
All warnings are printed through Python's ``warnings`` library, and they can be suppressed.

Data Truncation
~~~~~~~~~~~~~~~

When the returned number of entities (number of rows) is too large (> 100K), state APIs truncate the output data to ensure system stability
(when this happens, there's no way to choose truncated data). When truncation happens it will be informed through Python's
``warnings`` module.

Garbage Collected Resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Depending on the lifecycle of the resources, some "finished" resources are not accessible
through the APIs because they are already garbage collected.
**It is recommended not to rely on this API to obtain correct information on finished resources**.
For example, Ray periodically garbage collects DEAD state actor data to reduce memory usage.
Or it cleans up the FINISHED state of tasks when its lineage goes out of scope.

API Reference
-------------

See :ref:`State API Reference <state-api-ref>`.