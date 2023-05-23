.. _observability-programmatic:

Monitoring with the CLI or SDK
==============================

Monitoring and debugging capabilities in Ray are available through a CLI or SDK.


Monitoring Cluster State and Resource Demands
---------------------------------------------
You can monitor cluster usage and auto-scaling status by running (on the head node) a CLI command ``ray status``. It displays

- **Cluster State**: Nodes that are up and running. Addresses of running nodes. Information about pending nodes and failed nodes.
- **Autoscaling Status**: The number of nodes that are autoscaling up and down.
- **Cluster Usage**: The resource usage of the cluster. E.g., requested CPUs from all Ray tasks and actors. Number of GPUs that are used.

Here's an example output.

.. code-block:: shell

   $ ray status
   ======== Autoscaler status: 2021-10-12 13:10:21.035674 ========
   Node status
   ---------------------------------------------------------------
   Healthy:
    1 ray.head.default
    2 ray.worker.cpu
   Pending:
    (no pending nodes)
   Recent failures:
    (no failures)

   Resources
   ---------------------------------------------------------------
   Usage:
    0.0/10.0 CPU
    0.00/70.437 GiB memory
    0.00/10.306 GiB object_store_memory

   Demands:
    (no resource demands)

.. _state-api-overview-ref:

Monitoring Ray States
=====================

.. tip:: We'd love to hear your feedback on using Ray state APIs - `feedback form <https://forms.gle/gh77mwjEskjhN8G46>`_!

Ray state APIs allow users to conveniently access the current state (snapshot) of Ray through CLI or Python SDK (developer APIs).

.. note::

    This feature requires a full installation of Ray using ``pip install "ray[default]"``. This feature also requires the dashboard component to be available. The dashboard component needs to be included when starting the Ray cluster, which is the default behavior for ``ray start`` and ``ray.init()``. For more in-depth debugging, check the dashboard log at ``<RAY_LOG_DIR>/dashboard.log``, which is usually ``/tmp/ray/session_latest/logs/dashboard.log``.

.. note::

    State API CLI commands are :ref:`stable <api-stability-stable>`, while python SDKs are :ref:`DeveloperAPI <developer-api-def>`. CLI usage is recommended over Python SDKs.

Run any workload. In this example, you will use the following script that runs 2 tasks and creates 2 actors.

.. testcode::
    :hide:

    import ray

    ray.shutdown()

.. testcode::

    import ray
    import time

    ray.init(num_cpus=4)

    @ray.remote
    def task_running_300_seconds():
        time.sleep(300)

    @ray.remote
    class Actor:
        def __init__(self):
            pass

    # Create 2 tasks
    tasks = [task_running_300_seconds.remote() for _ in range(2)]

    # Create 2 actors
    actors = [Actor.remote() for _ in range(2)]

.. testcode::
    :hide:

    # Wait for the tasks to be submitted.
    time.sleep(2)

Now, let's see the summarized states of tasks. If it doesn't return the output immediately, retry the command.

.. tabs::

    .. group-tab:: CLI (Recommended)

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

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import summarize_tasks
            print(summarize_tasks())

        .. testoutput::

            {'cluster': {'summary': {'task_running_300_seconds': {'func_or_class_name': 'task_running_300_seconds', 'type': 'NORMAL_TASK', 'state_counts': {'RUNNING': 2}}, 'Actor.__init__': {'func_or_class_name': 'Actor.__init__', 'type': 'ACTOR_CREATION_TASK', 'state_counts': {'FINISHED': 2}}}, 'total_tasks': 2, 'total_actor_tasks': 0, 'total_actor_scheduled': 2, 'summary_by': 'func_name'}}

Let's list all actors.

.. tabs::

    .. group-tab:: CLI (Recommended)

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

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import list_actors
            print(list_actors())

        .. testoutput::
            :options: +ELLIPSIS

            [ActorState(actor_id='...', class_name='Actor', state='ALIVE', job_id='01000000', name='', node_id='...', pid=..., ray_namespace='...', serialized_runtime_env=None, required_resources=None, death_cause=None, is_detached=None, placement_group_id=None, repr_name=None), ActorState(actor_id='...', class_name='Actor', state='ALIVE', job_id='01000000', name='', node_id='...', pid=..., ray_namespace='...', serialized_runtime_env=None, required_resources=None, death_cause=None, is_detached=None, placement_group_id=None, repr_name=None)]


You can get the state of a single task using the get API.

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            # In this case, 31405554844820381c2f0f8501000000
            ray get actors <ACTOR_ID>

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

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::
            :skipif: True

            from ray.util.state import get_actor
            # In this case, 31405554844820381c2f0f8501000000
            print(get_actor(id=<ACTOR_ID>))

You can also access logs through ``ray logs`` API.

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray list actors
            # In this case, ACTOR_ID is 31405554844820381c2f0f8501000000
            ray logs actor --id <ACTOR_ID>

        .. code-block:: text

            --- Log has been truncated to last 1000 lines. Use `--tail` flag to toggle. ---

            :actor_name:Actor
            Actor created

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::
            :skipif: True

            from ray.util.state import get_log

            # In this case, ACTOR_ID is 31405554844820381c2f0f8501000000
            for line in get_log(actor_id=<ACTOR_ID>):
                print(line)

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

E.g., Summarize all actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray summary actors

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import summarize_actors
            print(summarize_actors())

        .. testoutput::

            {'cluster': {'summary': {'Actor': {'class_name': 'Actor', 'state_counts': {'ALIVE': 2}}}, 'total_actors': 2, 'summary_by': 'class'}}

E.g., Summarize all tasks
~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray summary tasks

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import summarize_tasks
            print(summarize_tasks())

        .. testoutput::

            {'cluster': {'summary': {'task_running_300_seconds': {'func_or_class_name': 'task_running_300_seconds', 'type': 'NORMAL_TASK', 'state_counts': {'RUNNING': 2}}, 'Actor.__init__': {'func_or_class_name': 'Actor.__init__', 'type': 'ACTOR_CREATION_TASK', 'state_counts': {'FINISHED': 2}}}, 'total_tasks': 2, 'total_actor_tasks': 0, 'total_actor_scheduled': 2, 'summary_by': 'func_name'}}

E.g., Summarize all objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

    By default, objects are summarized by callsite. However, callsite is not recorded by Ray by default.
    To get callsite info, set env variable `RAY_record_ref_creation_sites=1` when starting the ray cluster
    RAY_record_ref_creation_sites=1 ray start --head

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray summary objects

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import summarize_objects
            print(summarize_objects())

        .. testoutput::

            {'cluster': {'summary': {'disabled': {'total_objects': 6, 'total_size_mb': 0.0, 'total_num_workers': 3, 'total_num_nodes': 1, 'task_state_counts': {'SUBMITTED_TO_WORKER': 2, 'FINISHED': 2, 'NIL': 2}, 'ref_type_counts': {'LOCAL_REFERENCE': 2, 'ACTOR_HANDLE': 4}}}, 'total_objects': 6, 'total_size_mb': 0.0, 'callsite_enabled': False, 'summary_by': 'callsite'}}

List
----

Get a list of resources, possible resources include:

- :ref:`Actors <actor-guide>`, e.g., actor id, state, pid, death_cause. (:class:`output schema <ray.util.state.common.ActorState>`)
- :ref:`Tasks <ray-remote-functions>`, e.g., name, scheduling state, type, runtime env info (:class:`output schema <ray.util.state.common.TaskState>`)
- :ref:`Objects <objects-in-ray>`, e.g., object id, callsites, reference types. (:class:`output schema <ray.util.state.common.ObjectState>`)
- :ref:`Jobs <jobs-overview>`, e.g., start/end time, entrypoint, status. (:class:`output schema <ray.util.state.common.JobState>`)
- :ref:`Placement Groups <ray-placement-group-doc-ref>`, e.g., name, bundles, stats. (:class:`output schema <ray.util.state.common.PlacementGroupState>`)
- Nodes (Ray worker nodes), e.g., node id, node ip, node state. (:class:`output schema <ray.util.state.common.NodeState>`)
- Workers (Ray worker processes), e.g., worker id, type, exit type and details. (:class:`output schema <ray.util.state.common.WorkerState>`)
- :ref:`Runtime environments <runtime-environments>`, e.g., runtime envs, creation time, nodes (:class:`output schema <ray.util.state.common.RuntimeEnvState>`)

E.g., List all nodes
~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray list nodes

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import list_nodes
            list_nodes()

E.g., List all placement groups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray list placement-groups

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import list_placement_groups
            list_placement_groups()


E.g., List local referenced objects created by a process
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tip:: You can list resources with one or multiple filters: using `--filter` or `-f`

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray list objects -f pid=<PID> -f reference_type=LOCAL_REFERENCE

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import list_objects
            list_objects(filters=[("pid", "=", 1234), ("reference_type", "=", "LOCAL_REFERENCE")])

E.g., List alive actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray list actors -f state=ALIVE

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import list_actors
            list_actors(filters=[("state", "=", "ALIVE")])

E.g., List running tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray list tasks -f state=RUNNING

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import list_tasks
            list_tasks(filters=[("state", "=", "RUNNING")])

E.g., List non-running tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray list tasks -f state!=RUNNING

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import list_tasks
            list_tasks(filters=[("state", "!=", "RUNNING")])

E.g., List running tasks that have a name func
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray list tasks -f state=RUNNING -f name="task_running_300_seconds()"

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import list_tasks
            list_tasks(filters=[("state", "=", "RUNNING"), ("name", "=", "task_running_300_seconds()")])

E.g., List tasks with more details
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tip:: When ``--detail`` is specified, the API can query more data sources to obtain state information in details.

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray list tasks --detail

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::

            from ray.util.state import list_tasks
            list_tasks(detail=True)

Get
---

E.g., Get a task info
~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray get tasks <TASK_ID>

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::
            :skipif: True

            from ray.util.state import get_task
            get_task(id=<TASK_ID>)

E.g., Get a node info
~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray get nodes <NODE_ID>

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::
            :skipif: True

            from ray.util.state import get_node
            get_node(id=<NODE_ID>)

Logs
----

.. _state-api-log-doc:

State API also allows you to conveniently access ray logs. Note that you cannot access the logs from a dead node.
By default, the API prints log from a head node.

E.g., Get all retrievable log file names from a head node in a cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray logs cluster

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::
            :skipif: True

            # You could get the node id / node ip from `ray list nodes`
            from ray.util.state import list_logs
            # `ray logs` by default print logs from a head node.
            # So in order to list the same logs, you should provide the head node id.
            # You could get the node id / node ip from `ray list nodes`
            list_logs(node_id=<HEAD_NODE_ID>)

E.g., Get a particular log file from a node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            # You could get the node id / node ip from `ray list nodes`
            ray logs cluster gcs_server.out --node-id <NODE_ID>
            # `ray logs cluster` is alias to `ray logs` when querying with globs.
            ray logs gcs_server.out --node-id <NODE_ID>

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::
            :skipif: True

            from ray.util.state import get_log

            # Node IP could be retrieved from list_nodes() or ray.nodes()
            for line in get_log(filename="gcs_server.out", node_id=<NODE_ID>):
                print(line)

E.g., Stream a log file from a node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            # You could get the node id / node ip from `ray list nodes`
            ray logs raylet.out --node-ip <NODE_IP> --follow
            # Or,
            ray logs cluster raylet.out --node-ip <NODE_IP> --follow


    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::
            :skipif: True

            from ray.util.state import get_log

            # Node IP could be retrieved from list_nodes() or ray.nodes()
            # The loop will block with `follow=True`
            for line in get_log(filename="raylet.out", node_ip=<NODE_IP>, follow=True):
                print(line)

E.g., Stream log from an actor with actor id
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray logs actor --id=<ACTOR_ID> --follow

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::
            :skipif: True

            from ray.util.state import get_log

            # You could get the actor's ID from the output of `ray list actors`.
            # The loop will block with `follow=True`
            for line in get_log(actor_id=<ACTOR_ID>, follow=True):
                print(line)

E.g., Stream log from a pid
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

    .. group-tab:: CLI (Recommended)

        .. code-block:: bash

            ray logs worker --pid=<PID> --follow

    .. group-tab:: Python SDK (Internal Developer API)

        .. testcode::
            :skipif: True

            from ray.util.state import get_log

            # Node IP could be retrieved from list_nodes() or ray.nodes()
            # You could get the pid of the worker running the actor easily when output
            # of worker being directed to the driver (default)
            # The loop will block with `follow=True`
            for line in get_log(pid=<PID>, node_ip=<NODE_IP>, follow=True):
                print(line)

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

- For the CLI Reference, see :ref:`State CLI Refernece <state-api-cli-ref>`.
- For the SDK Reference, see :ref:`State API Reference <state-api-ref>`.
- For the Log CLI Reference, see :ref:`Log CLI Reference <ray-logs-api-cli-ref>`.