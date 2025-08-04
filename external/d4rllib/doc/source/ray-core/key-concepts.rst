.. _core-key-concepts:

Key Concepts
============

This section overviews Ray's key concepts. These primitives work together to enable Ray to flexibly support a broad range of distributed applications.

.. _task-key-concept:

Tasks
-----

Ray enables arbitrary functions to execute asynchronously on separate worker processes. These asynchronous Ray functions are called tasks. Ray enables tasks to specify their resource requirements in terms of CPUs, GPUs, and custom resources. The cluster scheduler uses these resource requests to distribute tasks across the cluster for parallelized execution.

See the :ref:`User Guide for Tasks <ray-remote-functions>`.

.. _actor-key-concept:

Actors
------

Actors extend the Ray API from functions (tasks) to classes. An actor is essentially a stateful worker (or a service). When you instantiate a new actor, Ray creates a new worker and schedules methods of the actor on that specific worker. The methods can access and mutate the state of that worker. Like tasks, actors support CPU, GPU, and custom resource requirements.

See the :ref:`User Guide for Actors <actor-guide>`.

Objects
-------

Tasks and actors create objects and compute on objects. You can refer to these objects as *remote objects* because Ray stores them anywhere in a Ray cluster, and you use *object refs* to refer to them. Ray caches remote objects in its distributed `shared-memory <https://en.wikipedia.org/wiki/Shared_memory>`__ *object store* and creates one object store per node in the cluster. In the cluster setting, a remote object can live on one or many nodes, independent of who holds the object ref.

See the :ref:`User Guide for Objects <objects-in-ray>`.

Placement Groups
----------------

Placement groups allow users to atomically reserve groups of resources across multiple nodes. You can use them to schedule Ray tasks and actors packed as close as possible for locality (PACK), or spread apart (SPREAD). A common use case is gang-scheduling actors or tasks.

See the :ref:`User Guide for Placement Groups <ray-placement-group-doc-ref>`.

Environment Dependencies
------------------------

When Ray executes tasks and actors on remote machines, their environment dependencies, such as Python packages, local files, and environment variables, must be available on the remote machines. To address this problem, you can
1. Prepare your dependencies on the cluster in advance using the Ray :ref:`Cluster Launcher <vm-cluster-quick-start>`
2. Use Ray's :ref:`runtime environments <runtime-environments>` to install them on the fly.

See the :ref:`User Guide for Environment Dependencies <handling_dependencies>`.
