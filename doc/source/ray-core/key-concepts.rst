.. _core-key-concepts:

Key Concepts
============

This section overviews Ray's key concepts. These primitives work together to enable Ray to flexibly support a broad range of distributed applications.

Tasks
-----

Ray enables arbitrary functions to be executed asynchronously on separate Python workers. These asynchronous Ray functions are called "tasks". Ray enables tasks to specify their resource requirements in terms of CPUs, GPUs, and custom resources. These resource requests are used by the cluster scheduler to distribute tasks across the cluster for parallelized execution.

See the :ref:`User Guide for Tasks <ray-remote-functions>`.

Actors
------

Actors extend the Ray API from functions (tasks) to classes. An actor is essentially a stateful worker (or a service). When a new actor is instantiated, a new worker is created, and methods of the actor are scheduled on that specific worker and can access and mutate the state of that worker. Like tasks, actors support CPU, GPU, and custom resource requirements.

See the :ref:`User Guide for Actors <actor-guide>`.

Objects
-------

In Ray, tasks and actors create and compute on objects. We refer to these objects as *remote objects* because they can be stored anywhere in a Ray cluster, and we use *object refs* to refer to them. Remote objects are cached in Ray's distributed `shared-memory <https://en.wikipedia.org/wiki/Shared_memory>`__ *object store*, and there is one object store per node in the cluster. In the cluster setting, a remote object can live on one or many nodes, independent of who holds the object ref(s).

See the :ref:`User Guide for Objects <objects-in-ray>`.

Placement Groups
----------------

Placement groups allow users to atomically reserve groups of resources across multiple nodes (i.e., gang scheduling). They can be then used to schedule Ray tasks and actors packed as close as possible for locality (PACK), or spread apart (SPREAD). Placement groups are generally used for gang-scheduling actors, but also support tasks.

See the :ref:`User Guide for Placement Groups <ray-placement-group-doc-ref>`.

Environment Dependencies
------------------------

When Ray executes tasks and actors on remote machines, their environment dependencies (e.g., Python packages, local files, environment variables) must be available for the code to run. To address this problem, you can (1) prepare your dependencies on the cluster in advance using the Ray :ref:`Cluster Launcher <vm-cluster-quick-start>`, or (2) use Ray's :ref:`runtime environments <runtime-environments>` to install them on the fly.

See the :ref:`User Guide for Environment Dependencies <handling_dependencies>`.
