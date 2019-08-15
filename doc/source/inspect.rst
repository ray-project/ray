How-To: Inspect Cluster State
=============================

Applications written on top of Ray will often want to have some information
or diagnostics about the cluster. Some common questions include:

    1. How many nodes are in my autoscaling cluster?
    2. What resources are currently available in my cluster, both used and total?
    3. What are the objects currently in my cluster?

For this, you can use the global state API.

Context: Ray Processes
----------------------

For context, when using Ray, several processes are involved.

- Multiple **worker** processes execute tasks and store results in object
  stores. Each worker is a separate process.
- One **object store** per node stores immutable objects in shared memory and
  allows workers to efficiently share objects on the same node with minimal
  copying and deserialization.
- One **raylet** per node assigns tasks to workers on the same node.
- A **driver** is the Python process that the user controls. For example, if the
  user is running a script or using a Python shell, then the driver is the Python
  process that runs the script or the shell. A driver is similar to a worker in
  that it can submit tasks to its raylet and get objects from the object
  store, but it is different in that the raylet will not assign tasks to
  the driver to be executed.
- A **Redis server** maintains much of the system's state. For example, it keeps
  track of which objects live on which machines and of the task specifications
  (but not data). It can also be queried directly for debugging purposes.

Node Information
----------------

To get information about the current nodes in your cluster, you can use ``ray.nodes()``:

.. autofunction:: ray.nodes
    :noindex:


.. code-block:: python

    >>> import ray
    >>> ray.init()
    >>> ray.nodes()
    [{'ClientID': 'a9e430719685f3862ed7ba411259d4138f8afb1e',
      'IsInsertion': True,
      'NodeManagerAddress': '192.168.19.108',
      'NodeManagerPort': 37428,
      'ObjectManagerPort': 43415,
      'ObjectStoreSocketName': '/tmp/ray/session_2019-07-28_17-03-53_955034_24883/sockets/plasma_store',
      'RayletSocketName': '/tmp/ray/session_2019-07-28_17-03-53_955034_24883/sockets/raylet',
      'Resources': {'CPU': 4.0},
      'alive': True}]

The above information includes:

  - `ClientID`: A unique identifier for the raylet.
  - `alive`: Whether the node is still alive.
  - `NodeManagerAddress`: PrivateIP of the node that the raylet is on.
  - `Resources`: The total resource capacity on the node.

Resource Information
--------------------

To get information about the current total resource capacity of your cluster, you can use ``ray.cluster_resources()``.

.. autofunction:: ray.cluster_resources
    :noindex:


To get information about the current available resource capacity of your cluster, you can use ``ray.available_resources()``.

.. autofunction:: ray.cluster_resources
    :noindex:


Object Information
------------------

To get information about the current objects that have been placed in the Ray object store across the cluster, you can use ``ray.objects()``.

.. autofunction:: ray.objects
    :noindex:

